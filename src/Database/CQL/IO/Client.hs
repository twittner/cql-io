-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}

module Database.CQL.IO.Client
    ( Client
    , ClientState
    , runClient
    , Database.CQL.IO.Client.init
    , shutdown
    , request
    , command
    , showHosts
    , runningChecks
    ) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel)
import Control.Concurrent.STM hiding (retry)
import Control.Exception (IOException)
import Control.Lens hiding ((.=))
import Control.Monad (when, unless, void, filterM)
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader (ReaderT, runReaderT, MonadReader, ask)
import Control.Retry
import Data.Foldable (for_)
import Data.IP
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Set (Set)
import Data.Text (Text)
import Data.Traversable (forM)
import Data.Word
import Database.CQL.IO.Cluster.Discovery as Discovery
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Cleanups (Cleanups)
import Database.CQL.IO.Pool
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import Database.CQL.Protocol hiding (Set)
import Network.Socket (SockAddr (..))
import System.Logger.Class hiding (Settings, new, settings, create)

import qualified Data.List.NonEmpty         as NE
import qualified Data.Set                   as Set
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Cleanups   as Cleanups
import qualified Database.CQL.IO.Timeouts   as TM
import qualified System.Logger              as Logger

data ControlState
    = Disconnected
    | Connected
    | Reconnecting
    deriving (Eq, Ord, Show)

data Control = Control
    { _state      :: ControlState
    , _connection :: Connection
    }

data ClientState = ClientState
    { _settings :: Settings
    , _logger   :: Logger
    , _timeouts :: TimeoutManager
    , _policy   :: TVar Policy
    , _control  :: TVar Control
    , _failures :: TVar Word64
    , _hosts    :: TVar (Set Host)
    , _cleanups :: Cleanups SockAddr
    }

makeLenses ''Control
makeLenses ''ClientState

newtype Client a = Client
    { client :: ReaderT ClientState IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadThrow
               , MonadMask
               , MonadCatch
               , MonadReader ClientState
               )

instance MonadLogger Client where
    log l m = view logger >>= \g -> Logger.log g l m

init :: MonadIO m => Logger -> Settings -> m ClientState
init g s = liftIO $ do
    t <- TM.create 250
    c <- tryAll (s^.contacts) (mkConnection t) `onException` TM.destroy t True
    x <- ClientState s g t
            <$> (newTVarIO =<< s^.policyMaker)
            <*> newTVarIO (Control Connected c)
            <*> newTVarIO 0
            <*> newTVarIO Set.empty
            <*> Cleanups.new
    runClient x (initialise c)
    return x
  where
    mkConnection t h = do
        a <- C.resolve h (s^.portnumber)
        c <- C.connect (s^.connSettings) t (s^.protoVersion) g a
        Logger.info g $ msg (val "control connection: " +++ h +++ val ":" +++ show (s^.portnumber))
        return c

shutdown :: MonadIO m => ClientState -> m ()
shutdown s = liftIO $ do
    TM.destroy (s^.timeouts) True
    Cleanups.destroyAll (s^.cleanups)
    ignore $ C.close . view connection =<< readTVarIO (s^.control)
    hm <- readTVarIO (s^.hosts)
    mapM_ (destroy . view pool) (Set.toList hm)

runClient :: MonadIO m => ClientState -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

showHosts :: Client [(String, HostStatus)]
showHosts = do
    hh <- view hosts >>= liftIO . readTVarIO
    forM (Set.toList hh) $ \h -> do
        s <- liftIO $ readTVarIO (h^.status)
        return (show h, s)

runningChecks :: Client [SockAddr]
runningChecks = view cleanups >>= Cleanups.keys

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    s <- ask
    h <- pickHost
    trace $ msg (val "selected host: " +++ show h)
    action s (h^.pool) `catches` handlers h
  where
    action s p = case s^.settings.maxWaitQueue of
        Nothing -> liftIO $ with p (transaction s)
        Just  q -> liftIO $ tryWith p (go s) >>= maybe (retry s q p) return

    go s h = do
        atomically $ modifyTVar (s^.failures) $ \n -> if n > 0 then n - 1 else 0
        transaction s h

    retry s q p = do
        n <- atomically $ do
            n <- readTVar (s^.failures)
            unless (n == q) $
                writeTVar (s^.failures) (n + 1)
            return n
        unless (n < q) $
            throwM ConnectionsBusy
        with p (go s)

    transaction s c = do
        let x = s^.settings.connSettings.compression
        let v = s^.settings.protoVersion
        r <- parse x <$> C.request c (serialise v x a)
        r `seq` return r

    handlers h =
        [ Handler $ \(e :: ConnectionError) -> onConnectionError h e >> throwM e
        , Handler $ \(e :: IOException)     -> onConnectionError h e >> throwM e
        ]

    pickHost = do
        p <- getPolicy
        h <- liftIO $ getHost p
        maybe (throwM NoHostAvailable) return h

command :: Request k () () -> Client ()
command = void . request

onConnectionError :: Exception e => Host -> e -> Client ()
onConnectionError h exc = do
    e <- ask
    warn $ "exception" .= show exc
    mask_ $ do
        conn <- liftIO $ atomically $ do
            ctrl <- readTVar (e^.control)
            if ctrl^.state == Connected && ctrl^.connection.address == h^.hostAddr
                then do
                    writeTVar (e^.control) (set state Reconnecting ctrl)
                    return $ Just (ctrl^.connection)
                else return Nothing
        maybe (return ())
              (liftIO . void . async . recovering reconnectPolicy reconnectHandlers . continue e)
              conn
        monitor 30000000 h
  where
    continue e conn = do
        ignore $ C.close conn
        ignore $ readTVarIO (e^.policy) >>= flip handler_ (HostDown (h^.hostAddr))
        x <- NE.nonEmpty . map (view hostAddr) . Set.toList <$> readTVarIO (e^.hosts)
        case x of
            Just  a -> a `tryAll` (runClient e . replaceControl) `onException` reconnect e a
            Nothing -> do
                atomically $ modifyTVar (e^.control) (set state Disconnected)
                Logger.fatal (e^.logger) $ "error-handler" .= val "no host available"

    reconnect e a = do
        Logger.info (e^.logger) $ msg (val "reconnecting control ...")
        a `tryAll` (runClient e . replaceControl)

    reconnectPolicy = capDelay 5000000 (exponentialBackoff 5000)

    reconnectHandlers =
        [ const (Handler $ \(_ :: IOException)     -> return True)
        , const (Handler $ \(_ :: ConnectionError) -> return True)
        , const (Handler $ \(_ :: HostError)       -> return True)
        ]

replaceControl :: SockAddr -> Client ()
replaceControl a = do
    e <- ask
    let g = e^.logger
    let s = e^.settings
    let t = e^.timeouts
    c <- C.connect (s^.connSettings) t (s^.protoVersion) g a
    liftIO $ e^.settings.policyMaker >>= atomically . writeTVar (e^.policy)
    liftIO $ atomically $ writeTVar (e^.control) (Control Connected c)
    view cleanups >>= Cleanups.destroyAll
    initialise c
    info $ msg (val "new control: " +++ show (sockAddr2IP a) +++ val ":" +++ show (s^.portnumber))

monitor :: Int -> Host -> Client ()
monitor upperBound h = do
    ok <- setChecked h True
    when ok $ do
        e <- ask
        let action = hostCheck 0 maxN `finally` setChecked h False
        Cleanups.add (h^.hostAddr) (async (runClient e action)) cancel (e^.cleanups)
  where
    hostCheck :: Int -> Int -> Client ()
    hostCheck n mx = do
        liftIO $ threadDelay (2^(min n mx) * 50000)
        isUp <- C.ping (h^.hostAddr)
        if isUp
            then do
                info $ msg (val "reachable: " +++ show h)
                getPolicy >>= liftIO . flip handler_ (HostUp (h^.hostAddr))
                view cleanups >>= Cleanups.remove (h^.hostAddr)
            else do
                info $ msg (val "unreachable: " +++ show h)
                getPolicy >>= liftIO . flip handler_ (HostDown (h^.hostAddr))
                hostCheck (n + 1) mx

    maxN = floor . logBase 2 $ (fromIntegral (upperBound `div` 50000) :: Double)

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

initialise :: Connection -> Client ()
initialise c = do
    startup c
    let a = c^.address
    l <- listToMaybe <$> query c One Discovery.local ()
    x <- discoverPeers c
    h <- mkHost (fromMaybe "" $ view _2 <$> l) (fromMaybe "" $ view _3 <$> l) (sockAddr2IP a) a
    p <- getPolicy
    y <- liftIO $ filterM (fmap (Accepted ==) . handler p . HostAdded) (h:x)
    s <- ask
    liftIO $ atomically $ writeTVar (s^.hosts) (Set.fromList y)
    register c allEventTypes (runClient s . eventHandler)
    info $ msg (val "known hosts: " +++ show (h:x))

getPolicy :: Client Policy
getPolicy = view policy >>= liftIO . readTVarIO

discoverPeers :: Connection -> Client [Host]
discoverPeers c = do
    r <- query c One peers ()
    p <- view (settings.portnumber)
    mapM (fn p) (map asRecord r)
  where
    fn p h = do
        let a = ip2SockAddr p (peerRPC h)
        mkHost (peerDC h) (peerRack h) (peerRPC h) a

mkHost :: Text -> Text -> IP -> SockAddr -> Client Host
mkHost dc rk ip a = do
    let hostState t = if t then StatusUp else StatusDown
    b <- liftIO (C.ping a)
    h <- Host ip a
            <$> liftIO (newTVarIO (hostState b))
            <*> liftIO (newTVarIO False)
            <*> pure dc
            <*> pure rk
            <*> mkPool a
    when (hostState b == StatusDown) $
        monitor 60000000 h
    return h

mkPool :: SockAddr -> Client Pool
mkPool a = do
    s <- view settings
    t <- view timeouts
    g <- view logger
    liftIO $ create (connOpen s t g)
                    (connClose g)
                    g
                    (s^.poolSettings)
                    (s^.connSettings.maxStreams)
  where
    connOpen s t g = do
        c <- C.connect (s^.connSettings) t (s^.protoVersion) g a
        Logger.debug g $
            msg (val "client.connect")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show c
        connInit s c `onException` connClose g c
        return c

    connInit s con = do
        C.startup con
        for_ (s^.connSettings.defKeyspace) $
            C.useKeyspace con

    connClose g con = do
        Logger.debug g $
            msg (val "client.close")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show con
        C.close con

eventHandler :: Event -> Client ()
eventHandler x = do
    info $ "client.event" .= show x
    s <- view settings
    p <- getPolicy
    case x of
        StatusEvent Up   sa -> liftIO $ handler_ p (HostUp (mapPort (s^.portnumber) sa))
        StatusEvent Down sa -> liftIO $ handler_ p (HostDown (mapPort (s^.portnumber) sa))
        TopologyEvent RemovedNode sa -> do
            liftIO $ handler_ p (HostRemoved (mapPort (s^.portnumber) sa))
            view cleanups >>= Cleanups.destroy sa
        TopologyEvent NewNode sa -> do
            let sa' = mapPort (s^.portnumber) sa
            h <- mkHost "" "" (sockAddr2IP sa') sa'
            o <- view hosts
            liftIO $ do
                res <- handler p (HostAdded h)
                when (res == Accepted) $
                    atomically $ modifyTVar o (Set.insert h)
        SchemaEvent   _ -> return ()
  where
    mapPort i (SockAddrInet _ a)      = SockAddrInet i a
    mapPort i (SockAddrInet6 _ f a b) = SockAddrInet6 i f a b
    mapPort _ unix                    = unix

tryAll :: NonEmpty a -> (a -> IO b) -> IO b
tryAll (a :| []) f = f a
tryAll (a :| aa) f = f a `catchAll` (const $ tryAll (NE.fromList aa) f)
