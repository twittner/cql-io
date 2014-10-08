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
    , showNodes
    ) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel)
import Control.Concurrent.STM hiding (retry)
import Control.Exception (IOException)
import Control.Lens hiding ((.=))
import Control.Monad.Catch
import Control.Monad.Reader as Reader
import Control.Retry
import Data.Foldable (for_)
import Data.IORef
import Data.IP
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Set ((\\))
import Data.Text (Text)
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
import Database.CQL.Protocol hiding (Map)
import Network.Socket (SockAddr (..))
import System.Logger.Class hiding (Settings, new, settings, create)

import qualified Data.List.NonEmpty         as NE
import qualified Data.Map.Strict            as Map
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
    , _lbPolicy :: Policy
    , _control  :: IORef Control
    , _failures :: IORef Word64
    , _hosts    :: HostMap
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
    h <- newTVarIO (Hosts Map.empty Map.empty)
    p <- s^.policy $ h
    x <- ClientState s g t p
            <$> newIORef (Control Connected c)
            <*> newIORef 0
            <*> pure h
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
    ignore $ C.close . view connection =<< readIORef (s^.control)
    hm <- readTVarIO (s^.hosts)
    mapM_ (destroy . view pool) $ Map.elems (hm^.alive)
    mapM_ (destroy . view pool) $ Map.elems (hm^.other)

runClient :: MonadIO m => ClientState -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

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
        atomicModifyIORef' (s^.failures) $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction s h

    retry s q p = do
        k <- atomicModifyIORef' (s^.failures) $ \n -> (n + 1, n)
        unless (k < q) $
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
        p <- view lbPolicy
        h <- liftIO $ getHost p
        maybe (throwM NoHostAvailable) return h

command :: Request k () () -> Client ()
command = void . request

showNodes :: Client [(String, HostStatus)]
showNodes = do
    hh <- view hosts
    liftIO $ atomically $ do
        m <- readTVar hh
        (++) <$> showMap (m^.alive) <*> showMap (m^.other)
  where
    showMap m = forM (Map.elems m) $ \h -> do
        s <- readTVar (h^.status)
        return (show h, s)

onConnectionError :: Exception e => Host -> e -> Client ()
onConnectionError h exc = do
    e <- ask
    warn $ "exception" .= show exc
    mask_ $ do
        conn <- liftIO $ atomicModifyIORef' (e^.control) $ \ctrl ->
            if ctrl^.state == Connected && ctrl^.connection.address == h^.hostAddr
                then (set state Reconnecting ctrl, Just (ctrl^.connection))
                else (ctrl, Nothing)
        maybe (return ())
              (liftIO . void . async . recovering reconnectPolicy reconnectHandlers . continue e)
              conn
        monitor h
  where
    continue e conn = do
        ignore $ C.close conn
        ignore $ handler (e^.lbPolicy) $ HostUnreachable (h^.hostAddr)
        m <- readTVarIO (e^.hosts)
        let a = NE.nonEmpty . Map.keys $ view alive m
        let o = NE.nonEmpty . Map.keys $ view other m
        case (a, o) of
            (Nothing, Just o') -> o' `tryAll` (runClient e . replaceControl) `onException` reconnect e o'
            (Just a',       _) -> a' `tryAll` (runClient e . replaceControl) `onException` reconnect e a'
            (Nothing, Nothing) -> do
                atomicModifyIORef' (e^.control) $ \ctrl -> (set state Disconnected ctrl, ())
                Logger.fatal (e^.logger) $ "error-handler" .= val "no host available"

    reconnect e a = do
        Logger.info (e^.logger) $ msg (val "reconnecting ...")
        a `tryAll` (runClient e . replaceControl)

    reconnectPolicy = capDelay 30000000 (exponentialBackoff 5000)

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
    startup c
    register c allEventTypes (runClient e . eventHandler)
    p <- discoverPeers c
    m <- liftIO $ readTVarIO (e^.hosts)
    let known = Set.fromList (Map.elems (m^.alive)) `Set.union` Set.fromList (Map.elems (m^.other))
    let new   = Set.fromList p \\ known
    info $ msg (val "new hosts: " +++ show (Set.toList new))
    liftIO $ do
        for_ new $ addHost (e^.lbPolicy)
        atomicWriteIORef (e^.control) (Control Connected c)
        handler (e^.lbPolicy) $ HostUp a
    info $ msg (val "new control connection: " +++ show (sockAddr2IP a) +++ val ":" +++ show (s^.portnumber))
    dead <- liftIO $ atomically $ filterM reviveTheDead (Set.toList known)
    mapM_ monitor dead
  where
    reviveTheDead h = do
        s <- readTVar (h^.status)
        if s == Dead
            then writeTVar (h^.status) Unreachable >> return True
            else return False

monitor :: Host -> Client ()
monitor h = do
    ok <- setChecked h $ \ck -> if ck then (ck, False) else (True, True)
    when ok $ do
        e <- ask
        let action = hostCheck 0 50000 `finally` setChecked h (const (False, ()))
        Cleanups.add (e^.cleanups) (h^.hostAddr) (async (runClient e action)) cancel
  where
    hostCheck :: Int -> Int -> Client ()
    hostCheck n b = do
        hostStatus <- liftIO $ threadDelay (2^n * b) >> readTVarIO (h^.status)
        unless (hostStatus == Dead) $ do
            isUp <- C.ping (h^.hostAddr)
            if isUp
                then do
                    info $ msg (val "reachable again: " +++ show h)
                    view lbPolicy >>= liftIO . flip handler (HostReachable (h^.hostAddr))
                    view cleanups >>= \c -> Cleanups.remove c (h^.hostAddr)
                else do
                    info $ msg (val "unreachable: " +++ show h)
                    view lbPolicy >>= liftIO . flip handler (HostUnreachable (h^.hostAddr))
                    hostCheck (min (succ n) 9) 50000

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

initialise :: Connection -> Client ()
initialise c = do
    startup c
    let a = c^.address
    l <- listToMaybe <$> query c One Discovery.local ()
    x <- discoverPeers c
    h <- mkHost (fromMaybe "" $ view _2 <$> l) (fromMaybe "" $ view _3 <$> l) (sockAddr2IP a) a
    p <- view lbPolicy
    liftIO $ mapM_ (addHost p) (h:x)
    s <- ask
    register c allEventTypes (runClient s . eventHandler)
    info $ msg (val "known hosts: " +++ show (h:x))

discoverPeers :: Connection -> Client [Host]
discoverPeers c = do
    r <- query c One peers ()
    p <- view (settings.portnumber)
    mapM (f p) (map asRecord r)
  where
    f p h = do
        let a = ip2SockAddr p (peerRPC h)
        mkHost (peerDC h) (peerRack h) (peerRPC h) a

mkHost :: Text -> Text -> IP -> SockAddr -> Client Host
mkHost dc rk ip a = do
    b <- liftIO (C.ping a)
    let hostState t = if t then Alive else Unreachable
    Host ip a
        <$> liftIO (newTVarIO (hostState b))
        <*> liftIO (newTVarIO False)
        <*> pure dc
        <*> pure rk
        <*> mkPool a

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
    s <- view settings
    p <- view lbPolicy
    info $ "client.event" .= show x
    case x of
        StatusEvent   Up   sa        -> liftIO $ handler p (HostUp (mapPort (s^.portnumber) sa))
        StatusEvent   Down sa        -> liftIO $ handler p (HostDown (mapPort (s^.portnumber) sa))
        TopologyEvent RemovedNode sa -> do
            liftIO $ handler p (HostRemoved (mapPort (s^.portnumber) sa))
            c <- view cleanups
            Cleanups.destroy c sa
        TopologyEvent NewNode     sa -> do
            let sa' = mapPort (s^.portnumber) sa
            host <- mkHost "" "" (sockAddr2IP sa') sa'
            liftIO $ handler p (HostAdded host)
        SchemaEvent   _              -> return ()
  where
    mapPort i (SockAddrInet _ a)      = SockAddrInet i a
    mapPort i (SockAddrInet6 _ f a b) = SockAddrInet6 i f a b
    mapPort _ unix                    = unix

tryAll :: NonEmpty a -> (a -> IO b) -> IO b
tryAll (a :| []) f = f a
tryAll (a :| aa) f = f a `catchAll` (const $ tryAll (NE.fromList aa) f)
