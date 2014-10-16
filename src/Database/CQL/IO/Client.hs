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
    , DebugInfo (..)
    , runClient
    , Database.CQL.IO.Client.init
    , shutdown
    , request
    , command
    , debugInfo
    ) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async)
import Control.Concurrent.STM hiding (retry)
import Control.Exception (IOException)
import Control.Lens hiding ((.=), Context)
import Control.Monad (unless, void, when)
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader (ReaderT, runReaderT, MonadReader, ask)
import Control.Retry
import Data.Foldable (for_, foldrM)
import Data.List (find)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Text (Text)
import Data.Word
import Database.CQL.IO.Cluster.Discovery as Discovery
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Jobs (Jobs)
import Database.CQL.IO.Pool
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Signal
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import Database.CQL.Protocol hiding (Map)
import Network.Socket (SockAddr (..), PortNumber)
import System.Logger.Class hiding (Settings, new, settings, create)

import qualified Data.List.NonEmpty         as NE
import qualified Data.Map.Strict            as Map
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Jobs       as Jobs
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

data Context = Context
    { _settings :: Settings
    , _logger   :: Logger
    , _timeouts :: TimeoutManager
    , _sigMonit :: Signal HostEvent
    }

data ClientState = ClientState
    { _context  :: Context
    , _policy   :: Policy
    , _control  :: TVar Control
    , _failures :: TVar Word64
    , _hostmap  :: TVar (Map Host Pool)
    , _jobs     :: Jobs InetAddr
    }

makeLenses ''Control
makeLenses ''Context
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
    log l m = view (context.logger) >>= \g -> Logger.log g l m

-----------------------------------------------------------------------------
-- API

runClient :: MonadIO m => ClientState -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    s <- ask
    n <- liftIO $ hostCount (s^.policy)
    start s n
  where
    start s n = do
        h <- pickHost (s^.policy)
        p <- Map.lookup h <$> readTVarIO' (s^.hostmap)
        case p of
            Just  x -> action s n x `catches` handlers h
            Nothing -> do
                err $ msg (val "no pool for host " +++ h)
                p' <- mkPool (s^.context) (h^.hostAddr)
                atomically' $ modifyTVar (s^.hostmap) (Map.alter (maybe (Just p') Just) h)
                start s n

    action s n p = do
        res <- tryWith p (go s)
        case res of
            Just  r -> return r
            Nothing ->
                if n > 0 then
                    start s (n - 1)
                else case s^.context.settings.maxWaitQueue of
                    Nothing -> with p (transaction s)
                    Just  q -> retry s q p

    go s h = do
        atomically $ modifyTVar (s^.failures) $ \n -> if n > 0 then n - 1 else 0
        transaction s h

    retry s q p = do
        n <- atomically' $ do
            n <- readTVar (s^.failures)
            unless (n == q) $
                writeTVar (s^.failures) (n + 1)
            return n
        unless (n < q) $
            throwM HostsBusy
        with p (go s)

    transaction s c = do
        let x = s^.context.settings.connSettings.compression
        let v = s^.context.settings.protoVersion
        r <- parse x <$> C.request c (serialise v x a)
        r `seq` return r

    handlers h =
        [ Handler $ \(e :: ConnectionError) -> onConnectionError h e >> throwM e
        , Handler $ \(e :: IOException)     -> onConnectionError h e >> throwM e
        ]

    pickHost p = maybe (throwM NoHostAvailable) return =<< liftIO (select p)

command :: Request k () () -> Client ()
command = void . request

data DebugInfo = DebugInfo
    { policyInfo :: String
    , jobInfo    :: [InetAddr]
    , hostInfo   :: [Host]
    }

instance Show DebugInfo where
    show dbg = showString "running jobs: "
             . shows (jobInfo dbg)
             . showString "\nknown hosts: "
             . shows (hostInfo dbg)
             . showString "\npolicy info: "
             . shows (policyInfo dbg)
             $ ""

debugInfo :: Client DebugInfo
debugInfo = do
    hosts <- Map.keys <$> (readTVarIO' =<< view hostmap)
    pols  <- liftIO . display =<< view policy
    jbs   <- Jobs.showJobs =<< view jobs
    return $ DebugInfo pols jbs hosts

-----------------------------------------------------------------------------
-- Initialisation

init :: MonadIO m => Logger -> Settings -> m ClientState
init g s = liftIO $ do
    t <- TM.create 250
    c <- tryAll (s^.contacts) (mkConnection t)
    e <- Context s g t <$> signal
    p <- s^.policyMaker
    x <- ClientState e
            <$> pure p
            <*> newTVarIO (Control Connected c)
            <*> newTVarIO 0
            <*> newTVarIO Map.empty
            <*> Jobs.new
    e^.sigMonit |-> onEvent p
    runClient x (initialise c)
    return x
  where
    mkConnection t h = do
        a <- C.resolve h (s^.portnumber)
        Logger.debug g $ msg (val "connecting to " +++ a)
        c <- C.connect (s^.connSettings) t (s^.protoVersion) g a
        Logger.info g $ msg (val "control connection: " +++ c)
        return c

initialise :: Connection -> Client ()
initialise c = do
    startup c
    env <- ask
    pol <- view policy
    ctx <- view context
    l <- local2Host (c^.address) . listToMaybe <$> query c One Discovery.local ()
    r <- discoverPeers ctx c
    (u, d) <- mkHostMap ctx pol (l:r)
    m <- view hostmap
    let h = Map.union u d
    atomically' $ writeTVar m h
    liftIO $ setup pol (Map.keys u) (Map.keys d)
    register c allEventTypes (runClient env . onCqlEvent)
    info $ msg (val "known hosts: " +++ show (Map.keys h))
    j <- view jobs
    for_ (Map.keys d) $ \down ->
        Jobs.add j (down^.hostAddr) $ monitor ctx 1000000 60000000 down

discoverPeers :: MonadIO m => Context -> Connection -> m [Host]
discoverPeers ctx c = liftIO $ do
    let p = ctx^.settings.portnumber
    map (peer2Host p . asRecord) <$> query c One peers ()

mkHostMap :: Context -> Policy -> [Host] -> Client (Map Host Pool, Map Host Pool)
mkHostMap c p = liftIO . foldrM checkHost (Map.empty, Map.empty)
  where
    checkHost h (up, down) = do
        okay <- acceptable p h
        if okay then do
            isUp <- C.ping (h^.hostAddr)
            if isUp then do
                up' <- Map.insert h <$> mkPool c (h^.hostAddr) <*> pure up
                return (up', down)
            else do
                down' <- Map.insert h <$> mkPool c (h^.hostAddr) <*> pure down
                return (up, down')
        else
            return (up, down)

mkPool :: MonadIO m => Context -> InetAddr -> m Pool
mkPool ctx i = liftIO $ do
    let s = ctx^.settings
    let m = s^.connSettings.maxStreams
    create (connOpen s) connClose (ctx^.logger) (s^.poolSettings) m
  where
    connOpen s = do
        let g = ctx^.logger
        c <- C.connect (s^.connSettings) (ctx^.timeouts) (s^.protoVersion) g i
        Logger.debug g $ "client.connect" .= c
        connInit c `onException` connClose c
        return c

    connInit con = do
        C.startup con
        for_ (ctx^.settings.connSettings.defKeyspace) $
            C.useKeyspace con

    connClose con = do
        Logger.debug (ctx^.logger) $ "client.close" .= con
        C.close con

-----------------------------------------------------------------------------
-- Termination

shutdown :: MonadIO m => ClientState -> m ()
shutdown s = liftIO $ do
    TM.destroy (s^.context.timeouts) True
    Jobs.destroy (s^.jobs)
    ignore $ C.close . view connection =<< readTVarIO (s^.control)
    mapM_ destroy . Map.elems =<< readTVarIO (s^.hostmap)

-----------------------------------------------------------------------------
-- Monitoring

monitor :: Context -> Int -> Int -> Host -> IO ()
monitor ctx initial upperBound h = do
    threadDelay initial
    Logger.info (ctx^.logger) $ msg (val "monitoring: " +++ h)
    hostCheck 0 maxN
  where
    hostCheck :: Int -> Int -> IO ()
    hostCheck n mx = do
        threadDelay (2^(min n mx) * 50000)
        isUp <- C.ping (h^.hostAddr)
        if isUp then do
            ctx^.sigMonit $$ HostUp (h^.hostAddr)
            Logger.info (ctx^.logger) $ msg (val "reachable: " +++ h)
        else do
            Logger.info (ctx^.logger) $ msg (val "unreachable: " +++ h)
            hostCheck (n + 1) mx

    maxN :: Int
    maxN = floor . logBase 2 $ (fromIntegral (upperBound `div` 50000) :: Double)

-----------------------------------------------------------------------------
-- Exception handling

onConnectionError :: Exception e => Host -> e -> Client ()
onConnectionError h exc = do
    warn $ "exception" .= show exc
    e <- ask
    c <- atomically' $ do
        ctrl <- readTVar (e^.control)
        let a = ctrl^.connection.address
        if ctrl^.state == Connected && a == h^.hostAddr then do
            writeTVar (e^.control) (set state Reconnecting ctrl)
            return $ Just (ctrl^.connection)
        else
            return Nothing
    maybe (liftIO . ignore . onEvent (e^.policy) $ HostDown (h^.hostAddr))
          (liftIO . void . async . recovering reconnectPolicy reconnectHandlers . continue e)
          c
    Jobs.add (e^.jobs) (h^.hostAddr) $
        monitor (e^.context) 0 30000000 h
  where
    continue e conn = do
        Jobs.destroy (e^.jobs)
        ignore $ C.close conn
        ignore $ onEvent (e^.policy) (HostDown (h^.hostAddr))
        x <- NE.nonEmpty . map (view hostAddr) . Map.keys <$> readTVarIO (e^.hostmap)
        case x of
            Just  a -> a `tryAll` (runClient e . replaceControl) `onException` reconnect e a
            Nothing -> do
                atomically $ modifyTVar (e^.control) (set state Disconnected)
                Logger.fatal (e^.context.logger) $ "error-handler" .= val "no host available"

    reconnect e a = do
        Logger.info (e^.context.logger) $ msg (val "reconnecting control ...")
        a `tryAll` (runClient e . replaceControl)

    reconnectPolicy = capDelay 5000000 (exponentialBackoff 5000)

    reconnectHandlers =
        [ const (Handler $ \(_ :: IOException)     -> return True)
        , const (Handler $ \(_ :: ConnectionError) -> return True)
        , const (Handler $ \(_ :: HostError)       -> return True)
        ]

replaceControl :: InetAddr -> Client ()
replaceControl a = do
    ctx <- view context
    ctl <- view control
    let s = ctx^.settings
    c <- C.connect (s^.connSettings) (ctx^.timeouts) (s^.protoVersion) (ctx^.logger) a
    initialise c
    atomically' $ writeTVar ctl (Control Connected c)
    info $ msg (val "new control connection: " +++ c)

onCqlEvent :: Event -> Client ()
onCqlEvent x = do
    info $ "client.event" .= show x
    pol <- view policy
    prt <- view (context.settings.portnumber)
    case x of
        StatusEvent Down sa -> do
            liftIO $ onEvent pol $ HostDown (InetAddr $ mapPort prt sa)
        TopologyEvent RemovedNode sa -> do
            let a = InetAddr $ mapPort prt sa
            hmap <- view hostmap
            liftIO $ onEvent pol $ HostGone a
            atomically' $
                modifyTVar hmap (Map.filterWithKey (\h _ -> h^.hostAddr /= a))
        StatusEvent Up sa ->
            startMonitor $ (InetAddr $ mapPort prt sa)
        TopologyEvent NewNode sa -> do
            ctx  <- view context
            hmap <- view hostmap
            ctrl <- readTVarIO' =<< view control
            let a = InetAddr $ mapPort prt sa
            let c = ctrl^.connection
            h    <- fromMaybe (Host a "" "") . find ((a == ) . view hostAddr) <$> discoverPeers ctx c
            okay <- liftIO $ acceptable pol h
            when okay $ do
                p <- mkPool ctx (h^.hostAddr)
                atomically' $ modifyTVar hmap (Map.alter (maybe (Just p) Just) h)
                liftIO $ onEvent pol $ HostNew h
        SchemaEvent _ -> return ()
  where
    mapPort i (SockAddrInet _ a)      = SockAddrInet i a
    mapPort i (SockAddrInet6 _ f a b) = SockAddrInet6 i f a b
    mapPort _ unix                    = unix

    startMonitor a = do
        hmp <- readTVarIO' =<< view hostmap
        case find ((a ==) . view hostAddr) (Map.keys hmp) of
            Just h  -> do
                ctx <- view context
                jbs <- view jobs
                Jobs.add jbs a $ monitor ctx 3000000 60000000 h
            Nothing -> return ()

-----------------------------------------------------------------------------
-- Utilities

peer2Host :: PortNumber -> Peer -> Host
peer2Host i p = Host (ip2inet i (peerRPC p)) (peerDC p) (peerRack p)

local2Host :: InetAddr -> Maybe (Text, Text) -> Host
local2Host i (Just (dc, rk)) = Host i dc rk
local2Host i Nothing         = Host i "" ""

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

tryAll :: NonEmpty a -> (a -> IO b) -> IO b
tryAll (a :| []) f = f a
tryAll (a :| aa) f = f a `catchAll` (const $ tryAll (NE.fromList aa) f)

atomically' :: STM a -> Client a
atomically' = liftIO . atomically

readTVarIO' :: TVar a -> Client a
readTVarIO' = liftIO . readTVarIO
