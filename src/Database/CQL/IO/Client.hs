-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}

module Database.CQL.IO.Client
    ( Client
    , State
    , runClient
    , Database.CQL.IO.Client.init
    , shutdown
    , request
    , command
    ) where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Exception (IOException)
import Control.Lens hiding ((.=))
import Control.Monad.Catch
import Control.Monad.Reader as Reader
import Data.Foldable (for_)
import Data.IORef
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict (Map)
import Data.Maybe (listToMaybe)
import Data.Word
import Database.CQL.IO.Cluster.Discovery as Discovery
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Pool
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import Database.CQL.Protocol hiding (Map)
import Network.Socket (SockAddr (..), PortNumber (..))
import System.Logger hiding (Settings, settings, create)

import qualified Data.List.NonEmpty         as NE
import qualified Data.Map.Strict            as Map
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM

-----------------------------------------------------------------------------
-- API

data State = State
    { _env     :: Env
    , _control :: Control
    }

data Env = Env
    { _settings :: Settings
    , _logger   :: Logger
    , _timeouts :: TimeoutManager
    , _lbPolicy :: Policy
    }

data Control = Control
    { _token      :: MVar ()
    , _connection :: IORef Connection
    , _failures   :: IORef Word64
    , _hosts      :: IORef (Map SockAddr Host)
    }

makeLenses ''Env
makeLenses ''Control
makeLenses ''State

newtype Client a = Client
    { client :: ReaderT State IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadThrow
               , MonadMask
               , MonadCatch
               , MonadReader State
               )

init :: Logger -> Settings -> IO State
init g s = do
    t <- TM.create 250
    h <- newIORef Map.empty
    e <- Env s g t <$> view policy s h
    c <- tryAll (s^.contacts) (mkConnection e)
    initialise e c
    State e <$>  mkControl c h
  where
    mkConnection e h = do
        a <- C.resolve h (s^.portnumber)
        C.connect (s^.connSettings) (e^.timeouts) (s^.protoVersion) g a (eventHandler e)

    mkControl c h = liftIO $
        Control <$> newMVar ()
                <*> newIORef c
                <*> newIORef 0
                <*> pure h

shutdown :: MonadIO m => State -> m ()
shutdown s = liftIO $ do
    c <- readIORef (s^.control.connection)
    h <- readIORef (s^.control.hosts)
    C.close c
    TM.destroy (s^.env.timeouts) True
    mapM_ (destroy . view pool) (Map.elems h)

runClient :: MonadIO m => State -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    s <- ask
    h <- pickHost
    let p = h^.pool
    action s p `catches` handlers h
  where
    action s p = case s^.env.settings.maxWaitQueue of
        Nothing -> liftIO $ with p (transaction s)
        Just  q -> liftIO $ tryWith p (go s) >>= maybe (retry s q p) return

    go s h = do
        atomicModifyIORef' (s^.control.failures) $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction s h

    retry s q p = do
        k <- atomicModifyIORef' (s^.control.failures) $ \n -> (n + 1, n)
        unless (k < q) $
            throwM ConnectionsBusy
        with p (go s)

    transaction s c = do
        let x = s^.env.settings.connSettings.compression
        let v = s^.env.settings.protoVersion
        r <- parse x <$> C.request c (serialise v x a)
        r `seq` return r

    handlers h =
        [ Handler $ \(e :: ConnectionError) -> onConnectionError h e >> throwM e
        , Handler $ \(e :: IOException)     -> onConnectionError h e >> throwM e
        ]

command :: Request k () () -> Client ()
command = void . request

onConnectionError :: Exception e => Host -> e -> Client ()
onConnectionError h exc = do
    s <- ask
    let c = s^.control.connection
    let m = s^.control.token
    warn (s^.env.logger) $ "exception" .= show exc
    liftIO $ void $ tryWithMVar m $ const $ do
        x <- readIORef c
        when (x^.address == h^.hostAddr) $ do
            handler (s^.env.lbPolicy) $ HostDown (h^.hostAddr)
            C.close x
            a <- NE.nonEmpty . Map.keys <$> readIORef (s^.control.hosts)
            maybe (report s) (`tryAll` replaceConn s c) a
  where
    report s = err (s^.env.logger) $ "error-handler" .= val "no host available"

    replaceConn e r a = do
        let g = e^.env.logger
        let s = e^.env.settings
        let i = show (sockAddr2IP a)
        c <- C.connect (s^.connSettings) (e^.env.timeouts) (s^.protoVersion) g a (eventHandler (e^.env))
        startup c
        register c allEventTypes
        writeIORef r c
        info g $ msg (val "new control connection: " +++ i)

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

-----------------------------------------------------------------------------
-- Internal

pickHost :: Client Host
pickHost = do
    p <- view (env.lbPolicy)
    h <- liftIO $ getHost p
    maybe (throwM NoHostAvailable) return h

initialise :: MonadIO m => Env -> Connection -> m ()
initialise e c = liftIO $ do
    startup c
    let a = c^.address
    l <- listToMaybe <$> query c One Discovery.local ()
    p <- discoverPeers e c
    h <- Host (sockAddr2IP a) a True (view _2 <$> l) (view _3 <$> l) <$> mkPool e a
    mapM_ (addHost (e^.lbPolicy)) (h:p)
    register c allEventTypes
    info (e^.logger) $ msg (val "known hosts: " +++ show (h:p))

discoverPeers :: MonadIO m => Env -> Connection -> m [Host]
discoverPeers e c = liftIO $ do
    r <- query c One peers ()
    mapM (mkHost e) (map asRecord r)

eventHandler :: MonadIO m => Env -> Event -> m ()
eventHandler e x = liftIO $ do
    info (e^.logger) $ "client.event" .= show x
    let p = e^.settings.portnumber
    case x of
        StatusEvent   Up   sa        -> handler (e^.lbPolicy) (HostUp (mapPort p sa))
        StatusEvent   Down sa        -> handler (e^.lbPolicy) (HostDown (mapPort p sa))
        TopologyEvent RemovedNode sa -> handler (e^.lbPolicy) (HostRemoved (mapPort p sa))
        TopologyEvent NewNode     sa -> do
            let sa' = mapPort p sa
            host <- Host (sockAddr2IP sa') sa' True Nothing Nothing <$> mkPool e sa'
            handler (e^.lbPolicy) (HostAdded host)
        SchemaEvent   _              -> return ()

mapPort :: PortNumber -> SockAddr -> SockAddr
mapPort p (SockAddrInet _ a)      = SockAddrInet p a
mapPort p (SockAddrInet6 _ f a s) = SockAddrInet6 p f a s
mapPort _ unix                    = unix

mkPool :: MonadIO m => Env -> SockAddr -> m Pool
mkPool e a = liftIO $ do
    let g = e^.logger
    let s = e^.settings.poolSettings
    let n = e^.settings.connSettings.maxStreams
    create connOpen connClose g s n
  where
    connOpen = do
        let lgr = e^.logger
        let tmg = e^.timeouts
        let cst = e^.settings.connSettings
        c <- C.connect cst tmg (e^.settings.protoVersion) lgr a unit
        debug lgr $
            msg (val "client.connect")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show c
        connInit c `onException` connClose c
        return c

    connInit con = do
        C.startup con
        for_ (e^.settings.connSettings.defKeyspace) $
            C.useKeyspace con

    connClose con = do
        debug (e^.logger) $
            msg (val "client.close")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show con
        C.close con

mkHost :: MonadIO m => Env -> Peer -> m Host
mkHost e h = liftIO $ do
    let p = e^.settings.portnumber
    let a = ip2SockAddr p (peerRPC h)
    Host (peerRPC h) a True (Just $ peerDC h) (Just $ peerRack h) <$> mkPool e a

tryAll :: MonadCatch m => NonEmpty a -> (a -> m b) -> m b
tryAll (a :| []) f = f a
tryAll (a :| aa) f = f a `onException` tryAll (NE.fromList aa) f

tryWithMVar :: MVar a -> (a -> IO b) -> IO (Maybe b)
tryWithMVar m io = mask $ \restore -> do
    ma <- tryTakeMVar m
    flip (maybe (return Nothing)) ma $ \a -> do
        b <- restore (io a) `onException` putMVar m a
        putMVar m a
        return (Just b)
