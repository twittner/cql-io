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
import Network.Socket (SockAddr (..))
import System.Logger hiding (Settings, settings, create)

import qualified Data.List.NonEmpty         as NE
import qualified Data.Map.Strict            as Map
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM

-----------------------------------------------------------------------------
-- API

data ClientState = ClientState
    { _settings   :: Settings
    , _logger     :: Logger
    , _timeouts   :: TimeoutManager
    , _lbPolicy   :: Policy
    , _connection :: IORef Connection
    , _failures   :: IORef Word64
    , _hosts      :: IORef (Map SockAddr Host)
    , _mutex      :: MVar ()
    }

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

init :: MonadIO m => Logger -> Settings -> m ClientState
init g s = liftIO $ do
    t <- TM.create 250
    c <- tryAll (s^.contacts) (mkConnection t) `onException` TM.destroy t True
    h <- newIORef Map.empty
    p <- s^.policy $ h
    initialise g s t p c
    ClientState s g t p <$> newIORef c <*> newIORef 0 <*> pure h <*> newMVar ()
  where
    mkConnection t h = do
        a <- C.resolve h (s^.portnumber)
        C.connect (s^.connSettings) t (s^.protoVersion) g a

shutdown :: MonadIO m => ClientState -> m ()
shutdown s = liftIO $ do
    TM.destroy (s^.timeouts) True
    C.close =<< readIORef (s^.connection)
    mapM_ (destroy . view pool) . Map.elems =<< readIORef (s^.hosts)

runClient :: MonadIO m => ClientState -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    s <- ask
    h <- pickHost
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

onConnectionError :: Exception e => Host -> e -> Client ()
onConnectionError h exc = do
    e <- ask
    let c = e^.connection
    let m = e^.mutex
    warn (e^.logger) $ "exception" .= show exc
    liftIO $ void $ tryWithMVar m $ const $ do
        x <- readIORef c
        when (x^.address == h^.hostAddr) $ do
            C.close x
            handler (e^.lbPolicy) $ HostDown (h^.hostAddr)
            a <- NE.nonEmpty . Map.keys <$> readIORef (e^.hosts)
            maybe (report e) (`tryAll` replaceConn e c) a
  where
    report e = err (e^.logger) $ "error-handler" .= val "no host available"

    replaceConn e r a = do
        let g = e^.logger
        let s = e^.settings
        let i = show (sockAddr2IP a)
        c <- C.connect (s^.connSettings) (e^.timeouts) (s^.protoVersion) g a
        startup c
        register c allEventTypes (eventHandler g s (e^.timeouts) (e^.lbPolicy))
        writeIORef r c
        info g $ msg (val "new control connection: " +++ i)

allEventTypes :: [EventType]
allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

-----------------------------------------------------------------------------
-- Internal

initialise :: Logger -> Settings -> TimeoutManager -> Policy -> Connection -> IO ()
initialise g s t p c = do
    startup c
    let a = c^.address
    l <- listToMaybe <$> query c One Discovery.local ()
    x <- discoverPeers g s t c
    h <- Host (sockAddr2IP a) a True (view _2 <$> l) (view _3 <$> l) <$> mkPool g s t a
    mapM_ (addHost p) (h:x)
    register c allEventTypes (eventHandler g s t p)
    info g $ msg (val "known hosts: " +++ show (h:x))

discoverPeers :: Logger -> Settings -> TimeoutManager -> Connection -> IO [Host]
discoverPeers g s t c = do
    r <- query c One peers ()
    mapM mkHost (map asRecord r)
  where
    mkHost h = do
        let p = s^.portnumber
        let a = ip2SockAddr p (peerRPC h)
        Host (peerRPC h) a True (Just $ peerDC h) (Just $ peerRack h) <$> mkPool g s t a

mkPool :: Logger -> Settings -> TimeoutManager -> SockAddr -> IO Pool
mkPool g s t a =
    create connOpen connClose g (s^.poolSettings) (s^.connSettings.maxStreams)
  where
    connOpen = do
        c <- C.connect (s^.connSettings) t (s^.protoVersion) g a
        debug g $
            msg (val "client.connect")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show c
        connInit c `onException` connClose c
        return c

    connInit con = do
        C.startup con
        for_ (s^.connSettings.defKeyspace) $
            C.useKeyspace con

    connClose con = do
        debug g $
            msg (val "client.close")
            ~~ "host" .= show (sockAddr2IP a)
            ~~ "conn" .= show con
        C.close con

eventHandler :: Logger -> Settings -> TimeoutManager -> Policy -> Event -> IO ()
eventHandler g s t p x = do
    info g $ "client.event" .= show x
    case x of
        StatusEvent   Up   sa        -> handler p (HostUp (mapPort (s^.portnumber) sa))
        StatusEvent   Down sa        -> handler p (HostDown (mapPort (s^.portnumber) sa))
        TopologyEvent RemovedNode sa -> handler p (HostRemoved (mapPort (s^.portnumber) sa))
        TopologyEvent NewNode     sa -> do
            let sa' = mapPort (s^.portnumber) sa
            host <- Host (sockAddr2IP sa') sa' True Nothing Nothing <$> mkPool g s t sa'
            handler p (HostAdded host)
        SchemaEvent   _              -> return ()
  where
    mapPort i (SockAddrInet _ a)      = SockAddrInet i a
    mapPort i (SockAddrInet6 _ f a b) = SockAddrInet6 i f a b
    mapPort _ unix                    = unix

-----------------------------------------------------------------------------

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
