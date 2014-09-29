-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Database.CQL.IO.Cluster where
--    ( Cluster
--    , mkCluster
--    , initialise
--    , shutdown
--    , request
--    , command
--    ) where

import Control.Applicative
import Control.Lens ((^.), view, makeLenses, Getting)
import Control.Monad (unless, void)
import Control.Monad.Catch
import Data.IORef
import Data.Map.Strict (Map)
import Data.Word
import Database.CQL.IO.Cluster.Discovery
import Database.CQL.IO.Cluster.Event
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Pool
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import Database.CQL.Protocol hiding (Map)
import Network.Socket (SockAddr, PortNumber (..))
import System.Logger hiding (Settings, settings, create)

import qualified Database.CQL.IO.Connection as C
import qualified Data.Map.Strict            as Map
import qualified Database.CQL.IO.Timeouts   as TM

data Cluster = Cluster
    { _env     :: Env
    , _control :: Control
    }

data Env = Env
    { _settings :: Settings
    , _logger   :: Logger
    , _timeouts :: TimeoutManager
    , _failures :: IORef Word64
    , _hosts    :: IORef (Map SockAddr Host)
    , _lbPolicy :: Policy
    }

newtype Control = Control
    { _connection :: IORef Connection
    }

makeLenses ''Env
makeLenses ''Control
makeLenses ''Cluster

-----------------------------------------------------------------------------
-- API

mkCluster :: Logger -> Settings -> IO Cluster
mkCluster g s = do
    t <- TM.create 250
    a <- validateSettings t
    h <- newIORef Map.empty
    e <- Env s g t <$> newIORef 0 <*> pure h <*> view policy s h
    c <- C.connect (s^.connSettings) t (s^.protoVersion) g a (eventHandler e)
    initialise e c
    Cluster e . Control <$> newIORef c
  where
    validateSettings t = do
        addr <- C.resolve (s^.bootstrapHost) (s^.bootstrapPort)
        Supported ca _ <- supportedOptions t addr
        let c = algorithm (s^.connSettings.compression)
        unless (c == None || c `elem` ca) $
            throwM $ UnsupportedCompression ca
        return addr

    supportedOptions t a = do
        let sett    = s^.connSettings
            acquire = C.connect sett t (s^.protoVersion) g a (const $ return ())
            options = RqOptions Options :: Request () () ()
        bracket acquire C.close $ \c -> do
            res <- C.request c (serialise (s^.protoVersion) noCompression options)
            case parse (sett^.compression) res :: Response () () () of
                RsSupported _ x -> return x
                other           -> throwM (UnexpectedResponse' other)

shutdown :: Cluster -> IO ()
shutdown e = do
    c <- get (control.connection) e
    h <- get (env.hosts) e
    C.close c
    TM.destroy (e^.env.timeouts) True
    mapM_ (destroy . view pool) (Map.elems h)

request :: (Tuple a, Tuple b) => Cluster -> Request k a b -> IO (Response k a b)
request e a = do
    h <- pickHost (e^.env)
    let p = h^.pool
    case e^.env.settings.maxWaitQueue of
        Nothing -> with p transaction
        Just  q -> tryWith p go >>= maybe (retry q p) return
  where
    go h = do
        atomicModifyIORef' (e^.env.failures) $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction h

    retry q p = do
        k <- atomicModifyIORef' (e^.env.failures) $ \n -> (n + 1, n)
        unless (k < q) $
            throwM ConnectionsBusy
        with p go

    transaction c = do
        let x = e^.env.settings.connSettings.compression
        let v = e^.env.settings.protoVersion
        parse x <$> C.request c (serialise v x a)

command :: Cluster -> Request k () () -> IO ()
command e r = void (request e r)

-----------------------------------------------------------------------------
-- Internal

initialise :: Env -> Connection -> IO ()
initialise e c = do
    startup c
    register c allEventTypes
    let a = c^.address
    addHost (e^.lbPolicy) . Host (sockAddr2IP a) a True Nothing Nothing =<< mkPool e a
    mapM_ (addHost (e^.lbPolicy)) =<< discoverPeers e c
  where
    allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

discoverPeers :: Env -> Connection -> IO [Host]
discoverPeers e c = do
    r <- query c One peers ()
    mapM (mkHost e) (map asRecord r)

pickHost :: Env -> IO Host
pickHost e = do
    h <- getHost (e^.lbPolicy)
    maybe (throwM NoHostAvailable) return h

eventHandler :: Env -> Event -> IO ()
eventHandler e x = do
    debug (e^.logger) $ "event" .= show x
    case x of
        StatusEvent   Up   sa        -> handler (e^.lbPolicy) (HostUp sa)
        StatusEvent   Down sa        -> handler (e^.lbPolicy) (HostDown sa)
        TopologyEvent RemovedNode sa -> handler (e^.lbPolicy) (HostRemoved sa)
        TopologyEvent NewNode     sa -> do
            host <- Host (sockAddr2IP sa) sa True Nothing Nothing <$> mkPool e sa
            handler (e^.lbPolicy) (HostAdded host)
        SchemaEvent   _              -> return ()

mkPool :: Env -> SockAddr -> IO Pool
mkPool e a = do
    let g = e^.logger
    let s = e^.settings.poolSettings
    let n = e^.settings.connSettings.maxStreams
    create connOpen connClose g s n
  where
    connOpen = do
        let lgr = e^.logger
        let tmg = e^.timeouts
        let cst = e^.settings.connSettings
        c <- C.connect cst tmg (e^.settings.protoVersion) lgr a (const $ return ())
        debug lgr $ "client.connect" .= show c
        C.startup c `onException` connClose c
        return c

    connClose con = do
        debug (e^.logger) $ "client.close" .= show con
        C.close con

mkHost :: Env -> Peer -> IO Host
mkHost e h = print h >> do
    let p = PortNum (e^.settings.bootstrapPort)
    let a = ip2SockAddr p (peerRPC h)
    Host (peerRPC h) a True (Just $ peerDC h) (Just $ peerRack h) <$> mkPool e a

get :: Getting (IORef a) s (IORef a) -> s -> IO a
get f e = readIORef (e^.f)
