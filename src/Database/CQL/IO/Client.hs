-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}

module Database.CQL.IO.Client
    ( Cluster
    , Client
    , mkCluster
    , initialise
    , shutdown
    , runClient
    , request
    , command
    ) where

import Control.Applicative
import Control.Lens ((^.), view, makeLenses)
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Reader
import Data.IORef
import Data.Map.Strict (Map)
import Data.Word
import Database.CQL.Protocol hiding (Map)
import Database.CQL.IO.Cluster
import Database.CQL.IO.Cluster.Discovery
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Pool
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import Network.Socket (SockAddr)
import System.Logger.Class hiding (create, Settings, settings)

import qualified Data.Map.Strict            as Map
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM
import qualified System.Logger              as Logger

data Cluster = Cluster
    { _settings :: Settings
    , _logger   :: Logger
    , _failures :: IORef Word64
    , _timeouts :: TimeoutManager
    , _control  :: IORef Connection
    , _hosts    :: IORef (Map SockAddr Host)
    , _lbPolicy :: Policy
    }

makeLenses ''Cluster

newtype Client a = Client
    { client :: ReaderT Cluster IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadThrow
               , MonadMask
               , MonadCatch
               , MonadReader Cluster
               )

instance MonadLogger Client where
    log l m = view logger >>= \g -> Logger.log g l m

mkCluster :: MonadIO m => Logger -> Settings -> m Cluster
mkCluster g s = liftIO $ do
    t <- TM.create 250
    a <- validateSettings t
    h <- newIORef Map.empty
    p <- s^.policy $ []
    c <- C.connect (s^.connSettings) t (s^.protoVersion) g a (eventHandler s g t p)
    Cluster s g <$> newIORef 0
                <*> pure t
                <*> newIORef c
                <*> pure h
                <*> pure p
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

initialise :: Client ()
initialise = do
    c <- controlConn
    liftIO $ startup c
    liftIO $ register c allEventTypes
    p <- view lbPolicy
    h <- discoverPeers
    liftIO $ mapM_ (addHost p) h
  where
    allEventTypes = [TopologyChangeEvent, StatusChangeEvent, SchemaChangeEvent]

shutdown :: MonadIO m => Cluster -> m ()
shutdown c = liftIO $ do
    con <- readIORef (c^.control)
    hst <- readIORef (c^.hosts)
    C.close con
    TM.destroy (c^.timeouts) True
    mapM_ (destroy . view pool) (Map.elems hst)

runClient :: MonadIO m => Cluster -> Client a -> m a
runClient c a = liftIO $ runReaderT (client a) c

discoverPeers :: Client [Host]
discoverPeers = do
    s <- view settings
    g <- view logger
    t <- view timeouts
    c <- controlConn
    liftIO $ do
        r <- query c One peers ()
        mapM (mkHost s g t (s^.bootstrapPort)) (map asRecord r)

pickHost :: Client Host
pickHost = do
    p <- view lbPolicy
    h <- liftIO $ getHost p
    maybe (throwM NoHostAvailable) return h

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    h <- pickHost
    s <- view settings
    f <- view failures
    let p = h^.pool
    liftIO $ case s^.maxWaitQueue of
        Nothing -> with p (transaction s)
        Just  q -> tryWith p (go s f) >>= maybe (retry q p s f) return
  where
    go s f h = do
        atomicModifyIORef' f $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction s h

    retry q p s f = do
        k <- atomicModifyIORef' f $ \n -> (n + 1, n)
        unless (k < q) $
            throwM ConnectionsBusy
        with p (go s f)

    transaction s c = do
        let x = s^.connSettings.compression
        let v = s^.protoVersion
        parse x <$> C.request c (serialise v x a)

command :: Request k () () -> Client ()
command r = void (request r)

controlConn :: Client Connection
controlConn = view control >>= liftIO . readIORef
