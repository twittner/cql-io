-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Cluster.Policies
    ( Policy (..)
    , random
    , roundRobin
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Lens ((^.), view, over, makeLenses)
import Control.Monad
import Control.Monad.IO.Class
import Data.Map.Strict (Map)
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Types (InetAddr)
import System.Random.MWC

import qualified Data.Map.Strict as Map

data Policy = Policy
    { setup      :: MonadIO m => [Host] -> [Host] -> m ()
    , onEvent    :: MonadIO m => HostEvent -> m ()
    , select     :: MonadIO m => m (Maybe Host)
    , acceptable :: MonadIO m => Host -> m Bool
    , display    :: MonadIO m => m String
    }

type HostMap = TVar Hosts

data Hosts = Hosts
    { _alive :: Map InetAddr Host
    , _other :: Map InetAddr Host
    } deriving Show

makeLenses ''Hosts

-- | Iterate over all hosts.
roundRobin :: IO Policy
roundRobin = do
    h <- newTVarIO emptyHosts
    c <- newTVarIO 0
    return $ Policy (defSetup h) (defOnEvent h) (pickHost h c) defAcceptable (defDisplay h)
  where
    pickHost h c = liftIO $ atomically $ do
        m <- view alive <$> readTVar h
        if Map.null m then
            return Nothing
        else do
            k <- readTVar c
            writeTVar c $ if k < Map.size m - 1 then succ k else 0
            return . Just . snd $ Map.elemAt k m

-- | Return hosts in random order.
random :: IO Policy
random = do
    h <- newTVarIO emptyHosts
    g <- createSystemRandom
    return $ Policy (defSetup h) (defOnEvent h) (pickHost h g) defAcceptable (defDisplay h)
  where
    pickHost h g = liftIO $ do
        m <- view alive <$> readTVarIO h
        if Map.null m then
            return Nothing
        else do
            let i = uniformR (0, Map.size m - 1) g
            Just . snd . flip Map.elemAt m <$> i

-----------------------------------------------------------------------------
-- Defaults

emptyHosts :: Hosts
emptyHosts = Hosts Map.empty Map.empty

defDisplay :: MonadIO m => HostMap -> m String
defDisplay h = liftIO $ show <$> readTVarIO h

defAcceptable :: MonadIO m => Host -> m Bool
defAcceptable = const $ return True

defSetup :: MonadIO m => HostMap -> [Host] -> [Host] -> m ()
defSetup r a b = liftIO $ do
    let ha = Map.fromList $ zip (map (view hostAddr) a) a
    let hb = Map.fromList $ zip (map (view hostAddr) b) b
    let hosts = Hosts ha hb
    atomically $ writeTVar r hosts

defOnEvent :: MonadIO m => HostMap -> HostEvent -> m ()
defOnEvent r (HostNew h) = liftIO $ atomically $ do
    m <- readTVar r
    when (Nothing == get (h^.hostAddr) m) $
        writeTVar r (over alive (Map.insert (h^.hostAddr) h) m)
defOnEvent r (HostGone a) = liftIO $ atomically $ do
    m <- readTVar r
    if Map.member a (m^.alive) then
        writeTVar r (over alive (Map.delete a) m)
    else
        writeTVar r (over other (Map.delete a) m)
defOnEvent r (HostUp a) = liftIO $ atomically $ do
    m <- readTVar r
    case get a m of
        Nothing -> return ()
        Just  h -> writeTVar r
            $ over alive (Map.insert a h)
            . over other (Map.delete a)
            $ m
defOnEvent r (HostDown a) = liftIO $ atomically $ do
    m <- readTVar r
    case get a m of
        Nothing -> return ()
        Just  h -> writeTVar r
            $ over other (Map.insert a h)
            . over alive (Map.delete a)
            $ m

get :: InetAddr -> Hosts -> Maybe Host
get a m = Map.lookup a (m^.alive) <|> Map.lookup a (m^.other)
