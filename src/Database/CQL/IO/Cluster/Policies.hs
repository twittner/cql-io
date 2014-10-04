-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TupleSections #-}

module Database.CQL.IO.Cluster.Policies
    ( Policy (..)
    , HostMap
    , random
    , roundRobin
    , constant
    ) where

import Control.Applicative
import Control.Lens ((^.), set, view)
import Data.IORef
import Data.Map.Strict (Map)
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Types (unit)
import Network.Socket (SockAddr)
import System.Random.MWC

import qualified Data.Map.Strict as Map

type HostMap = IORef (Map SockAddr Host)

data Policy = Policy
    { handler :: HostEvent -> IO ()
    , addHost :: Host -> IO ()
    , getHost :: IO (Maybe Host)
    }

-- | Always return the host that first became available.
constant :: HostMap -> IO Policy
constant _ = do
    r <- newIORef Nothing
    return $ Policy unit (ins r) (readIORef r)
  where
    ins r h = atomicModifyIORef' r $ maybe (Just h, ()) ((, ()) . Just)

-- | Iterate over all hosts.
roundRobin :: HostMap -> IO Policy
roundRobin hh = do
    ctr <- newIORef 0
    return $ Policy (onEvent hh) (insert hh) (pickHost ctr)
  where
    pickHost ctr = do
        m <- Map.filter (view alive) <$> readIORef hh
        i <- atomicModifyIORef' ctr $ \k ->
                if k < Map.size m - 1
                    then (succ k, k)
                    else (0, k)
        if i < Map.size m
            then return . Just . snd $ Map.elemAt i m
            else return Nothing

-- | Return hosts randomly.
random :: HostMap -> IO Policy
random hh = do
    gen <- createSystemRandom
    return $ Policy (onEvent hh) (insert hh) (pickHost gen)
  where
    pickHost gen = do
        h <- Map.filter (view alive) <$> readIORef hh
        let pickRandom = uniformR (0, Map.size h - 1) gen
        if Map.null h
            then return Nothing
            else Just . snd . flip Map.elemAt h <$> pickRandom

onEvent :: HostMap -> HostEvent -> IO ()
onEvent r (HostAdded   h) = withMap r (Map.insert (h^.hostAddr) h)
onEvent r (HostRemoved s) = withMap r (Map.delete s)
onEvent r (HostUp      s) = withMap r (Map.adjust (set alive True) s)
onEvent r (HostDown    s) = withMap r (Map.adjust (set alive False) s)

insert :: HostMap -> Host -> IO ()
insert hh h = withMap hh (Map.alter insertIfAbsent (h^.hostAddr))
  where
    insertIfAbsent Nothing = Just h
    insertIfAbsent other   = other

withMap :: HostMap -> (Map SockAddr Host -> Map SockAddr Host) -> IO ()
withMap s f = atomicModifyIORef' s $ \m -> (f m, ())
