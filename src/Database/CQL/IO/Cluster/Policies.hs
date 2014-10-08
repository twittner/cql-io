-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections   #-}

module Database.CQL.IO.Cluster.Policies
    ( Policy (..)
    , HostMap
    , Hosts (Hosts)
    , alive
    , other
    , random
    , roundRobin
    , constant
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Lens (view)
import Data.IORef
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Types (unit)
import System.Random.MWC

import qualified Data.Map.Strict as Map

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
        m <- view alive <$> readTVarIO hh
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
        h <- view alive <$> readTVarIO hh
        let pickRandom = uniformR (0, Map.size h - 1) gen
        if Map.null h
            then return Nothing
            else Just . snd . flip Map.elemAt h <$> pickRandom
