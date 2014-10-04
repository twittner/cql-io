-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TupleSections #-}

module Database.CQL.IO.Cluster.Policies
    ( Policy (..)
    , HostMap
    , empty
    , random
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

data RRState = RRState
    { rrHosts :: HostMap
    , rrGen   :: GenIO
    }

-- | Always return the host that first became available.
constant :: HostMap -> IO Policy
constant _ = do
    r <- newIORef Nothing
    return $ Policy unit (insert r) (readIORef r)
  where
    insert r h = atomicModifyIORef' r $ maybe (Just h, ()) ((, ()) . Just)

-- | Return hosts randomly.
random :: HostMap -> IO Policy
random hh = do
    state <- RRState hh <$> createSystemRandom
    return $ Policy (onEvent state) (insert state) (pickHost state)
  where
    onEvent :: RRState -> HostEvent -> IO ()
    onEvent rr (HostAdded   h) = withMap rr (Map.insert (h^.hostAddr) h)
    onEvent rr (HostRemoved s) = withMap rr (Map.delete s)
    onEvent rr (HostUp      s) = withMap rr (Map.adjust (set alive True) s)
    onEvent rr (HostDown    s) = withMap rr (Map.adjust (set alive False) s)

    insert :: RRState -> Host -> IO ()
    insert rr h = withMap rr (Map.alter insertIfAbsent (h^.hostAddr))
      where
        insertIfAbsent Nothing = Just h
        insertIfAbsent other   = other

    pickHost :: RRState -> IO (Maybe Host)
    pickHost rr = do
        h <- Map.filter (view alive) <$> readIORef (rrHosts rr)
        let pickRandom = uniformR (0, Map.size h - 1) (rrGen rr)
        if Map.null h
            then return Nothing
            else Just . snd . flip Map.elemAt h <$> pickRandom

withMap :: RRState -> (Map SockAddr Host -> Map SockAddr Host) -> IO ()
withMap s f = atomicModifyIORef' (rrHosts s) $ \m -> (f m, ())
