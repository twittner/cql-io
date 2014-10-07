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
import Control.Lens ((^.), view, over, makeLenses)
import Data.IORef
import Data.Map.Strict (Map)
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Types (unit)
import Network.Socket (SockAddr)
import System.Random.MWC

import qualified Data.Map.Strict as Map

type HostMap = IORef Hosts

data Hosts = Hosts
    { _alive :: Map SockAddr Host
    , _other :: Map SockAddr Host
    }

makeLenses ''Hosts

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
        m <- view alive <$> readIORef hh
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
        h <- view alive <$> readIORef hh
        let pickRandom = uniformR (0, Map.size h - 1) gen
        if Map.null h
            then return Nothing
            else Just . snd . flip Map.elemAt h <$> pickRandom

onEvent :: HostMap -> HostEvent -> IO ()
onEvent r (HostAdded       h) = withMap r $ over alive (Map.insert (h^.hostAddr) h)
onEvent r (HostRemoved     s) = withMap r $ \h ->
    if Map.member s (h^.alive)
        then over alive (Map.delete s) h
        else over other (Map.delete s) h
onEvent r (HostUp          s) = setStatus (const Alive) s r
onEvent r (HostDown        s) = setStatus (const Dead) s r
onEvent r (HostUnreachable s) = setStatus mark s r
  where
    mark Dead  = Dead
    mark _     = Unreachable
onEvent r (HostReachable   s) = setStatus mark s r
  where
    mark Unreachable = Alive
    mark x           = x

insert :: HostMap -> Host -> IO ()
insert hh h = withMap hh $ over alive (Map.alter insertIfAbsent (h^.hostAddr))
  where
    insertIfAbsent Nothing = Just h
    insertIfAbsent x       = x

withMap :: HostMap -> (Hosts -> Hosts) -> IO ()
withMap s f = atomicModifyIORef' s $ \m -> (f m, ())

setStatus :: (HostStatus -> HostStatus) -> SockAddr -> HostMap -> IO ()
setStatus f a m = do
    h <- get <$> readIORef m
    case h of
        Nothing  -> return ()
        Just hst -> atomicModifyIORef' (hst^.status) $ \s -> (f s, ())
  where
    get h = Map.lookup a (h^.alive) <|> Map.lookup a (h^.other)
