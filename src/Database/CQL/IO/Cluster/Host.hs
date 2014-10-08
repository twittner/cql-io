-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Cluster.Host where

import Control.Applicative
import Control.Concurrent.STM
import Control.Lens ((^.), over, makeLenses)
import Control.Monad.IO.Class
import Data.IP
import Data.List (intercalate)
import Data.Map.Strict (Map)
import Data.Text (Text, unpack)
import Database.CQL.IO.Pool
import Network.Socket (SockAddr)

import qualified Data.Map.Strict as Map

type HostMap = TVar Hosts

data HostStatus
    = Alive
    | Dead
    | Unreachable
    deriving (Eq, Ord, Show)

data Host = Host
    { _inetAddr   :: !IP
    , _hostAddr   :: !SockAddr
    , _status     :: !(TVar HostStatus)
    , _isChecked  :: !(TVar Bool)
    , _dataCentre :: !Text
    , _rack       :: !Text
    , _pool       :: !Pool
    }

data Hosts = Hosts
    { _alive :: Map SockAddr Host
    , _other :: Map SockAddr Host
    }

data HostEvent
    = HostAdded       !Host
    | HostRemoved     !SockAddr
    | HostUp          !SockAddr
    | HostDown        !SockAddr
    | HostUnreachable !SockAddr
    | HostReachable   !SockAddr

data Distance
    = Local
    | Remote
    | Ignored
    deriving (Eq, Ord, Show)

makeLenses ''Host
makeLenses ''Hosts

instance Show Host where
    show h = intercalate ":"
           [ unpack (h^.dataCentre)
           , unpack (h^.rack)
           , show (h^.inetAddr)
           ]

instance Eq Host where
    a == b = a^.inetAddr == b^.inetAddr

instance Ord Host where
    a `compare` b = (a^.inetAddr) `compare` (b^.inetAddr)

onEvent :: HostMap -> HostEvent -> IO ()
onEvent r (HostAdded h) = atomically $
    modifyTVar r (over alive (Map.insert (h^.hostAddr) h))
onEvent r (HostRemoved s) = atomically $ do
    h <- readTVar r
    if Map.member s (h^.alive)
        then writeTVar r (over alive (Map.delete s) h)
        else writeTVar r (over other (Map.delete s) h)
onEvent r (HostUp s) = atomically $ do
    h <- readTVar r
    case get s h of
        Nothing -> return ()
        Just  x -> do
            writeTVar (x^.status) Alive
            writeTVar r (over alive (Map.insert s x) . over other (Map.delete s) $ h)
onEvent r (HostDown s) = atomically $ do
    h <- readTVar r
    case get s h of
        Nothing -> return ()
        Just  x -> do
            writeTVar (x^.status) Dead
            writeTVar r (over other (Map.insert s x) . over alive (Map.delete s) $ h)
onEvent r (HostUnreachable s) = atomically $ do
    h <- readTVar r
    case get s h of
        Nothing -> return ()
        Just  x -> do
            modifyTVar (x^.status) mark
            writeTVar r (over other (Map.insert s x) . over alive (Map.delete s) $ h)
  where
    mark Dead  = Dead
    mark _     = Unreachable
onEvent r (HostReachable s) = atomically $ do
    h <- readTVar r
    case get s h of
        Nothing -> return ()
        Just  x -> do
            modifyTVar (x^.status) mark
            writeTVar r (over alive (Map.insert s x) . over other (Map.delete s) $ h)
  where
    mark Unreachable = Alive
    mark x           = x

insert :: HostMap -> Host -> IO ()
insert hh h = atomically $
    modifyTVar hh (over alive (Map.alter insertIfAbsent (h^.hostAddr)))
  where
    insertIfAbsent Nothing = Just h
    insertIfAbsent x       = x

get :: SockAddr -> Hosts -> Maybe Host
get a h = Map.lookup a (h^.alive) <|> Map.lookup a (h^.other)

setChecked :: MonadIO m => Host -> (Bool -> (Bool, a)) -> m a
setChecked h f = liftIO $ atomically $ do
    checked <- readTVar (h^.isChecked)
    let (ck, r) = f checked
    writeTVar (h^.isChecked) ck
    return r
