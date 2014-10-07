-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens (makeLenses, (^.))
import Data.IP
import Data.IORef
import Data.List (intercalate)
import Data.Text (Text, unpack)
import Database.CQL.IO.Pool
import Network.Socket (SockAddr)

data HostStatus
    = Alive
    | Dead
    | Unreachable
    deriving (Eq, Ord, Show)

data Host = Host
    { _inetAddr   :: !IP
    , _hostAddr   :: !SockAddr
    , _status     :: !(IORef HostStatus)
    , _dataCentre :: !Text
    , _rack       :: !Text
    , _pool       :: !Pool
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
