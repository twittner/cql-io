-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module Database.CQL.IO.Cluster.Discovery where

import Data.Functor.Identity (Identity)
import Data.IP
import Data.Text (Text)
import Data.UUID (UUID)
import Database.CQL.Protocol

data Peer = Peer
    { peerId   :: !UUID
    , peerAddr :: !IP
    , peerRPC  :: !IP
    , peerDC   :: !Text
    , peerRack :: !Text
    } deriving Show

recordInstance ''Peer

peers :: QueryString R () (UUID, IP, IP, Text, Text)
peers = "SELECT host_id, peer, rpc_address, data_center, rack FROM system.peers"

peer :: QueryString R (Identity IP) (UUID, IP, IP, Text, Text)
peer = "SELECT host_id, peer, rpc_address, data_center, rack FROM system.peers where peer = ?"

local :: QueryString R () (UUID, Text, Text)
local = "SELECT host_id, data_center, rack FROM system.local WHERE key='local'"
