-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}

module Database.CQL.IO.Cluster.Discovery where

import Data.Functor.Identity (Identity)
import Data.Text (Text)
import Data.UUID (UUID)
import Database.CQL.Protocol

data Peer = Peer
    { peerId   :: !UUID
    , peerAddr :: !Inet
    , peerRPC  :: !Inet
    , peerDC   :: !Text
    , peerRack :: !Text
    } deriving Show

recordInstance ''Peer

peers :: QueryString R () (UUID, Inet, Inet, Text, Text)
peers = "SELECT host_id, peer, rpc_address, data_center, rack FROM system.peers"

peer :: QueryString R (Identity Inet) (UUID, Inet, Inet, Text, Text)
peer = "SELECT host_id, peer, rpc_address, data_center, rack FROM system.peers where peer = ?"
