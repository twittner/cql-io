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
import Database.CQL.Protocol
import Database.CQL.IO.Cluster.Token (Token)

data Peer = Peer
    { peerAddr   :: !IP
    , peerRPC    :: !IP
    , peerDC     :: !Text
    , peerRack   :: !Text
    , peerTokens :: !(Set Token)
    } deriving Show

recordInstance ''Peer

peers :: QueryString R () (IP, IP, Text, Text, Set Token)
peers = "SELECT peer, rpc_address, data_center, rack, tokens FROM system.peers"

peer :: QueryString R (Identity IP) (IP, IP, Text, Text, Set Token)
peer = "SELECT peer, rpc_address, data_center, rack, tokens FROM system.peers where peer = ?"

local :: QueryString R () (Text, Text, Set Token)
local = "SELECT data_center, rack, tokens FROM system.local WHERE key='local'"
