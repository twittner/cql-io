-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Cluster.Event where

import Database.CQL.IO.Cluster.Host
import Network.Socket (SockAddr)

data ClusterEvent
    = HostAdded   !Host
    | HostRemoved !SockAddr
    | HostUp      !SockAddr
    | HostDown    !SockAddr


