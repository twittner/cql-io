-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens ((^.), Lens')
import Data.ByteString.Lazy.Char8 (unpack)
import Data.Text (Text)
import Database.CQL.IO.Types (InetAddr)
import System.Logger.Message

-- | This event will be passed to a 'Policy' to inform it about
-- cluster changes.
data HostEvent
    = HostNew  !Host     -- ^ a new host has been added to the cluster
    | HostGone !InetAddr -- ^ a host has been removed from the cluster
    | HostUp   !InetAddr -- ^ a host has been started
    | HostDown !InetAddr -- ^ a host has been stopped

-- | Host representation.
data Host = Host
    { _hostAddr   :: !InetAddr
    , _dataCentre :: !Text
    , _rack       :: !Text
    } deriving (Eq, Ord)

-- | The IP address and port number of this host.
hostAddr :: Lens' Host InetAddr
hostAddr f ~(Host a c r) = fmap (\x -> Host x c r) (f a)
{-# INLINE hostAddr #-}

-- | The data centre name (may be an empty string).
dataCentre :: Lens' Host Text
dataCentre f ~(Host a c r) = fmap (\x -> Host a x r) (f c)
{-# INLINE dataCentre #-}

-- | The rack name (may be an empty string).
rack :: Lens' Host Text
rack f ~(Host a c r) = fmap (\x -> Host a c x) (f r)
{-# INLINE rack #-}

instance Show Host where
    show = unpack . eval . bytes

instance ToBytes Host where
    bytes h = h^.dataCentre +++ val ":" +++ h^.rack +++ val ":" +++ h^.hostAddr
