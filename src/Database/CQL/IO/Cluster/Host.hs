-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens ((^.), makeLenses)
import Data.ByteString.Lazy.Char8 (unpack)
import Data.Text (Text)
import Database.CQL.IO.Types (InetAddr)
import System.Logger.Message

data HostEvent
    = HostNew  !Host
    | HostGone !InetAddr
    | HostUp   !InetAddr
    | HostDown !InetAddr

data Host = Host
    { _hostAddr   :: !InetAddr
    , _dataCentre :: !Text
    , _rack       :: !Text
    } deriving (Eq, Ord)

makeLenses ''Host

instance Show Host where
    show = unpack . eval . bytes

instance ToBytes Host where
    bytes h = h^.dataCentre +++ val ":" +++ h^.rack +++ val ":" +++ h^.hostAddr
