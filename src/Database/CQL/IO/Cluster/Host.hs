-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens ((^.), makeLenses)
import Data.List (intercalate)
import Data.Text (Text, unpack)
import Database.CQL.IO.Types (InetAddr)

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
    show h = intercalate ":"
           [ unpack (h^.dataCentre)
           , unpack (h^.rack)
           , show (h^.hostAddr)
           ]
