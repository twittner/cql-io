-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Cluster.Host where

import Control.Lens (makeLenses)
import Data.Text (Text)
import Database.CQL.IO.Pool
import Network.Socket (SockAddr)

data Host = Host
    { _hostAddr   :: !SockAddr
    , _alive      :: !Bool
    , _dataCentre :: (Maybe Text)
    , _rack       :: (Maybe Text)
    , _pool       :: !Pool
    }

data Distance
    = Local
    | Remote
    | Ignored
    deriving (Eq, Ord, Show)

makeLenses ''Host
