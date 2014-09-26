-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Database.CQL.IO.Settings where

import Control.Lens
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Connection
import Database.CQL.IO.Cluster.Host (Host)
import Database.CQL.IO.Cluster.Policies (Policy, random)
import Database.CQL.IO.Connection as C
import Database.CQL.IO.Pool as P
import Database.CQL.IO.Types (EventHandler, Milliseconds (..))

data Settings = Settings
    { _poolSettings  :: PoolSettings
    , _connSettings  :: ConnectionSettings
    , _protoVersion  :: Version
    , _bootstrapHost :: String
    , _bootstrapPort :: Word16
    , _maxWaitQueue  :: Maybe Word64
    , _onEvent       :: EventHandler
    , _policy        :: [Host] -> IO Policy
    }

makeLenses ''Settings

defSettings :: Settings
defSettings = Settings
    P.defSettings
    C.defSettings
    V3
    "localhost" 9042
    Nothing
    (const $ return ())
    random

-----------------------------------------------------------------------------
-- Settings

setProtocolVersion :: Version -> Settings -> Settings
setProtocolVersion v = set protoVersion v

setBootstrapHost :: String -> Settings -> Settings
setBootstrapHost v = set bootstrapHost v

setBootstrapPort :: Word16 -> Settings -> Settings
setBootstrapPort v = set bootstrapPort v

setOnEventHandler :: EventHandler -> Settings -> Settings
setOnEventHandler v = set onEvent v

setPolicy :: ([Host] -> IO Policy) -> Settings -> Settings
setPolicy v = set policy v

setMaxWaitQueue :: Word64 -> Settings -> Settings
setMaxWaitQueue v = set maxWaitQueue (Just v)

-----------------------------------------------------------------------------
-- Pool Settings

setIdleTimeout :: NominalDiffTime -> Settings -> Settings
setIdleTimeout v = set (poolSettings.idleTimeout) v

-- | Maximum connections per pool stripe.
setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v = set (poolSettings.maxConnections) v

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s
    | v < 1     = error "Database.CQL.IO.Settings: stripes must be greater than 0"
    | otherwise = set (poolSettings.poolStripes) v s

setMaxTimeouts :: Int -> Settings -> Settings
setMaxTimeouts v = set (poolSettings.maxTimeouts) v

-----------------------------------------------------------------------------
-- Connection Settings

setCompression :: Compression -> Settings -> Settings
setCompression v = set (connSettings.compression) v

-- | Maximum streams per connection.
setMaxStreams :: Int -> Settings -> Settings
setMaxStreams v s
    | v < 1 || v > 128 = error "Database.CQL.IO.Settings: streams must be within [1, 128]"
    | otherwise        = set (connSettings.maxStreams) v s

setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v = set (connSettings.connectTimeout) (Ms $ round (1000 * v))

setSendTimeout :: NominalDiffTime -> Settings -> Settings
setSendTimeout v = set (connSettings.sendTimeout) (Ms $ round (1000 * v))

setResponseTimeout :: NominalDiffTime -> Settings -> Settings
setResponseTimeout v = set (connSettings.responseTimeout) (Ms $ round (1000 * v))
