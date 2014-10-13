-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Database.CQL.IO.Settings where

import Control.Lens hiding ((<|))
import Data.List.NonEmpty (NonEmpty (..), (<|))
import Data.Time
import Data.Int
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Connection
import Database.CQL.IO.Cluster.Policies (Policy, random)
import Database.CQL.IO.Connection as C
import Database.CQL.IO.Pool as P
import Database.CQL.IO.Types (EventHandler, Milliseconds (..))
import Network.Socket (PortNumber (..))

data Settings = Settings
    { _poolSettings  :: PoolSettings
    , _connSettings  :: ConnectionSettings
    , _protoVersion  :: Version
    , _portnumber    :: PortNumber
    , _contacts      :: NonEmpty String
    , _maxWaitQueue  :: Maybe Word64
    , _eventHandler  :: EventHandler
    , _policyMaker   :: IO Policy
    }

makeLenses ''Settings

defSettings :: Settings
defSettings = Settings
    P.defSettings
    C.defSettings
    V3
    (fromInteger 9042)
    ("localhost" :| [])
    Nothing
    (const $ return ())
    random

-----------------------------------------------------------------------------
-- Settings

setProtocolVersion :: Version -> Settings -> Settings
setProtocolVersion v = set protoVersion v

setContacts :: String -> [String] -> Settings -> Settings
setContacts v vv = set contacts (v :| vv)

addContact :: String -> Settings -> Settings
addContact v = over contacts (v <|)

setPortNumber :: PortNumber -> Settings -> Settings
setPortNumber v = set portnumber v

setEventHandler :: EventHandler -> Settings -> Settings
setEventHandler v = set eventHandler v

setPolicy :: IO Policy -> Settings -> Settings
setPolicy v = set policyMaker v

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
setMaxStreams v s =
    case s^.protoVersion of
        V2 | v < 1 || v > mb8  -> error "Database.CQL.IO.Settings: streams must be within [1, 127]"
        V3 | v < 1 || v > mb16 -> error "Database.CQL.IO.Settings: streams must be within [1, 32767]"
        _                      -> set (connSettings.maxStreams) v s
  where
    mb8 :: Int
    mb8 = fromIntegral (maxBound :: Int8)

    mb16 :: Int
    mb16 = fromIntegral (maxBound :: Int16)

setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v = set (connSettings.connectTimeout) (Ms $ round (1000 * v))

setSendTimeout :: NominalDiffTime -> Settings -> Settings
setSendTimeout v = set (connSettings.sendTimeout) (Ms $ round (1000 * v))

setResponseTimeout :: NominalDiffTime -> Settings -> Settings
setResponseTimeout v = set (connSettings.responseTimeout) (Ms $ round (1000 * v))

setKeyspace :: Keyspace -> Settings -> Settings
setKeyspace v = set (connSettings.defKeyspace) (Just v)
