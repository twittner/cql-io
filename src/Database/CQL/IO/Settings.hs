-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Settings where

import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Types (Milliseconds (..))

type EventHandler = Event -> IO ()

data Settings = Settings
    { sVersion         :: CqlVersion
    , sProtoVersion    :: Version
    , sCompression     :: Compression
    , sHost            :: String
    , sPort            :: Word16
    , sKeyspace        :: Maybe Keyspace
    , sIdleTimeout     :: NominalDiffTime
    , sMaxConnections  :: Int
    , sMaxStreams      :: Int
    , sMaxTimeouts     :: Int
    , sPoolStripes     :: Int
    , sMaxWaitQueue    :: Maybe Word64
    , sConnectTimeout  :: Milliseconds
    , sSendTimeout     :: Milliseconds
    , sResponseTimeout :: Milliseconds
    , sOnEvent         :: EventHandler
    }

defSettings :: Settings
defSettings = let handler = const $ return () in
    Settings Cqlv300 V3 noCompression "localhost" 9042
             Nothing -- keyspace
             60      -- idle timeout
             2       -- max connections per stripe
             128     -- max streams per connection
             16      -- max timeouts per connection
             4       -- max stripes
             Nothing -- max wait queue
             5000    -- connect timeout
             3000    -- send timeout
             10000   -- response timeout
             handler -- event handler

setCqlVersion :: CqlVersion -> Settings -> Settings
setCqlVersion v s = s { sVersion = v }

setProtoVersion :: Version -> Settings -> Settings
setProtoVersion v s = s { sProtoVersion = v }

setCompression :: Compression -> Settings -> Settings
setCompression v s = s { sCompression = v }

setHost :: String -> Settings -> Settings
setHost v s = s { sHost = v }

setPort :: Word16 -> Settings -> Settings
setPort v s = s { sPort = v }

setKeyspace :: Keyspace -> Settings -> Settings
setKeyspace v s = s { sKeyspace = Just v }

setIdleTimeout :: NominalDiffTime -> Settings -> Settings
setIdleTimeout v s = s { sIdleTimeout = v }

-- | Maximum connections per pool stripe.
setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v s = s { sMaxConnections = v }

-- | Maximum streams per connection.
setMaxStreams :: Int -> Settings -> Settings
setMaxStreams v s
    | v < 1 || v > 128 = error "Database.CQL.IO.Settings: streams must be within [1, 128]"
    | otherwise        = s { sMaxStreams = v }

setMaxWaitQueue :: Word64 -> Settings -> Settings
setMaxWaitQueue v s = s { sMaxWaitQueue = Just v }

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s
    | v < 1     = error "Database.CQL.IO.Settings: stripes must be greater than 0"
    | otherwise = s { sPoolStripes = v }

setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v s = s { sConnectTimeout = Ms $ round (1000 * v) }

setSendTimeout :: NominalDiffTime -> Settings -> Settings
setSendTimeout v s = s { sSendTimeout = Ms $ round (1000 * v) }

setResponseTimeout :: NominalDiffTime -> Settings -> Settings
setResponseTimeout v s = s { sResponseTimeout = Ms $ round (1000 * v) }

setMaxTimeouts :: Int -> Settings -> Settings
setMaxTimeouts v s = s { sMaxTimeouts = v }

setOnEventHandler :: EventHandler -> Settings -> Settings
setOnEventHandler v s = s { sOnEvent = v }

