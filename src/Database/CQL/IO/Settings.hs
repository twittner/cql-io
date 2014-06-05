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
    , sCompression     :: Compression
    , sHost            :: String
    , sPort            :: Word16
    , sKeyspace        :: Maybe Keyspace
    , sIdleTimeout     :: NominalDiffTime
    , sMaxConnections  :: Int
    , sMaxStreams      :: Int
    , sMaxWaitQueue    :: Maybe Word64
    , sPoolStripes     :: Int
    , sConnectTimeout  :: Milliseconds
    , sResponseTimeout :: Milliseconds
    , sOnEvent         :: EventHandler
    }

defSettings :: Settings
defSettings = let handler = const $ return () in
    Settings Cqlv300 noCompression "localhost" 9042 Nothing 60 2 1 Nothing 4 5000 10000 handler

setVersion :: CqlVersion -> Settings -> Settings
setVersion v s = s { sVersion = v }

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

setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v s = s { sMaxConnections = v }

setMaxStreams :: Int -> Settings -> Settings
setMaxStreams v s = s { sMaxStreams = v }

setMaxWaitQueue :: Word64 -> Settings -> Settings
setMaxWaitQueue v s = s { sMaxWaitQueue = Just v }

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s = s { sPoolStripes = v }

setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v s = s { sConnectTimeout = Ms $ round (1000 * v) }

setResponseTimeout :: NominalDiffTime -> Settings -> Settings
setResponseTimeout v s = s { sResponseTimeout = Ms $ round (1000 * v) }

setOnEventHandler :: EventHandler -> Settings -> Settings
setOnEventHandler v s = s { sOnEvent = v }

