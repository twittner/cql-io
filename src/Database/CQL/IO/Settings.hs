-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Database.CQL.IO.Settings where

import Data.Time
import Data.Word
import Database.CQL.Protocol

type EventHandler = Event -> IO ()

data Settings = Settings
    { sVersion        :: CqlVersion
    , sCompression    :: Compression
    , sHost           :: String
    , sPort           :: Word16
    , sKeyspace       :: Maybe Keyspace
    , sIdleTimeout    :: NominalDiffTime
    , sMaxConnections :: Int
    , sMaxWaitQueue   :: Maybe Word
    , sPoolStripes    :: Int
    , sConnectTimeout :: Int
    , sRecvTimeout    :: Int
    , sSendTimeout    :: Int
    , sCacheSize      :: Int
    , sOnEvent        :: EventHandler
    }

defSettings :: Settings
defSettings = let handler = const $ return () in
    Settings Cqlv300 noCompression "localhost" 9042 Nothing 60 60 Nothing 4 3000 5000 5000 1024 handler

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

setMaxWaitQueue :: Word -> Settings -> Settings
setMaxWaitQueue v s = s { sMaxWaitQueue = Just v }

setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s = s { sPoolStripes = v }

setConnectTimeout :: Int -> Settings -> Settings
setConnectTimeout v s = s { sConnectTimeout = v }

setRecvTimeout :: Int -> Settings -> Settings
setRecvTimeout v s = s { sRecvTimeout = v }

setSendTimeout :: Int -> Settings -> Settings
setSendTimeout v s = s { sSendTimeout = v }

setCacheSize :: Int -> Settings -> Settings
setCacheSize v s = s { sCacheSize = v }

setOnEventHandler :: EventHandler -> Settings -> Settings
setOnEventHandler v s = s { sOnEvent = v }

