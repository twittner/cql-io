-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.CQL.IO.Internal where

import Control.Applicative
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.ByteString (ByteString)
import Data.Text.Lazy (Text)
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Cache (Cache)
import System.IO

import qualified Data.Pool as P

type EventHandler = Event -> IO ()

data Settings = Settings
    { setVersion     :: CqlVersion
    , setCompression :: Compression
    , setHost        :: String
    , setPort        :: Word16
    , setKeyspace    :: Maybe Keyspace
    , setIdleTimeout :: NominalDiffTime
    , setPoolSize    :: Word32
    , cacheSize      :: Int
    , setOnEvent     :: EventHandler
    }

data Pool = Pool
    { sets   :: Settings
    , qCache :: Maybe (Cache Text ByteString)
    , pool   :: P.Pool Handle
    }

data Env = Env
    { conn       :: Handle
    , settings   :: Settings
    , queryCache :: Maybe (Cache Text ByteString)
    }

newtype Client a = Client
    { client :: ReaderT Env IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadCatch
               , MonadReader Env
               )

mapClient :: (IO a -> IO b) -> Client a -> Client b
mapClient f = Client . mapReaderT f . client
