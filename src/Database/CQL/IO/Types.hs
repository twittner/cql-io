-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Database.CQL.IO.Types where

import Control.Exception (Exception, SomeException, catch)
import Data.Typeable
import Database.CQL.Protocol (Event, Response, CompressionAlgorithm)

type EventHandler = Event -> IO ()

newtype Milliseconds = Ms { ms :: Int } deriving (Eq, Show, Num)

-----------------------------------------------------------------------------
-- InvalidSettings

data InvalidSettings
    = UnsupportedCompression [CompressionAlgorithm]
    | InvalidCacheSize
    deriving Typeable

instance Exception InvalidSettings

instance Show InvalidSettings where
    show (UnsupportedCompression cc) =
        "Database.CQL.IO.UnsupportedCompression: " ++ show cc
    show InvalidCacheSize = "Database.CQL.IO.InvalidCacheSize"

-----------------------------------------------------------------------------
-- InternalError

data InternalError = InternalError String
    deriving Typeable

instance Exception InternalError

instance Show InternalError where
    show (InternalError e) = "Database.CQL.IO.InternalError: " ++ show e

-----------------------------------------------------------------------------
-- HostError

data HostError
    = NoHostAvailable
    deriving Typeable

instance Exception HostError

instance Show HostError where
    show NoHostAvailable = "Database.CQL.IO.NoHostAvailable"

-----------------------------------------------------------------------------
-- ConnectionError

data ConnectionError
    = ConnectionsBusy
    | ConnectionClosed
    | ConnectTimeout
    deriving Typeable

instance Exception ConnectionError

instance Show ConnectionError where
    show ConnectionsBusy   = "Database.CQL.IO.ConnectionsBusy"
    show ConnectionClosed  = "Database.CQL.IO.ConnectionClosed"
    show ConnectTimeout    = "Database.CQL.IO.ConnectTimeout"

-----------------------------------------------------------------------------
-- Timeout

data Timeout = Timeout String
    deriving Typeable

instance Exception Timeout

instance Show Timeout where
    show (Timeout e) = "Database.CQL.IO.Timeout: " ++ e

-----------------------------------------------------------------------------
-- UnexpectedResponse

data UnexpectedResponse where
    UnexpectedResponse  :: UnexpectedResponse
    UnexpectedResponse' :: (Show b) => Response k a b -> UnexpectedResponse

deriving instance Typeable UnexpectedResponse
instance Exception UnexpectedResponse

instance Show UnexpectedResponse where
    show UnexpectedResponse      = "Database.CQL.IO.UnexpectedResponse"
    show (UnexpectedResponse' r) = "Database.CQL.IO.UnexpectedResponse: " ++ show r

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())
{-# INLINE ignore #-}

unit :: Monad m => a -> m ()
unit = const $ return ()
{-# INLINE unit #-}
