-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.CQL.IO.Types where

import Control.Exception (Exception)
import Data.Typeable
import Database.CQL.Protocol (Response, CompressionAlgorithm)

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
-- UnexpectedResponse

data UnexpectedResponse where
    UnexpectedResponse  :: UnexpectedResponse
    UnexpectedResponse' :: (Show b) => Response k a b -> UnexpectedResponse

deriving instance Typeable UnexpectedResponse
instance Exception UnexpectedResponse

instance Show UnexpectedResponse where
    show UnexpectedResponse      = "Database.CQL.IO.UnexpectedResponse"
    show (UnexpectedResponse' r) = "Database.CQL.IO.UnexpectedResponse: " ++ show r

