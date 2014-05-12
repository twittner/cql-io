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

data InvalidSettings
    = UnsupportedCompression [CompressionAlgorithm]
    | InvalidCacheSize
    deriving (Show, Typeable)

data InternalError = InternalError String
    deriving (Show, Typeable)

data ConnectionsBusy = ConnectionsBusy
    deriving (Show, Typeable)

data Timeout
    = ConnectTimeout
    | SendTimeout
    | RecvTimeout
    deriving (Show, Typeable)

data UnexpectedResponse where
    UnexpectedResponse  :: UnexpectedResponse
    UnexpectedResponse' :: (Show b) => Response k a b -> UnexpectedResponse

deriving instance Show     UnexpectedResponse
deriving instance Typeable UnexpectedResponse

instance Exception InvalidSettings
instance Exception InternalError
instance Exception UnexpectedResponse
instance Exception ConnectionsBusy
instance Exception Timeout
