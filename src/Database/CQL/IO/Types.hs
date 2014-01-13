-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable #-}

module Database.CQL.IO.Types where

import Control.Exception (Exception)
import Data.Typeable

data UnexpectedResponse
    = UnexpectedResponse
    | UnexpectedResponseDetails String
    deriving (Eq, Show, Typeable)

instance Exception UnexpectedResponse

