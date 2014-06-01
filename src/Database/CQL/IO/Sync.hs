-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Sync
    ( Sync
    , create
    , get
    , put
    , close
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception (throw)
import Database.CQL.IO.Types (InternalError (..))

data State a = Empty | Value !a | Closed

newtype Sync a = Sync (TVar (State a))

create :: IO (Sync a)
create = Sync <$> newTVarIO Empty

get :: Sync a -> IO a
get (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Empty   -> retry
        Value a -> writeTVar s Empty >> return a
        Closed  -> throw $ InternalError "sync closed"

put :: Sync a -> a -> IO ()
put (Sync s) a = atomically $ do
    v <- readTVar s
    case v of
        Closed -> return ()
        _      -> writeTVar s (Value a)

close :: Sync a -> IO ()
close (Sync s) = atomically $ writeTVar s Closed
