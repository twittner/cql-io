-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Sync
    ( Sync
    , create
    , get
    , put
    , kill
    , close
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Database.CQL.IO.Types (InternalError (..))

data State a = Empty | Value !a | Killed | Closed

newtype Sync a = Sync (TVar (State a))

create :: IO (Sync a)
create = Sync <$> newTVarIO Empty

get :: Sync a -> IO a
get (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Empty   -> retry
        Value a -> writeTVar s Empty >> return a
        Closed  -> throwSTM $ InternalError "sync closed"
        Killed  -> throwSTM $ InternalError "sync killed"

put :: Sync a -> a -> IO Bool
put (Sync s) a = atomically $ do
    v <- readTVar s
    case v of
        Empty  -> writeTVar s (Value a) >> return True
        Closed -> return True
        _      -> writeTVar s Empty     >> return False

kill :: Sync a -> IO ()
kill (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Closed -> return ()
        _      -> writeTVar s Killed

close :: Sync a -> IO ()
close (Sync s) = atomically $ writeTVar s Closed

