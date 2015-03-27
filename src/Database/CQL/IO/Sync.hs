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
import Control.Exception (SomeException, Exception, toException)
import Prelude

data State a
    = Empty
    | Value  !a
    | Killed !SomeException
    | Closed !SomeException

newtype Sync a = Sync (TVar (State a))

create :: IO (Sync a)
create = Sync <$> newTVarIO Empty

get :: Sync a -> IO a
get (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Empty    -> retry
        Value  a -> writeTVar s Empty >> return a
        Closed x -> throwSTM x
        Killed x -> throwSTM x

put :: a -> Sync a -> IO Bool
put a (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Empty    -> writeTVar s (Value a) >> return True
        Closed _ -> return True
        _        -> writeTVar s Empty     >> return False

kill :: Exception e => e -> Sync a -> IO ()
kill x (Sync s) = atomically $ do
    v <- readTVar s
    case v of
        Closed _ -> return ()
        _        -> writeTVar s (Killed $ toException x)

close :: Exception e => e -> Sync a -> IO ()
close x (Sync s) = atomically $ writeTVar s (Closed $ toException x)

