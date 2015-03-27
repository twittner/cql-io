-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Tickets
    ( Ticket
    , toInt
    , Pool
    , pool
    , close
    , get
    , markAvailable
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception (SomeException, Exception, toException)
import Data.Set (Set)
import Prelude

import qualified Data.Set as Set

newtype Ticket = Ticket { toInt :: Int } deriving (Eq, Ord, Show)

newtype Pool = Pool (TVar (Either SomeException (Set Ticket)))

pool :: Int -> IO Pool
pool n = Pool <$> newTVarIO (Right . Set.fromList $ map Ticket [0 .. n-1])

close :: Exception e => e -> Pool -> IO ()
close x (Pool p) = atomically $ writeTVar p (Left $ toException x)

get :: Pool -> IO Ticket
get (Pool p) = atomically $ readTVar p >>= popHead
  where
    popHead (Left x) = throwSTM x
    popHead (Right x)
        | Set.null x = retry
        | otherwise  = do
            let (t, tt) = Set.deleteFindMin x
            writeTVar p (Right tt)
            return t

markAvailable :: Pool -> Int -> IO ()
markAvailable (Pool p) t =
    atomically $ modifyTVar' p (fmap (Set.insert (Ticket t)))

