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
import Control.Monad
import Data.Set (Set)
import Database.CQL.IO.Types (InternalError (..))

import qualified Data.Set as Set

newtype Ticket = Ticket { toInt :: Int } deriving (Eq, Ord, Show)

newtype Pool = Pool (TVar (Maybe (Set Ticket)))

pool :: Int -> IO Pool
pool n = Pool <$> newTVarIO (Just . Set.fromList $ map Ticket [0 .. n-1])

close :: Pool -> IO ()
close (Pool p) = void . atomically $ swapTVar p Nothing

get :: Pool -> IO Ticket
get (Pool p) = atomically $ readTVar p >>= popHead
  where
    popHead Nothing  = throwSTM $ InternalError "ticket pool closed"
    popHead (Just x)
        | Set.null x = retry
        | otherwise  = do
            let (t, tt) = Set.deleteFindMin x
            writeTVar p (Just tt)
            return t

markAvailable :: Pool -> Int -> IO ()
markAvailable (Pool p) t = atomically $ modifyTVar' p pushHead
  where
    pushHead (Just tt) = Just $ Set.insert (Ticket t) tt
    pushHead Nothing   = Nothing

