-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Jobs
    ( Jobs
    , new
    , add
    , destroy
    , showJobs
    ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Database.CQL.IO.Types
import Data.Map.Strict (Map)

import qualified Data.Map.Strict as Map

data Job
    = Reserved
    | Running (Async ())

newtype Jobs k = Jobs (TVar (Map k Job))

new :: MonadIO m => m (Jobs k)
new = liftIO $ Jobs <$> newTVarIO Map.empty

add :: (MonadIO m, Ord k) => Jobs k -> k -> IO () -> m ()
add (Jobs d) k j = liftIO $ do
    (ok, prev) <- atomically $ do
        m <- readTVar d
        case Map.lookup k m of
            Just Reserved    -> return (False, Nothing)
            Just (Running a) -> do
                modifyTVar d (Map.insert k Reserved)
                return (True, Just a)
            Nothing          -> do
                modifyTVar d (Map.insert k Reserved)
                return (True, Nothing)
    when ok $ do
        maybe (return ()) (ignore . cancel) prev
        a <- async j
        atomically $
            modifyTVar d (Map.insert k (Running a))

destroy :: MonadIO m => Jobs k -> m ()
destroy (Jobs d) = liftIO $ do
    items <- Map.elems <$> atomically (swapTVar d Map.empty)
    mapM_ f items
  where
    f (Running a) = ignore (cancel a)
    f _           = return ()

showJobs :: MonadIO m => Jobs k -> m [k]
showJobs (Jobs d) = liftIO $ Map.keys <$> readTVarIO d
