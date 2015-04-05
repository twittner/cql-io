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
import Control.Monad.Catch
import Control.Monad.IO.Class
import Database.CQL.IO.Types
import Data.Map.Strict (Map)
import Data.Unique
import Prelude

import qualified Data.Map.Strict as Map

data Job
    = Reserved
    | Running !Unique !(Async ())

newtype Jobs k = Jobs (TVar (Map k Job))

new :: MonadIO m => m (Jobs k)
new = liftIO $ Jobs <$> newTVarIO Map.empty

add :: (MonadIO m, Ord k) => Jobs k -> k -> Bool -> IO () -> m ()
add (Jobs d) k replace j = liftIO $ do
    (ok, prev) <- atomically $ do
        m <- readTVar d
        case Map.lookup k m of
            Nothing -> do
                modifyTVar' d (Map.insert k Reserved)
                return (True, Nothing)
            Just (Running _ a) | replace -> do
                modifyTVar' d (Map.insert k Reserved)
                return (True, Just a)
            _ -> return (False, Nothing)
    when ok $ do
        maybe (return ()) (ignore . cancel) prev
        u <- newUnique
        a <- async $ j `finally` remove u
        atomically $
            modifyTVar' d (Map.insert k (Running u a))
  where
    remove u = atomically $
        modifyTVar' d $ flip Map.update k $ \a ->
            case a of
                Running u' _ | u == u' -> Nothing
                _                      -> Just a

destroy :: MonadIO m => Jobs k -> m ()
destroy (Jobs d) = liftIO $ do
    items <- Map.elems <$> atomically (swapTVar d Map.empty)
    mapM_ f items
  where
    f (Running _ a) = ignore (cancel a)
    f _             = return ()

showJobs :: MonadIO m => Jobs k -> m [k]
showJobs (Jobs d) = liftIO $ Map.keys <$> readTVarIO d
