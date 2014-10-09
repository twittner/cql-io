-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Cleanups
    ( Cleanups
    , new
    , add
    , keys
    , remove
    , destroy
    , destroyAll
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Database.CQL.IO.Types
import Data.Map.Strict (Map)

import qualified Data.Map.Strict as Map

newtype Cleanups k = Cleanups (TVar (Map k (IO ())))

new :: MonadIO m => m (Cleanups k)
new = liftIO $ Cleanups <$> newTVarIO Map.empty

add :: (MonadIO m, Ord k) => k -> IO a -> (a -> IO ()) -> Cleanups k -> m ()
add k a f (Cleanups d) = liftIO $ do
    x <- a
    atomically $ modifyTVar d (Map.insert k (f x))

remove :: (MonadIO m, Ord k) => k -> Cleanups k -> m ()
remove k (Cleanups d) = liftIO $ do
    atomically $ modifyTVar d (Map.delete k)

destroy :: (MonadIO m, Ord k) => k -> Cleanups k -> m ()
destroy k (Cleanups d) = liftIO $ do
    fn <- atomically $ do
        m <- readTVar d
        case Map.lookup k m of
            Nothing -> return Nothing
            fn      -> writeTVar d (Map.delete k m) >> return fn
    maybe (return ()) ignore fn

destroyAll :: MonadIO m => Cleanups k -> m ()
destroyAll (Cleanups d) = liftIO $ do
    items <- Map.elems <$> atomically (swapTVar d Map.empty)
    mapM_ ignore items

keys :: MonadIO m => Cleanups k -> m [k]
keys (Cleanups d) = liftIO $ Map.keys <$> readTVarIO d
