-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Cache
    ( Cache
    , new
    , add
    , lookup
    ) where

import Control.Applicative
import Control.Monad.IO.Class
import Data.IORef
import Data.Map.Strict (Map)
import Prelude hiding (lookup)
import System.Random.MWC

import qualified Data.Map.Strict as Map

data Cache k v = Cache
    { maxSize :: !Int
    , cache   :: !(IORef (Map k v))
    , rgen    :: !GenIO
    }

new :: (MonadIO m) => Int -> m (Cache k v)
new s = liftIO (Cache s <$> newIORef Map.empty <*> create)

add :: (Ord k, MonadIO m) => k -> v -> Cache k v -> m ()
add k v c = liftIO $ do
    i <- asGenIO (uniformR (0, maxSize c - 1)) (rgen c)
    atomicModifyIORef' (cache c) $ \m ->
        if Map.size m < maxSize c
            then (Map.insert k v m, ())
            else (Map.insert k v (Map.deleteAt i m), ())

lookup :: (Ord k, MonadIO m) => k -> Cache k v -> m (Maybe v)
lookup k c = liftIO (Map.lookup k <$> readIORef (cache c))
