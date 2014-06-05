-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Time
    ( TimeCache
    , create
    , close
    , currentTime
    ) where

import Control.Concurrent
import Control.Monad
import Data.IORef
import Data.Time.Clock

data TimeCache = TimeCache
    { currentTime :: IO UTCTime
    , close       :: IO ()
    , _final      :: IORef ()
    }

create :: IO TimeCache
create = do
    r <- newIORef =<< getCurrentTime
    f <- newIORef ()
    t <- forkIO $ forever $ do
        threadDelay 1000000
        writeIORef r =<< getCurrentTime
    void . mkWeakIORef f $ killThread t
    return $ TimeCache (readIORef r) (killThread t) f

