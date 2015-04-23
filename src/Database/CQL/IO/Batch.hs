-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE CPP                        #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.CQL.IO.Batch
    ( BatchM
    , batch
    , addQuery
    , addPrepQuery
    , setType
    , setConsistency
    , setSerialConsistency
    ) where

import Control.Applicative
import Control.Concurrent.STM (atomically)
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Trans
import Control.Monad.Trans.State.Strict
import Database.CQL.IO.Client
import Database.CQL.IO.PrepQuery
import Database.CQL.IO.Types
import Database.CQL.Protocol
import Prelude

-- | 'Batch' construction monad.
newtype BatchM a = BatchM
    { unBatchM :: StateT Batch Client a
    } deriving (Functor, Applicative, Monad)

-- | Execute the complete 'Batch' statement.
batch :: BatchM a -> Client ()
batch m = do
    b <- execStateT (unBatchM m) s
    checkRs =<< executeWithPrepare Nothing (RqBatch b :: Raw Request)
  where
    checkRs (RsResult _ VoidResult) = return ()
    checkRs (RsError  _ e)          = throwM e
    checkRs _                       = throwM UnexpectedResponse

    s = Batch BatchLogged [] Quorum Nothing

-- | Add a query to this batch.
addQuery :: (Show a, Tuple a, Tuple b) => QueryString W a b -> a -> BatchM ()
addQuery q p = BatchM $ modify' $ \b ->
    b { batchQuery = BatchQuery q p : batchQuery b }

-- | Add a prepared query to this batch.
addPrepQuery :: (Show a, Tuple a, Tuple b) => PrepQuery W a b -> a -> BatchM ()
addPrepQuery q p = BatchM $ do
    pq <- lift preparedQueries
    maybe (fresh pq) add =<< liftIO (atomically (lookupQueryId q pq))
  where
    fresh pq = do
        i <- snd <$> lift (prepare Nothing (queryString q))
        liftIO $ atomically (insert q i pq)
        add i

    add i = modify' $ \b -> b { batchQuery = BatchPrepared i p : batchQuery b }

-- | Set the type of this batch.
setType :: BatchType -> BatchM ()
setType t = BatchM $ modify' $ \b -> b { batchType = t }

-- | Set 'Batch' consistency level.
setConsistency :: Consistency -> BatchM ()
setConsistency c = BatchM $ modify' $ \b -> b { batchConsistency = c }

-- | Set 'Batch' serial consistency.
setSerialConsistency :: SerialConsistency -> BatchM ()
setSerialConsistency c = BatchM $ modify' $ \b -> b { batchSerialConsistency = Just c }

#if ! MIN_VERSION_transformers(0,4,0)
modify' :: Monad m => (s -> s) -> StateT s m ()
modify' f = do
    s <- get
    put $! f s
#endif
