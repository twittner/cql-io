-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO
    ( runQuery
    , runQuery_
    , runPrepare
    , runExecute
    , module M
    ) where

import Control.Monad
import Control.Monad.Catch
import Database.CQL.Protocol
import Database.CQL.IO.Client as M

runQuery :: (Tuple a, Tuple b) => Query a b -> Client [b]
runQuery q = do
    r <- query q
    case r of
        RsResult _ (RowsResult _ b) -> return b
        RsError  _ e                -> throwM e
        _                           -> return []
  where
    query :: (Tuple a, Tuple b) => Query a b -> Client (Response a b)
    query = request

runQuery_ :: (Tuple a) => Query a () -> Client ()
runQuery_ q = void (runQuery q :: Client [()])

runPrepare :: (Tuple a, Tuple b) => QueryString a b -> Client (QueryId a b)
runPrepare q = do
    h <- send (Prepare q)
    r <- receive h
    case r of
        RsResult _ (PreparedResult i _ _) -> return i
        RsError  _ e                      -> throwM e
        _                                 -> fail "unexpected response"

runExecute :: (Tuple a, Tuple b) => QueryId a b -> QueryParams a -> Client [b]
runExecute q p = do
    r <- exec (Execute q p)
    case r of
        RsResult _ (RowsResult _ b) -> return b
        RsError  _ e                -> throwM e
        _                           -> return []
  where
    exec :: (Tuple a, Tuple b) => Execute a b -> Client (Response a b)
    exec = request
