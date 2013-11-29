-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO
    ( query
    , query_
    , prepare
    , execute
    , register
    , batch
    , module M
    ) where

import Control.Monad
import Database.CQL.Protocol
import Database.CQL.IO.Client as M

query :: (Tuple a, Tuple b) => Query a b -> Client [b]
query q = do
    r <- query' q
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> return []
  where
    query' :: (Tuple a, Tuple b) => Query a b -> Client (Response a b)
    query' = request

query_ :: (Tuple a) => Query a () -> Client ()
query_ q = void (query q)

prepare :: (Tuple a, Tuple b) => QueryString a b -> Client (QueryId a b)
prepare q = do
    r <- request (Prepare q)
    case r of
        RsResult _ (PreparedResult i _ _) -> return i
        _                                 -> fail "unexpected response"

execute :: (Tuple a, Tuple b) => QueryId a b -> QueryParams a -> Client [b]
execute q p = do
    r <- exec (Execute q p)
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> return []
  where
    exec :: (Tuple a, Tuple b) => Execute a b -> Client (Response a b)
    exec = request

batch :: Consistency -> BatchType -> [BatchQuery] -> Client ()
batch c t q = command (Batch t q c)

register :: [EventType] -> Client ()
register = command . Register
