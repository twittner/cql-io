-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO
    ( Settings (..)
    , Pool
    , Client

    , mkPool
    , defSettings
    , runClient
    , query
    , prepare
    , execute
    , register
    , batch
    , cached
    , uncompressed

    , send
    , receive
    , request
    , command
    , supportedOptions
    ) where

import Database.CQL.Protocol
import Database.CQL.IO.Client

query :: (Tuple a, Tuple b) => QueryString a b -> QueryParams a -> Client [b]
query q p = do
    r <- req (Query q p)
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> return []
  where
    req :: (Tuple a, Tuple b) => Query a b -> Client (Response a b)
    req = request

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

cached :: (Tuple a, Tuple b) => QueryString a b -> QueryParams a -> Client [b]
cached q p = do
    i <- cacheLookup q
    case i of
        Nothing -> addCacheEntry >>= flip execute p
        Just i' -> execute i' p
  where
    addCacheEntry = do
        i <- prepare q
        cache q i
        return i

batch :: Consistency -> BatchType -> [BatchQuery] -> Client ()
batch c t q = command (Batch t q c)

register :: [EventType] -> Client ()
register = command . Register
