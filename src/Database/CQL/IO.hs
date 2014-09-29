-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO
    ( Settings
    , defSettings
    , setProtocolVersion
    , setCompression
    , setBootstrapHost
    , setBootstrapPort
    , setIdleTimeout
    , setMaxConnections
    , setMaxStreams
    , setMaxWaitQueue
    , setPoolStripes
    , setConnectTimeout
    , setSendTimeout
    , setMaxTimeouts
    , setResponseTimeout
    , setOnEventHandler
    , setPolicy

    , Cluster
    , mkCluster
    , shutdown

    , Policy
    , random

    , query
    , write
    , schema

    , prepare
    , prepareWrite
    , prepareSchema

    , execute
    , executeWrite
    , executeSchema

    , register
    , batch

    , request
    , command

    -- * Exceptions
    , InvalidSettings    (..)
    , InternalError      (..)
    , ConnectionError    (..)
    , UnexpectedResponse (..)
    , Timeout            (..)
    ) where

import Control.Monad.Catch
import Control.Monad (void)
import Database.CQL.Protocol
import Database.CQL.IO.Cluster
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Settings
import Database.CQL.IO.Types

------------------------------------------------------------------------------
-- query

query' :: (Tuple a, Tuple b) => Cluster -> QueryString k a b -> QueryParams a -> IO (Response k a b)
query' c q p = do
    r <- request c (RqQuery (Query q p))
    case r of
        RsError _ e -> throwM e
        _           -> return r

query :: (Tuple a, Tuple b) => Cluster -> QueryString R a b -> QueryParams a -> IO [b]
query c q p = do
    r <- query' c q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

write :: (Tuple a) => Cluster -> QueryString W a () -> QueryParams a -> IO ()
write c q p = void $ query' c q p

schema :: (Tuple a) => Cluster -> QueryString S a () -> QueryParams a -> IO (Maybe SchemaChange)
schema c x y = do
    r <- query' c x y
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

------------------------------------------------------------------------------
-- prepare

prepare' :: (Tuple a, Tuple b) => Cluster -> QueryString k a b -> IO (QueryId k a b)
prepare' c q = do
    r <- request c (RqPrepare (Prepare q))
    case r of
        RsResult _ (PreparedResult i _ _) -> return i
        RsError  _ e                      -> throwM e
        _                                 -> throwM UnexpectedResponse

prepare :: (Tuple a, Tuple b) => Cluster -> QueryString R a b -> IO (QueryId R a b)
prepare = prepare'

prepareWrite :: (Tuple a) => Cluster -> QueryString W a () -> IO (QueryId W a ())
prepareWrite = prepare'

prepareSchema :: (Tuple a) => Cluster -> QueryString S a () -> IO (QueryId S a ())
prepareSchema = prepare'

------------------------------------------------------------------------------
-- execute

execute' :: (Tuple a, Tuple b) => Cluster -> QueryId k a b -> QueryParams a -> IO (Response k a b)
execute' c q p = do
    r <- request c (RqExecute (Execute q p))
    case r of
        RsError  _ e -> throwM e
        _            -> return r

execute :: (Tuple a, Tuple b) => Cluster -> QueryId R a b -> QueryParams a -> IO [b]
execute c q p = do
    r <- execute' c q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

executeWrite :: (Tuple a) => Cluster -> QueryId W a () -> QueryParams a -> IO ()
executeWrite c q p = void $ execute' c q p

executeSchema :: (Tuple a) => Cluster -> QueryId S a () -> QueryParams a -> IO (Maybe SchemaChange)
executeSchema c q p = do
    r <- execute' c q p
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

batch :: Cluster -> Batch -> IO ()
batch c b = command c (RqBatch b)

------------------------------------------------------------------------------
-- register

register :: Cluster -> [EventType] -> IO ()
register c = command c . RqRegister . Register

