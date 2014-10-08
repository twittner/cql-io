-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- | This cassandra driver is modelled as follows:
--
-- We support /n/ hosts, each of which has a pool of at most /k/ connections,
-- each of which supports up to /i/ concurrent streams.
--
-- Additionally we maintain a single control connection to one host which is
-- registered to receive events such as topology changes from the C* node.
--
-- Policies are provided which maintain a single driver-wide set of hosts and
-- are responsible for host addition/deletion in response to topology change
-- events as well as host selection on CQL requests. Some policies are for
-- instance 'random' or 'round-robin'.
--
-- The picture is complicated by the fact that the nodes may become
-- unavailable without notification. To mitigate this we do periodic health
-- checks (i.e. connection attempts) to every host and if necessary keep
-- trying to reconnect.
module Database.CQL.IO
    ( Settings
    , defSettings
    , setProtocolVersion
    , setCompression
    , setContacts
    , addContact
    , setPortNumber
    , setIdleTimeout
    , setMaxConnections
    , setMaxStreams
    , setMaxWaitQueue
    , setPoolStripes
    , setConnectTimeout
    , setSendTimeout
    , setMaxTimeouts
    , setResponseTimeout
    , setKeyspace
    , setOnEventHandler
    , setPolicy

    , Client
    , ClientState
    , runClient
    , Database.CQL.IO.Client.init
    , shutdown

    , Policy
    , random
    , constant

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
import Database.CQL.IO.Client
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Settings
import Database.CQL.IO.Types

------------------------------------------------------------------------------
-- query

query' :: (Tuple a, Tuple b) => QueryString k a b -> QueryParams a -> Client (Response k a b)
query' q p = do
    r <- request (RqQuery (Query q p))
    case r of
        RsError _ e -> throwM e
        _           -> return r

query :: (Tuple a, Tuple b) => QueryString R a b -> QueryParams a -> Client [b]
query q p = do
    r <- query' q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

write :: Tuple a => QueryString W a () -> QueryParams a -> Client ()
write q p = void $ query' q p

schema :: Tuple a => QueryString S a () -> QueryParams a -> Client (Maybe SchemaChange)
schema x y = do
    r <- query' x y
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

------------------------------------------------------------------------------
-- prepare

prepare' :: (Tuple a, Tuple b) => QueryString k a b -> Client (QueryId k a b)
prepare' q = do
    r <- request (RqPrepare (Prepare q))
    case r of
        RsResult _ (PreparedResult i _ _) -> return i
        RsError  _ e                      -> throwM e
        _                                 -> throwM UnexpectedResponse

prepare :: (Tuple a, Tuple b) => QueryString R a b -> Client (QueryId R a b)
prepare = prepare'

prepareWrite :: Tuple a => QueryString W a () -> Client (QueryId W a ())
prepareWrite = prepare'

prepareSchema :: Tuple a => QueryString S a () -> Client (QueryId S a ())
prepareSchema = prepare'

------------------------------------------------------------------------------
-- execute

execute' :: (Tuple a, Tuple b) => QueryId k a b -> QueryParams a -> Client (Response k a b)
execute' q p = do
    r <- request (RqExecute (Execute q p))
    case r of
        RsError  _ e -> throwM e
        _            -> return r

execute :: (Tuple a, Tuple b) => QueryId R a b -> QueryParams a -> Client [b]
execute q p = do
    r <- execute' q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

executeWrite :: Tuple a => QueryId W a () -> QueryParams a -> Client ()
executeWrite q p = void $ execute' q p

executeSchema :: Tuple a => QueryId S a () -> QueryParams a -> Client (Maybe SchemaChange)
executeSchema q p = do
    r <- execute' q p
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

batch :: Batch -> Client ()
batch b = command (RqBatch b)

------------------------------------------------------------------------------
-- register

register :: [EventType] -> Client ()
register = command . RqRegister . Register
