-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- | This driver operates on some state which must be initialised prior to
-- executing client operations and terminated eventually. The library uses
-- <http://hackage.haskell.org/package/tinylog tinylog> for its logging
-- output and expects a 'Logger'.
--
-- For example:
--
-- @
-- > import Data.Text (Text)
-- > import Data.Functor.Identity
-- > import Database.CQL.IO as Client
-- > import Database.CQL.Protocol
-- > import qualified System.Logger as Logger
-- >
-- > g <- Logger.new Logger.defSettings
-- > c <- Client.init g defSettings
-- > let p = QueryParams One False () Nothing Nothing Nothing
-- > runClient c $ query ("SELECT cql_version from system.local" :: QueryString R () (Identity Text)) p
-- [Identity "3.2.0"]
-- > shutdown c
-- @
--
-- __Note on prepared statements__
--
-- Prepared statements are fully supported but imply certain
-- complexities which lead to some assumptions beyond the scope
-- of the CQL binary protocol specification (spec):
--
-- (1) The spec scopes the 'QueryId' to the node the query has
--     been prepared with. The spec does not state anything
--     about the format of the 'QueryId', however it seems that
--     at least the official Java driver assumes that any given
--     'QueryString' yields the same 'QueryId' on every node.
--     We make the same assumption.
-- (2) In case a node does not know a given 'QueryId' an 'Unprepared'
--     error is returned. We assume that it is always safe to then
--     transparently re-prepare the corresponding 'QueryString' and
--     to re-execute the original request against the same node.
--
-- Besides these assumptions there is also a potential tradeoff in
-- regards to /eager/ vs. /lazy/ query preparation.
-- We understand /eager/ to mean preparation against all current nodes of
-- a cluster and /lazy/ to mean preparation against a single node if
-- required, i.e. after an 'Unprepared' error response. Which strategy to
-- choose depends on the scope of query reuse and the size of the cluster.
-- The global default can be changed through the 'Settings' module and per
-- action using 'withPrepareStrategy'.

{-# LANGUAGE DeriveFunctor #-}

module Database.CQL.IO
    ( -- * Client settings
      Settings
    , PrepareStrategy (..)
    , defSettings
    , addContact
    , setCompression
    , setConnectTimeout
    , setContacts
    , setIdleTimeout
    , setKeyspace
    , setMaxConnections
    , setMaxStreams
    , setMaxTimeouts
    , setPolicy
    , setPoolStripes
    , setPortNumber
    , setPrepareStrategy
    , setProtocolVersion
    , setResponseTimeout
    , setSendTimeout
    , setRetrySettings
    , setMaxRecvBuffer

      -- ** Retry Settings
    , RetrySettings
    , noRetry
    , retryForever
    , maxRetries
    , adjustConsistency
    , constDelay
    , expBackoff
    , fibBackoff
    , adjustSendTimeout
    , adjustResponseTimeout

      -- * Query Runner
    , RunQ (..)

      -- * Client monad
    , Client
    , MonadClient (..)
    , ClientState
    , DebugInfo   (..)
    , init
    , runClient
    , retry
    , shutdown
    , debugInfo

    , query
    , query1
    , write
    , schema
    , trans

    , Page      (..)
    , emptyPage
    , paginate

      -- * Prepared Queries
    , PrepQuery
    , prepared
    , queryString

      -- * Batch
    , BatchM
    , addQuery
    , addPrepQuery
    , setType
    , setConsistency
    , setSerialConsistency
    , batch

      -- ** low-level
    , request

      -- * Policies
    , Policy (..)
    , random
    , roundRobin

      -- ** Hosts
    , Host
    , HostEvent (..)
    , InetAddr  (..)
    , hostAddr
    , dataCentre
    , rack

    -- * Exceptions
    , InvalidSettings    (..)
    , InternalError      (..)
    , HostError          (..)
    , ConnectionError    (..)
    , UnexpectedResponse (..)
    , Timeout            (..)
    , HashCollision      (..)
    ) where

import Control.Applicative
import Control.Monad (void)
import Control.Monad.Catch
import Data.Maybe (isJust, listToMaybe)
import Database.CQL.Protocol
import Database.CQL.IO.Batch hiding (batch)
import Database.CQL.IO.Client
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.PrepQuery
import Database.CQL.IO.Settings
import Database.CQL.IO.Types
import Prelude hiding (init)

import qualified Database.CQL.IO.Batch as B

-- | A type which can run a query against Cassandra.
class RunQ q where
    runQ :: (MonadClient m, Tuple a, Tuple b) => q k a b -> QueryParams a -> m (Response k a b)

instance RunQ QueryString where
    runQ q p = do
        r <- request (RqQuery (Query q p))
        case r of
            RsError _ e -> throwM e
            _           -> return r

instance RunQ PrepQuery where
    runQ q = liftClient . execute q

-- | Run a CQL read-only query against a Cassandra node.
query :: (MonadClient m, Tuple a, Tuple b, RunQ q) => q R a b -> QueryParams a -> m [b]
query q p = do
    r <- runQ q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

-- | Run a CQL read-only query against a Cassandra node.
query1 :: (MonadClient m, Tuple a, Tuple b, RunQ q) => q R a b -> QueryParams a -> m (Maybe b)
query1 q p = listToMaybe <$> query q p

-- | Run a CQL insert/update query against a Cassandra node.
write :: (MonadClient m, Tuple a, RunQ q) => q W a () -> QueryParams a -> m ()
write q p = void $ runQ q p

-- | Run a CQL insert/update query as a \"lightweight transaction\" against a Cassandra node.
trans :: (MonadClient m, Tuple a, RunQ q) => q W a Row -> QueryParams a -> m [Row]
trans q p = do
    r <- runQ q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

-- | Run a CQL schema query against a Cassandra node.
schema :: (MonadClient m, Tuple a, RunQ q) => q S a () -> QueryParams a -> m (Maybe SchemaChange)
schema x y = do
    r <- runQ x y
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

-- | Run a batch query against a Cassandra node.
batch :: MonadClient m => BatchM () -> m ()
batch = liftClient . B.batch

-- | Return value of 'paginate'. Contains the actual result values as well
-- as an indication of whether there is more data available and the actual
-- action to fetch the next page.
data Page a = Page
    { hasMore  :: !Bool
    , result   :: [a]
    , nextPage :: Client (Page a)
    } deriving (Functor)

-- | A page with an empty result list.
emptyPage :: Page a
emptyPage = Page False [] (return emptyPage)

-- | Run a CQL read-only query against a Cassandra node.
--
-- This function is like 'query', but limits the result size to 10000
-- (default) unless there is an explicit size restriction given in
-- 'QueryParams'. The returned 'Page' can be used to continue the query.
--
-- Please note that -- as of Cassandra 2.1.0 -- if your requested page size
-- is equal to the result size, 'hasMore' might be true and a subsequent
-- 'nextPage' will return an empty list in 'result'.
paginate :: (MonadClient m, Tuple a, Tuple b, RunQ q) => q R a b -> QueryParams a -> m (Page b)
paginate q p = do
    let p' = p { pageSize = pageSize p <|> Just 10000 }
    r <- runQ q p'
    case r of
        RsResult _ (RowsResult m b) ->
            if isJust (pagingState m) then
                return $ Page True b (paginate q p' { queryPagingState = pagingState m })
            else
                return $ Page False b (return emptyPage)
        _ -> throwM UnexpectedResponse
