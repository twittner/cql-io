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

{-# LANGUAGE DeriveFunctor #-}

module Database.CQL.IO
    ( -- * Client settings
      Settings
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
    , setMaxWaitQueue
    , setPolicy
    , setPoolStripes
    , setPortNumber
    , setProtocolVersion
    , setResponseTimeout
    , setSendTimeout

      -- * Client monad
    , Client
    , ClientState
    , DebugInfo (..)
    , init
    , runClient
    , retry
    , shutdown
    , debugInfo

    , query
    , query1
    , write
    , schema
    , batch

    , Page      (..)
    , emptyPage
    , paginate

      -- ** low-level
    , request
    , command

      -- * Retry Settings
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
    ) where

import Control.Applicative
import Control.Monad.Catch
import Control.Monad (void)
import Data.Maybe (isJust, listToMaybe)
import Database.CQL.Protocol
import Database.CQL.IO.Client
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Settings
import Database.CQL.IO.Types
import Prelude hiding (init)

runQuery :: (Tuple a, Tuple b) => QueryString k a b -> QueryParams a -> Client (Response k a b)
runQuery q p = do
    r <- request (RqQuery (Query q p))
    case r of
        RsError _ e -> throwM e
        _           -> return r

-- | Run a CQL read-only query against a Cassandra node.
query :: (Tuple a, Tuple b) => QueryString R a b -> QueryParams a -> Client [b]
query q p = do
    r <- runQuery q p
    case r of
        RsResult _ (RowsResult _ b) -> return b
        _                           -> throwM UnexpectedResponse

-- | Run a CQL read-only query against a Cassandra node.
query1 :: (Tuple a, Tuple b) => QueryString R a b -> QueryParams a -> Client (Maybe b)
query1 q p = listToMaybe <$> query q p

-- | Run a CQL insert/update query against a Cassandra node.
write :: Tuple a => QueryString W a () -> QueryParams a -> Client ()
write q p = void $ runQuery q p

-- | Run a CQL schema query against a Cassandra node.
schema :: Tuple a => QueryString S a () -> QueryParams a -> Client (Maybe SchemaChange)
schema x y = do
    r <- runQuery x y
    case r of
        RsResult _ (SchemaChangeResult s) -> return $ Just s
        RsResult _ VoidResult             -> return Nothing
        _                                 -> throwM UnexpectedResponse

-- | Run a batch query against a Cassandra node.
batch :: Batch -> Client ()
batch b = command (RqBatch b)

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
paginate :: (Tuple a, Tuple b) => QueryString R a b -> QueryParams a -> Client (Page b)
paginate q p = do
    let p' = p { pageSize = pageSize p <|> Just 10000 }
    r <- runQuery q p'
    case r of
        RsResult _ (RowsResult m b) ->
            if isJust (pagingState m) then
                return $ Page True b (paginate q p' { queryPagingState = pagingState m })
            else
                return $ Page False b (return emptyPage)
        _ -> throwM UnexpectedResponse
