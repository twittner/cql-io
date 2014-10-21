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
    , shutdown
    , debugInfo

    , query
    , write
    , schema
    , batch

      -- ** low-level
    , request
    , command

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

import Control.Monad.Catch
import Control.Monad (void)
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
