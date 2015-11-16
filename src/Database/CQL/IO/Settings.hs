-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Database.CQL.IO.Settings where

import Control.Lens hiding ((<|))
import Control.Retry hiding (retryPolicy)
import Data.List.NonEmpty (NonEmpty (..), (<|))
import Data.Monoid
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Connection
import Database.CQL.IO.Cluster.Policies (Policy, random)
import Database.CQL.IO.Connection as C
import Database.CQL.IO.Pool as P
import Database.CQL.IO.Types (Milliseconds (..))
import Network.Socket (PortNumber (..))
import OpenSSL.Session (SSLContext)
import Prelude

import {-# SOURCE #-} Database.CQL.IO.Client

data PrepareStrategy
    = EagerPrepare -- ^ cluster-wide preparation
    | LazyPrepare  -- ^ on-demand per node preparation
    deriving (Eq, Ord, Show)

data RetrySettings = RetrySettings
    { _retryPolicy        :: !(RetryPolicyM Client)
    , _reducedConsistency :: !(Maybe Consistency)
    , _sendTimeoutChange  :: !Milliseconds
    , _recvTimeoutChange  :: !Milliseconds
    }

data Settings = Settings
    { _poolSettings  :: !PoolSettings
    , _connSettings  :: !ConnectionSettings
    , _retrySettings :: !RetrySettings
    , _protoVersion  :: !Version
    , _portnumber    :: !PortNumber
    , _contacts      :: !(NonEmpty String)
    , _policyMaker   :: !(IO Policy)
    , _prepStrategy  :: !PrepareStrategy
    }

makeLenses ''RetrySettings
makeLenses ''Settings

-- | Default settings:
--
-- * contact point is \"localhost\" port 9042
--
-- * load-balancing policy is 'random'
--
-- * binary protocol version is 3 (supported by Cassandra >= 2.1.0)
--
-- * connection idle timeout is 60s
--
-- * the connection pool uses 4 stripes to mitigate thread contention
--
-- * connections use a connect timeout of 5s, a send timeout of 3s and
-- a receive timeout of 10s
--
-- * 128 streams per connection are used
--
-- * 16k receive buffer size
--
-- * no compression is applied to frame bodies
--
-- * no default keyspace is used.
--
-- * no retries are done
--
-- * lazy prepare strategy
defSettings :: Settings
defSettings = Settings
    P.defSettings
    C.defSettings
    noRetry
    V3
    (fromInteger 9042)
    ("localhost" :| [])
    random
    LazyPrepare

-----------------------------------------------------------------------------
-- Settings

-- | Set the binary protocol version to use.
setProtocolVersion :: Version -> Settings -> Settings
setProtocolVersion v = set protoVersion v

-- | Set the initial contact points (hosts) from which node discovery will
-- start.
setContacts :: String -> [String] -> Settings -> Settings
setContacts v vv = set contacts (v :| vv)

-- | Add an additional host to the contact list.
addContact :: String -> Settings -> Settings
addContact v = over contacts (v <|)

-- | Set the portnumber to use to connect on /every/ node of the cluster.
setPortNumber :: PortNumber -> Settings -> Settings
setPortNumber v = set portnumber v

-- | Set the load-balancing policy.
setPolicy :: IO Policy -> Settings -> Settings
setPolicy v = set policyMaker v

-- | Set strategy to use for preparing statements.
setPrepareStrategy :: PrepareStrategy -> Settings -> Settings
setPrepareStrategy v = set prepStrategy v

-----------------------------------------------------------------------------
-- Pool Settings

-- | Set the connection idle timeout. Connections in a pool will be closed
-- if not in use for longer than this timeout.
setIdleTimeout :: NominalDiffTime -> Settings -> Settings
setIdleTimeout v = set (poolSettings.idleTimeout) v

-- | Maximum connections per pool /stripe/.
setMaxConnections :: Int -> Settings -> Settings
setMaxConnections v = set (poolSettings.maxConnections) v

-- | Set the number of pool stripes to use. A good setting is equal to the
-- number of CPU cores this codes is running on.
setPoolStripes :: Int -> Settings -> Settings
setPoolStripes v s
    | v < 1     = error "cql-io settings: stripes must be greater than 0"
    | otherwise = set (poolSettings.poolStripes) v s

-- | When receiving a response times out, we can no longer use the stream of the
-- connection that was used to make the request as it is uncertain if
-- a response will arrive later. Thus the bandwith of a connection will be
-- decreased. This settings defines a threshold after which we close the
-- connection to get a new one with all streams available.
setMaxTimeouts :: Int -> Settings -> Settings
setMaxTimeouts v = set (poolSettings.maxTimeouts) v

-----------------------------------------------------------------------------
-- Connection Settings

-- | Set the compression to use for frame body compression.
setCompression :: Compression -> Settings -> Settings
setCompression v = set (connSettings.compression) v

-- | Set the maximum number of streams per connection. In version 2 of the
-- binary protocol at most 128 streams can be used. Version 3 supports up
-- to 32768 streams.
setMaxStreams :: Int -> Settings -> Settings
setMaxStreams v s = case s^.protoVersion of
    V2 | v < 1 || v > 128   -> error "cql-io settings: max. streams must be within [1, 128]"
    V3 | v < 1 || v > 32768 -> error "cql-io settings: max. streams must be within [1, 32768]"
    _                       -> set (connSettings.maxStreams) v s

-- | Set the connect timeout of a connection.
setConnectTimeout :: NominalDiffTime -> Settings -> Settings
setConnectTimeout v = set (connSettings.connectTimeout) (Ms $ round (1000 * v))

-- | Set the send timeout of a connection. Request exceeding the send will
-- cause the connection to be closed and fail with 'ConnectionClosed'
-- exception.
setSendTimeout :: NominalDiffTime -> Settings -> Settings
setSendTimeout v = set (connSettings.sendTimeout) (Ms $ round (1000 * v))

-- | Set the receive timeout of a connection. Requests exceeding the
-- receive timeout will fail with a 'Timeout' exception.
setResponseTimeout :: NominalDiffTime -> Settings -> Settings
setResponseTimeout v = set (connSettings.responseTimeout) (Ms $ round (1000 * v))

-- | Set the default keyspace to use. Every new connection will be
-- initialised to use this keyspace.
setKeyspace :: Keyspace -> Settings -> Settings
setKeyspace v = set (connSettings.defKeyspace) (Just v)

-- | Set default retry settings to use.
setRetrySettings :: RetrySettings -> Settings -> Settings
setRetrySettings v = set retrySettings v

-- | Set maximum receive buffer size.
--
-- The actual buffer size used will be the minimum of the CQL response size
-- and the value set here.
setMaxRecvBuffer :: Int -> Settings -> Settings
setMaxRecvBuffer v = set (connSettings.maxRecvBuffer) v

-- | Set a fully configured SSL context.
--
-- This will make client server queries use TLS.
setSSLContext :: SSLContext -> Settings -> Settings
setSSLContext v = set (connSettings.tlsContext) (Just v)

-----------------------------------------------------------------------------
-- Retry Settings

-- | Never retry.
noRetry :: RetrySettings
noRetry = RetrySettings (RetryPolicyM $ const (return Nothing)) Nothing 0 0

-- | Forever retry immediately.
retryForever :: RetrySettings
retryForever = RetrySettings mempty Nothing 0 0

-- | Limit number of retries.
maxRetries :: Word -> RetrySettings -> RetrySettings
maxRetries v = over retryPolicy (mappend (limitRetries $ fromIntegral v))

-- | When retrying a (batch-) query, change consistency to the given value.
adjustConsistency :: Consistency -> RetrySettings -> RetrySettings
adjustConsistency v = set reducedConsistency (Just v)

-- | Wait a constant time between retries.
constDelay :: NominalDiffTime -> RetrySettings -> RetrySettings
constDelay v = setDelayFn constantDelay v v

-- | Delay retries with exponential backoff.
expBackoff :: NominalDiffTime
           -- ^ Initial delay.
           -> NominalDiffTime
           -- ^ Maximum delay.
           -> RetrySettings
           -> RetrySettings
expBackoff = setDelayFn exponentialBackoff

-- | Delay retries using Fibonacci sequence as backoff.
fibBackoff :: NominalDiffTime
           -- ^ Initial delay.
           -> NominalDiffTime
           -- ^ Maximum delay.
           -> RetrySettings
           -> RetrySettings
fibBackoff = setDelayFn fibonacciBackoff

-- | On retry adjust the send timeout.
adjustSendTimeout :: NominalDiffTime -> RetrySettings -> RetrySettings
adjustSendTimeout v = set sendTimeoutChange (Ms $ round (1000 * v))

-- | On retry adjust the response timeout.
adjustResponseTimeout :: NominalDiffTime -> RetrySettings -> RetrySettings
adjustResponseTimeout v = set recvTimeoutChange (Ms $ round (1000 * v))

setDelayFn :: (Int -> RetryPolicyM Client)
           -> NominalDiffTime
           -> NominalDiffTime
           -> RetrySettings
           -> RetrySettings
setDelayFn d v w = over retryPolicy
    (mappend $ capDelay (round (1000000 * w)) $ d (round (1000000 * v)))
