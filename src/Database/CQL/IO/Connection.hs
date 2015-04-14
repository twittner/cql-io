-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.IO.Connection
    ( Connection
    , resolve
    , ping
    , connect
    , close
    , request
    , startup
    , register
    , query
    , useKeyspace
    , address
    , protocol
    , eventSig

    , ConnectionSettings
    , defSettings
    , connectTimeout
    , sendTimeout
    , responseTimeout
    , maxStreams
    , compression
    , defKeyspace
    , maxRecvBuffer
    ) where

import Control.Applicative
import Control.Concurrent (myThreadId)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Exception (throwTo, AsyncException (ThreadKilled))
import Control.Lens ((^.), makeLenses, view)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.ByteString.Builder
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Maybe (isJust, fromMaybe)
import Data.Monoid
import Data.Text.Lazy (fromStrict)
import Data.Unique
import Data.Vector (Vector, (!))
import Database.CQL.Protocol
import Database.CQL.IO.Hexdump
import Database.CQL.IO.Protocol
import Database.CQL.IO.Signal hiding (connect)
import Database.CQL.IO.Sync (Sync)
import Database.CQL.IO.Types
import Database.CQL.IO.Tickets (Pool, toInt, markAvailable)
import Database.CQL.IO.Timeouts (TimeoutManager, withTimeout)
import Foreign.C.Types (CInt (..))
import Network
import Network.Socket hiding (connect, close, recv, send)
import Network.Socket.ByteString.Lazy (sendAll)
import System.IO (nativeNewline, Newline (..))
import System.IO.Error (mkIOError, illegalOperationErrorType)
import System.Logger hiding (Settings, close, defSettings, settings)
import System.Timeout
import Prelude

import qualified Data.ByteString            as B
import qualified Data.ByteString.Lazy       as L
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Vector                as Vector
import qualified Database.CQL.IO.Sync       as Sync
import qualified Database.CQL.IO.Tickets    as Tickets
import qualified Network.Socket             as S
import qualified Network.Socket.ByteString  as NB

data ConnectionSettings = ConnectionSettings
    { _connectTimeout  :: !Milliseconds
    , _sendTimeout     :: !Milliseconds
    , _responseTimeout :: !Milliseconds
    , _maxStreams      :: !Int
    , _compression     :: !Compression
    , _defKeyspace     :: !(Maybe Keyspace)
    , _maxRecvBuffer   :: !Int
    }

type Streams = Vector (Sync (Header, ByteString))

data SocketState = SocketAlive | SocketDead

data Connection = Connection
    { _settings :: !ConnectionSettings
    , _address  :: !InetAddr
    , _tmanager :: !TimeoutManager
    , _protocol :: !Version
    , _sock     :: !Socket
    , _streams  :: !Streams
    , _wLock    :: !(MVar SocketState)
    , _reader   :: !(Async ())
    , _tickets  :: !Pool
    , _logger   :: !Logger
    , _eventSig :: !(Signal Event)
    , _ident    :: !Unique
    }

makeLenses ''ConnectionSettings
makeLenses ''Connection

instance Eq Connection where
    a == b = a^.ident == b^.ident

instance Show Connection where
    show = Char8.unpack . eval . bytes

instance ToBytes Connection where
    bytes c = bytes (c^.address) +++ val "#" +++ fd (c^.sock)

defSettings :: ConnectionSettings
defSettings =
    ConnectionSettings 5000          -- connect timeout
                       3000          -- send timeout
                       10000         -- response timeout
                       128           -- max streams per connection
                       noCompression -- compression
                       Nothing       -- keyspace
                       16384         -- receive buffer size

resolve :: String -> PortNumber -> IO InetAddr
resolve host port =
    InetAddr . addrAddress . head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: MonadIO m => ConnectionSettings -> TimeoutManager -> Version -> Logger -> InetAddr -> m Connection
connect t m v g a = liftIO $ do
    c <- bracketOnError (mkSock a) S.close $ \s -> do
        ok <- timeout (ms (t^.connectTimeout) * 1000) (S.connect s (sockAddr a))
        unless (isJust ok) $
            throwM (ConnectTimeout a)
        open s -- once this has happened, the reader thread owns the socket, so bracketOnError has to end
    validateSettings c `onException` cancel (c^.reader)
    return c
  where
    open s = do
        tck <- Tickets.pool (t^.maxStreams)
        syn <- Vector.replicateM (t^.maxStreams) Sync.create
        lck <- newMVar SocketAlive
        sig <- signal
        rdr <- async (readLoop v g t tck a s syn sig lck)
        Connection t a m v s syn lck rdr tck g sig <$> newUnique

mkSock :: InetAddr -> IO Socket
mkSock (InetAddr a) = socket (familyOf a) Stream defaultProtocol
  where
    familyOf (SockAddrInet  _ _)     = AF_INET
    familyOf (SockAddrInet6 _ _ _ _) = AF_INET6
    familyOf (SockAddrUnix  _)       = AF_UNIX

ping :: MonadIO m => InetAddr -> m Bool
ping a = liftIO $ bracket (mkSock a) S.close $ \s ->
    fromMaybe False <$> timeout 5000000
        ((S.connect s (sockAddr a) >> return True) `catchAll` const (return False))

readLoop :: Version -> Logger -> ConnectionSettings -> Pool -> InetAddr -> Socket -> Streams -> Signal Event -> MVar SocketState -> IO ()
readLoop v g set tck i sck syn s lck = run `catch` logException `finally` cleanup
  where
    run = forever $ do
        x <- readSocket v g i sck (set^.maxRecvBuffer)
        case fromStreamId $ streamId (fst x) of
            -1 ->
                case parse (set^.compression) x :: Response () () () of
                    RsError _ e -> throwM e
                    RsEvent _ e -> emit s e
                    r           -> throwM (UnexpectedResponse' r)
            sid -> do
                ok <- Sync.put x (syn ! sid)
                unless ok $
                    markAvailable tck sid

    -- This doesn't have to happen immediately (just "soon"); running
    -- it asynchronously prevents FD leaks in a scenario where the
    -- 'run' loop terminates _just_ before 'close' happens.  If the
    -- cancel performed by 'close' were to happen while the reader is
    -- blocked in modifyMVar_ (I don't believe Tickets.close nor
    -- Sync.close can block), the socket would be leaked.
    cleanup = async $ do
        Tickets.close (ConnectionClosed i) tck
        Vector.mapM_ (Sync.close (ConnectionClosed i)) syn
        S.shutdown sck S.ShutdownBoth
        modifyMVar_ lck $ \_ -> do
            S.close sck
            return SocketDead

    logException :: SomeException -> IO ()
    logException e = case fromException e of
        Just ThreadKilled -> return ()
        _                 -> warn g $ msg i ~~ msg (val "read-loop: " +++ show e)

close :: Connection -> IO ()
close = cancel . view reader

request :: Connection -> (Int -> ByteString) -> IO (Header, ByteString)
request c f = send >>= receive
  where
    send = withTimeout (c^.tmanager) (c^.settings.sendTimeout) (close c) $ do
        i <- toInt <$> Tickets.get (c^.tickets)
        let req = f i
        trace (c^.logger) $ msg c
            ~~ "stream" .= i
            ~~ "type"   .= val "request"
            ~~ msg' (hexdump (L.take 160 req))
        withMVar (c^.wLock) $ \ss ->
            case ss of
                SocketAlive -> sendAll (c^.sock) req
                SocketDead  -> throwM $ mkIOError illegalOperationErrorType "Socket closed" Nothing Nothing
        return i

    receive i = do
        let e = TimeoutRead (show c ++ ":" ++ show i)
        tid <- myThreadId
        withTimeout (c^.tmanager) (c^.settings.responseTimeout) (throwTo tid e) $ do
            x <- Sync.get (view streams c ! i) `onException` (Sync.kill e) (view streams c ! i)
            markAvailable (c^.tickets) i
            return x

readSocket :: Version -> Logger -> InetAddr -> Socket -> Int -> IO (Header, ByteString)
readSocket v g i s n = do
    b <- recv n i s (if v == V3 then 9 else 8)
    h <- case header v b of
            Left  e -> throwM $ InternalError ("response header reading: " ++ e)
            Right h -> return h
    case headerType h of
        RqHeader -> throwM $ InternalError "unexpected request header"
        RsHeader -> do
            let len = lengthRepr (bodyLength h)
            x <- recv n i s (fromIntegral len)
            trace g $ msg (i +++ val "#" +++ fd s)
                ~~ "stream" .= fromStreamId (streamId h)
                ~~ "type"   .= val "response"
                ~~ msg' (hexdump $ L.take 160 (b <> x))
            return (h, x)

recv :: Int -> InetAddr -> Socket -> Int -> IO ByteString
recv _ _ _ 0 = return L.empty
recv x i s n = toLazyByteString <$> go n mempty
  where
    go !k !bb = do
        a <- NB.recv s (k `min` x)
        when (B.null a) $
            throwM (ConnectionClosed i)
        let b = bb <> byteString a
        let m = k - B.length a
        if m > 0 then go m b else return b

-----------------------------------------------------------------------------
-- Operations

startup :: MonadIO m => Connection -> m ()
startup c = liftIO $ do
    let cmp = c^.settings.compression
    let req = RqStartup (Startup Cqlv300 (algorithm cmp))
    let enc = serialise (c^.protocol) cmp (req :: Request () () ())
    res <- request c enc
    (parse cmp res :: Response () () ()) `seq` return ()

register :: MonadIO m => Connection -> [EventType] -> EventHandler -> m ()
register c e f = liftIO $ do
    let req = RqRegister (Register e) :: Request () () ()
    let enc = serialise (c^.protocol) (c^.settings.compression) req
    res <- request c enc
    case parse (c^.settings.compression) res :: Response () () () of
        RsReady _ Ready -> c^.eventSig |-> f
        other           -> throwM (UnexpectedResponse' other)

validateSettings :: MonadIO m => Connection -> m ()
validateSettings c = liftIO $ do
    Supported ca _ <- supportedOptions c
    let x = algorithm (c^.settings.compression)
    unless (x == None || x `elem` ca) $
        throwM $ UnsupportedCompression ca

supportedOptions :: MonadIO m => Connection -> m Supported
supportedOptions c = liftIO $ do
    let options = RqOptions Options :: Request () () ()
    res <- request c (serialise (c^.protocol) noCompression options)
    case parse noCompression res :: Response () () () of
        RsSupported _ x -> return x
        other           -> throwM (UnexpectedResponse' other)

useKeyspace :: MonadIO m => Connection -> Keyspace -> m ()
useKeyspace c ks = liftIO $ do
    let cmp    = c^.settings.compression
        params = QueryParams One False () Nothing Nothing Nothing
        kspace = quoted (fromStrict $ unKeyspace ks)
        req    = RqQuery (Query (QueryString $ "use " <> kspace) params)
    res <- request c (serialise (c^.protocol) cmp req)
    case parse cmp res :: Response () () () of
        RsResult _ (SetKeyspaceResult _) -> return ()
        other                            -> throwM (UnexpectedResponse' other)

query :: forall k a b m. (Tuple a, Tuple b, Show b, MonadIO m)
      => Connection
      -> Consistency
      -> QueryString k a b
      -> a
      -> m [b]
query c cons q p = liftIO $ do
    let req = RqQuery (Query q params) :: Request k a b
    let enc = serialise (c^.protocol) (c^.settings.compression) req
    res <- request c enc
    case parse (c^.settings.compression) res :: Response k a b of
        RsResult _ (RowsResult _ b) -> return b
        other                       -> throwM (UnexpectedResponse' other)
  where
    params = QueryParams cons False p Nothing Nothing Nothing

-- logging helpers:

fd :: Socket -> Int32
fd !s = let CInt !n = fdSocket s in n

msg' :: ByteString -> Msg -> Msg
msg' x = msg $ case nativeNewline of
    LF   -> val "\n"   +++ x
    CRLF -> val "\r\n" +++ x
