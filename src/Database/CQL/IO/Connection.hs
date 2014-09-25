-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Connection
    ( Connection
    , resolve
    , connect
    , close
    , request
    ) where

import Control.Applicative
import Control.Concurrent (myThreadId)
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Data.ByteString.Builder
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Maybe (isJust)
import Data.Monoid
import Data.Unique
import Data.Vector (Vector, (!))
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Hexdump
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Sync (Sync)
import Database.CQL.IO.Types
import Database.CQL.IO.Tickets (Pool, toInt, markAvailable)
import Database.CQL.IO.Timeouts (TimeoutManager, withTimeout)
import Foreign.C.Types (CInt (..))
import Network
import Network.Socket hiding (connect, close, recv, send)
import Network.Socket.ByteString.Lazy (sendAll)
import System.IO (nativeNewline, Newline (..))
import System.Logger hiding (Settings, close)
import System.Timeout

import qualified Data.ByteString           as B
import qualified Data.ByteString.Lazy      as L
import qualified Data.Vector               as Vector
import qualified Database.CQL.IO.Sync      as Sync
import qualified Database.CQL.IO.Tickets   as Tickets
import qualified Network.Socket            as S
import qualified Network.Socket.ByteString as NB

type Streams = Vector (Sync (Header, ByteString))

data Connection = Connection
    { sock    :: !Socket
    , streams :: Streams
    , wLock   :: MVar ()
    , reader  :: Async ()
    , tickets :: !Pool
    , logger  :: !Logger
    , ident   :: !Unique
    }

instance Eq Connection where
    a == b = ident a == ident b

instance Show Connection where
    show c = "Connection" ++ show (sock c)

resolve :: String -> Word16 -> IO AddrInfo
resolve host port =
    head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Settings -> Logger -> AddrInfo -> Compression -> EventHandler -> IO Connection
connect t g a c f =
    bracketOnError mkSock S.close $ \s -> do
        ok <- timeout (ms (sConnectTimeout t) * 1000) (S.connect s (addrAddress a))
        unless (isJust ok) $
            throwIO ConnectTimeout
        open s
  where
    mkSock = socket (addrFamily a) (addrSocketType a) (addrProtocol a)
    open s = do
        tck <- Tickets.pool (sMaxStreams t)
        syn <- Vector.replicateM (sMaxStreams t) Sync.create
        lck <- newMVar ()
        rdr <- async (runReader (sProtoVersion t) g c f tck s syn)
        Connection s syn lck rdr tck g <$> newUnique

runReader :: Version -> Logger -> Compression -> EventHandler -> Pool -> Socket -> Streams -> IO ()
runReader v g cmp fn tck sck syn = run `finally` cleanup
  where
    run = forever $ do
        x <- readSocket v g sck
        case fromStreamId $ streamId (fst x) of
            -1 ->
                case parse cmp x :: Response () () () of
                    RsError _ e -> throwIO e
                    RsEvent _ e -> fn e
                    r           -> throwIO (UnexpectedResponse' r)
            sid -> do
                ok <- Sync.put (syn ! sid) x
                unless ok $
                    markAvailable tck sid

    cleanup = do
        Tickets.close tck
        Vector.mapM_ Sync.close syn
        S.close sck

close :: Connection -> IO ()
close = cancel . reader

request :: Settings -> TimeoutManager -> Connection -> (Int -> ByteString) -> IO (Header, ByteString)
request s t c f = send >>= receive
  where
    send = withTimeout t (sSendTimeout s) (close c) $ do
        i <- toInt <$> Tickets.get (tickets c)
        let req = f i
        trace (logger c) $ "socket" .= fd (sock c)
            ~~ "stream" .= i
            ~~ "type"   .= val "request"
            ~~ msg' (hexdump (L.take 160 req))
        withMVar (wLock c) $
            const $ sendAll (sock c) req
        return i

    receive i = do
        let e = Timeout (show c ++ ":" ++ show i)
        tid <- myThreadId
        withTimeout t (sResponseTimeout s) (throwTo tid e) $ do
            x <- Sync.get (streams c ! i) `onException` Sync.kill (streams c ! i)
            markAvailable (tickets c) i
            return x

readSocket :: Version -> Logger -> Socket -> IO (Header, ByteString)
readSocket v g s = do
    b <- recv 8 s
    h <- case header v b of
            Left  e -> throwIO $ InternalError ("response header reading: " ++ e)
            Right h -> return h
    case headerType h of
        RqHeader -> throwIO $ InternalError "unexpected request header"
        RsHeader -> do
            let len = lengthRepr (bodyLength h)
            x <- recv (fromIntegral len) s
            trace g $ "socket" .= fd s
                ~~ "stream" .= fromStreamId (streamId h)
                ~~ "type"   .= val "response"
                ~~ msg' (hexdump $ L.take 160 (b <> x))
            return (h, x)

recv :: Int -> Socket -> IO ByteString
recv 0 _ = return L.empty
recv n c = toLazyByteString <$> go 0 mempty
  where
    go !k !bb = do
        a <- NB.recv c (n - k)
        when (B.null a) $
            throwIO ConnectionClosed
        let b = bb <> byteString a
            m = B.length a + k
        if m < n then go m b else return b

-- logging helpers:

fd :: Socket -> Int32
fd !s = let CInt !n = fdSocket s in n

msg' :: ByteString -> Msg -> Msg
msg' x = msg $ case nativeNewline of
    LF   -> val "\n"   +++ x
    CRLF -> val "\r\n" +++ x
