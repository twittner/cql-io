-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}

module Database.CQL.IO.Connection.Socket
    ( Socket
    , mkSock
    , open
    , send
    , recv
    , close
    , shutdown
    ) where

import Control.Monad
import Control.Monad.Catch
import Data.ByteString (ByteString)
import Data.ByteString.Builder
import Data.Maybe (isJust)
import Data.Monoid
import Database.CQL.IO.Types
import Foreign.C.Types (CInt (..))
import Network.Socket hiding (Stream, Socket, connect, close, recv, send, shutdown)
import Network.Socket.ByteString.Lazy (sendAll)
import OpenSSL
import OpenSSL.Session (SSL, SSLContext)
import System.Logger (ToBytes (..))
import System.Timeout

import qualified Data.ByteString            as Bytes
import qualified Data.ByteString.Lazy       as Lazy
import qualified Network.Socket             as S
import qualified Network.Socket.ByteString  as NB
import qualified OpenSSL.Session            as SSL

data Socket = Stream !S.Socket | Tls !S.Socket !SSL

instance ToBytes Socket where
    bytes s = bytes $ case s of
        Stream x -> fd x
        Tls  x _ -> fd x
      where
        fd x = let CInt n = S.fdSocket x in n

open :: Milliseconds -> InetAddr -> Maybe SSLContext -> IO Socket
open to a ctx = do
    bracketOnError (mkSock a) S.close $ \s -> do
        ok <- timeout (ms to * 1000) (S.connect s (sockAddr a))
        unless (isJust ok) $
            throwM (ConnectTimeout a)
        case ctx of
            Nothing  -> return (Stream s)
            Just set -> withOpenSSL $ do
                c <- SSL.connection set s
                SSL.connect c
                return (Tls s c)

mkSock :: InetAddr -> IO S.Socket
mkSock (InetAddr a) = S.socket (familyOf a) S.Stream defaultProtocol
  where
    familyOf (SockAddrInet  _ _)     = AF_INET
    familyOf (SockAddrInet6 _ _ _ _) = AF_INET6
    familyOf (SockAddrUnix  _)       = AF_UNIX
#if MIN_VERSION_network(2,6,1)
    familyOf (SockAddrCan   _      ) = AF_CAN
#endif

close :: Socket -> IO ()
close (Stream s) = S.close s
close (Tls s  c) = SSL.shutdown c SSL.Bidirectional >> S.close s

shutdown :: Socket -> ShutdownCmd -> IO ()
shutdown (Stream s) cmd = S.shutdown s cmd
shutdown _          _   = return ()

recv :: Int -> InetAddr -> Socket -> Int -> IO Lazy.ByteString
recv x a (Stream s) n = receive x a (NB.recv s) n
recv x a (Tls _  c) n = receive x a (SSL.read c) n

receive :: Int -> InetAddr -> (Int -> IO ByteString) -> Int -> IO Lazy.ByteString
receive _ _ _ 0 = return Lazy.empty
receive x i f n = toLazyByteString <$> go n mempty
  where
    go !k !bb = do
        a <- f (k `min` x)
        when (Bytes.null a) $
            throwM (ConnectionClosed i)
        let b = bb <> byteString a
        let m = k - Bytes.length a
        if m > 0 then go m b else return b

send :: Socket -> Lazy.ByteString -> IO ()
send (Stream s) b = sendAll s b
send (Tls _  c) b = mapM_ (SSL.write c) (Lazy.toChunks b)
