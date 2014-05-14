-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns #-}

module Database.CQL.IO.Connection
    ( Connection
    , resolve
    , connect
    , close
    , send
    , recv
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Data.ByteString.Builder
import Data.ByteString.Lazy (ByteString)
import Data.Maybe (isJust)
import Data.Monoid
import Data.Word
import Database.CQL.IO.Types (Timeout (..))
import Network
import Network.Socket hiding (connect, close, send, recv)
import System.Timeout

import qualified Data.ByteString                as B
import qualified Data.ByteString.Lazy           as L
import qualified Network.Socket                 as S
import qualified Network.Socket.ByteString      as NB
import qualified Network.Socket.ByteString.Lazy as NL

newtype Connection = Connection { sock :: Socket }

resolve :: String -> Word16 -> IO AddrInfo
resolve host port =
    head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Int -> AddrInfo -> IO Connection
connect t a =
    bracketOnError mkSock S.close $ \s -> do
        ok <- timeout (t * 1000) (S.connect s (addrAddress a))
        unless (isJust ok) $
            throwIO ConnectTimeout
        return (Connection s)
  where
    mkSock = socket (addrFamily a) (addrSocketType a) (addrProtocol a)

close :: Connection -> IO ()
close = S.close . sock

send :: Int -> ByteString -> Connection -> IO ()
send t s c = do
    ok <- timeout (t * 1000) (NL.sendAll (sock c) s)
    unless (isJust ok) $
        throwIO SendTimeout

recv :: Int -> Int -> Connection -> IO ByteString
recv _ 0 _ = return L.empty
recv t n c = do
    bs <- timeout (t * 1000) (toLazyByteString <$> go 0 mempty)
    maybe (throwIO ReceiveTimeout) return bs
  where
    go !k !bytes = do
        a <- NB.recv (sock c) (n - k)
        if B.null a
            then return bytes
            else let b = bytes <> byteString a
                     m = B.length a + k
                 in if m < n then go m b else return b

