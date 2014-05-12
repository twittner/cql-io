-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
import Data.ByteString.Lazy (ByteString)
import Data.Maybe (isJust)
import Data.Word
import Database.CQL.IO.Types
import Network
import Network.Socket hiding (close, connect, recv, send)
import System.Timeout

import qualified Data.ByteString.Lazy           as L
import qualified Network.Socket                 as S
import qualified Network.Socket.ByteString.Lazy as N

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
    ok <- timeout (t * 1000) (N.sendAll (sock c) s)
    unless (isJust ok) $
        throwIO SendTimeout

recv :: Int -> Int -> Connection -> IO ByteString
recv _ 0 _ = return L.empty
recv t s c = do
    bs <- timeout (t * 1000) (N.recv (sock c) (fromIntegral s))
    maybe (throwIO RecvTimeout) return bs

