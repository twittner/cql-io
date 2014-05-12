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
import Data.ByteString.Lazy (ByteString)
import Data.Word
import Network
import Network.Socket.Eager (Descriptor, Milliseconds (..))
import Network.Socket hiding (connect, close, send, recv)

import qualified Network.Socket       as Net
import qualified Network.Socket.Eager as Eager

data Connection = Connection
    { sock :: !Socket
    , desc :: !Descriptor
    }

resolve :: String -> Word16 -> IO AddrInfo
resolve host port =
    head <$> getAddrInfo (Just hints) (Just host) (Just (show port))
  where
    hints = defaultHints { addrFlags = [AI_ADDRCONFIG], addrSocketType = Stream }

connect :: Int -> AddrInfo -> IO Connection
connect t a =
    bracketOnError mkSock Net.close $ \s -> do
        let d = Eager.descriptor s
        Eager.connect' (Milliseconds t) (addrAddress a) d
        return (Connection s d)
  where
    mkSock = socket (addrFamily a) (addrSocketType a) (addrProtocol a)

close :: Connection -> IO ()
close = Net.close . sock

send :: Int -> ByteString -> Connection -> IO ()
send t s c = Eager.send' (Milliseconds t) s (desc c) (sock c)

recv :: Int -> Int -> Connection -> IO ByteString
recv t s c = Eager.recv' (Milliseconds t) s (desc c) (sock c)

