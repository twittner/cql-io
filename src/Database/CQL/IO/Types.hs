-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Database.CQL.IO.Types where

import Control.Monad.Catch
import Data.IP
import Data.Typeable
import Database.CQL.Protocol (Event, Response, CompressionAlgorithm)
import Network.Socket (SockAddr (..), PortNumber)

type EventHandler = Event -> IO ()

newtype Milliseconds = Ms { ms :: Int } deriving (Eq, Show, Num)

-----------------------------------------------------------------------------
-- InetAddr

newtype InetAddr = InetAddr { sockAddr :: SockAddr } deriving (Eq, Ord)

instance Show InetAddr where
    show (InetAddr (SockAddrInet p a)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrInet6 p _ a _)) =
        let i = fromIntegral p :: Int in
        shows (fromHostAddress6 a) . showString ":" . shows i $ ""
    show (InetAddr (SockAddrUnix unix)) = unix

ip2inet :: PortNumber -> IP -> InetAddr
ip2inet p (IPv4 a) = InetAddr $ SockAddrInet p (toHostAddress a)
ip2inet p (IPv6 a) = InetAddr $ SockAddrInet6 p 0 (toHostAddress6 a) 0

inet2ip :: InetAddr -> IP
inet2ip (InetAddr (SockAddrInet _ a))      = IPv4 (fromHostAddress a)
inet2ip (InetAddr (SockAddrInet6 _ _ a _)) = IPv6 (fromHostAddress6 a)
inet2ip _                                  = error "inet2Ip: not IP4/IP6 address"

-----------------------------------------------------------------------------
-- InvalidSettings

data InvalidSettings
    = UnsupportedCompression [CompressionAlgorithm]
    | InvalidCacheSize
    deriving Typeable

instance Exception InvalidSettings

instance Show InvalidSettings where
    show (UnsupportedCompression cc) =
        "Database.CQL.IO.UnsupportedCompression: " ++ show cc
    show InvalidCacheSize = "Database.CQL.IO.InvalidCacheSize"

-----------------------------------------------------------------------------
-- InternalError

data InternalError = InternalError String
    deriving Typeable

instance Exception InternalError

instance Show InternalError where
    show (InternalError e) = "Database.CQL.IO.InternalError: " ++ show e

-----------------------------------------------------------------------------
-- HostError

data HostError
    = NoHostAvailable
    deriving Typeable

instance Exception HostError

instance Show HostError where
    show NoHostAvailable = "Database.CQL.IO.NoHostAvailable"

-----------------------------------------------------------------------------
-- ConnectionError

data ConnectionError
    = ConnectionsBusy
    | ConnectionClosed
    | ConnectTimeout
    deriving Typeable

instance Exception ConnectionError

instance Show ConnectionError where
    show ConnectionsBusy   = "Database.CQL.IO.ConnectionsBusy"
    show ConnectionClosed  = "Database.CQL.IO.ConnectionClosed"
    show ConnectTimeout    = "Database.CQL.IO.ConnectTimeout"

-----------------------------------------------------------------------------
-- Timeout

data Timeout = Timeout String
    deriving Typeable

instance Exception Timeout

instance Show Timeout where
    show (Timeout e) = "Database.CQL.IO.Timeout: " ++ e

-----------------------------------------------------------------------------
-- UnexpectedResponse

data UnexpectedResponse where
    UnexpectedResponse  :: UnexpectedResponse
    UnexpectedResponse' :: (Show b) => Response k a b -> UnexpectedResponse

deriving instance Typeable UnexpectedResponse
instance Exception UnexpectedResponse

instance Show UnexpectedResponse where
    show UnexpectedResponse      = "Database.CQL.IO.UnexpectedResponse"
    show (UnexpectedResponse' r) = "Database.CQL.IO.UnexpectedResponse: " ++ show r

ignore :: IO () -> IO ()
ignore a = catchAll a (const $ return ())
{-# INLINE ignore #-}
