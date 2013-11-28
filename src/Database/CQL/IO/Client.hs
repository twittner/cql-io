-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.CQL.IO.Client
    ( Settings (..)
    , Pool
    , Client
    , mkPool
    , runClient
    , send
    , receive
    , receive_
    , request
    , compression
    ) where

import Control.Applicative
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.ByteString.Lazy hiding (pack, unpack)
import Data.Pool hiding (Pool)
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Network
import System.IO

import qualified Data.Pool as P

data Settings = Settings
    { setVersion     :: !CqlVersion
    , setCompression :: !Compression
    , setHost        :: !String
    , setPort        :: !Word16
    , setIdleTimeout :: !NominalDiffTime
    , setPoolSize    :: !Word32
    } deriving (Show)

data Pool = Pool
    { _sets :: !Settings
    , _pool :: !(P.Pool Handle)
    }

data Env = Env
    { _compress   :: !Compression
    , _connection :: !Handle
    }

newtype Client a = Client
    { _client :: ReaderT Env IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadCatch
               , MonadIO
               , MonadReader Env
               )

mkPool :: (Functor m, MonadIO m) => Settings -> m Pool
mkPool s = liftIO $
    Pool s <$> createPool
        connInit
        hClose
        (if setPoolSize s < 4 then 1 else 4)
        (setIdleTimeout s)
        (setPoolSize s)
  where
    connInit = do
        con <- connectTo (setHost s) (PortNumber . fromIntegral . setPort $ s)
        let cmp = setCompression s
        let req = Startup (setVersion s) (algorithm cmp)
        hdr <- internalSend cmp req con
        res <- internalReceive cmp hdr con :: IO (Response () ())
        case res of
            RsError _ e -> fail (show e)
            _           -> return con

runClient :: (MonadIO m, MonadCatch m) => Pool -> Client a -> m a
runClient p a = withResource (_pool p) $ \conn ->
    liftIO (runReaderT (_client a) (Env (setCompression (_sets p)) conn))

send :: (Request a) => a -> Client Header
send r = do
    h <- asks _connection
    c <- asks _compress
    liftIO $ internalSend c r h

receive :: (Tuple a, Tuple b) => Header -> Client (Response a b)
receive r = do
    h <- asks _connection
    c <- asks _compress
    liftIO $ internalReceive c r h

receive_ :: Header -> Client (Response () ())
receive_ = receive

request :: (Request r, Tuple a, Tuple b) => r -> Client (Response a b)
request = send >=> receive

compression :: Client Compression
compression = asks _compress

------------------------------------------------------------------------------
-- Internal

internalSend :: (Request a) => Compression -> a -> Handle -> IO Header
internalSend f r h = do
    let s = StreamId 1
    c <- case getOpCode r rqCode of
        OcStartup -> return noCompression
        OcOptions -> return noCompression
        _         -> return f
    a <- either (fail "failed to create request") return (pack c False s r)
    hPut h a
    b <- hGet h 8
    case header b of
        Left  e -> fail $ "failed to read response header: " ++ e
        Right x -> return x

internalReceive :: (Tuple a, Tuple b) => Compression -> Header -> Handle -> IO (Response a b)
internalReceive a h c = case headerType h of
    RqHeader -> fail "unexpected request header"
    RsHeader -> do
        let len = lengthRepr (bodyLength h)
        b <- hGet c (fromIntegral len)
        case unpack a h b of
            Left  e -> fail $ "failed to read response body: " ++ e
            Right x -> return x

