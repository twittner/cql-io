-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Database.CQL.IO.Client
    ( Settings (..)
    , Pool
    , Client
    , defSettings
    , mkPool
    , runClient
    , request
    , command
    , supportedOptions
    , uncompressed
    , uncached
    , compression
    , cache
    , cacheLookup
    ) where

import Control.Applicative
import Control.Exception
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.ByteString (ByteString)
import Data.ByteString.Lazy hiding (ByteString, pack, unpack, elem)
import Data.Foldable (for_)
import Data.Monoid ((<>))
import Data.Pool hiding (Pool)
import Data.Text.Lazy (Text)
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Cache (Cache)
import Database.CQL.IO.Types
import Network
import System.IO

import qualified Data.Text.Lazy        as LT
import qualified Data.Pool             as P
import qualified Database.CQL.IO.Cache as Cache

type EventHandler = Event -> IO ()

data Settings = Settings
    { setVersion     :: CqlVersion
    , setCompression :: Compression
    , setHost        :: String
    , setPort        :: Word16
    , setKeyspace    :: Maybe Keyspace
    , setIdleTimeout :: NominalDiffTime
    , setPoolSize    :: Word32
    , cacheSize      :: Int
    , setOnEvent     :: EventHandler
    }

data Pool = Pool
    { settings   :: Settings
    , queryCache :: Maybe (Cache Text ByteString)
    , connPool   :: P.Pool Handle
    }

newtype Client a = Client
    { client :: ReaderT Pool IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadReader Pool
               )

defSettings :: Settings
defSettings = let handler = const $ return () in
    Settings Cqlv300 noCompression "localhost" 9042 Nothing 60 4 1024 handler

mkPool :: (MonadIO m) => Settings -> m Pool
mkPool s = liftIO $ do
    validateSettings
    Pool s <$> cacheInit
           <*> createPool
                connInit
                hClose
                (if setPoolSize s < 4 then 1 else 4)
                (setIdleTimeout s)
                (setPoolSize s)
  where
    validateSettings = do
        Supported ca _ <- supportedOptions (setHost s) (setPort s)
        let c = algorithm (setCompression s)
        unless (c == None || c `elem` ca) $
            throw $ UnsupportedCompression ca
        unless (cacheSize s >= 0) $
            throw InvalidCacheSize

    cacheInit =
        if cacheSize s > 0
            then Just <$> Cache.new (cacheSize s)
            else return Nothing

    connInit = do
        con <- connectTo (setHost s) (PortNumber . fromIntegral . setPort $ s)
        startup con
        for_ (setKeyspace s) $ useKeyspace con
        return con

    startup con = do
        let cmp = setCompression s
        let req = RqStartup (Startup (setVersion s) (algorithm cmp))
        send cmp con (req :: Request () () ())
        res <- intReceive cmp con :: IO (Response () () ())
        case res of
            RsEvent _ e -> setOnEvent s e
            RsError _ e -> throw e
            _           -> return ()

    useKeyspace con ks = do
        let cmp    = setCompression s
        let params = QueryParams One False () Nothing Nothing Nothing
        let kspace = quoted (LT.fromStrict $ unKeyspace ks)
        send cmp con $
            RqQuery (Query (QueryString $ "use " <> kspace) params)
        res <- intReceive cmp con :: IO (Response () () ())
        case res of
            RsResult _ (SetKeyspaceResult _) -> return ()
            RsError  _ e                     -> throw e
            _                                -> throw (UnexpectedResponse' res)


runClient :: (MonadIO m) => Pool -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

supportedOptions :: (MonadIO m) => String -> Word16 -> m Supported
supportedOptions h p = liftIO $
    bracket (connectTo h (PortNumber (fromIntegral p))) hClose $ \c -> do
        send noCompression c (RqOptions Options :: Request () () ())
        b <- intReceive noCompression c :: IO (Response () () ())
        case b of
            RsSupported _ s -> return s
            RsError     _ e -> throw e
            _               -> throw (UnexpectedResponse' b)

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    p <- asks connPool
    s <- asks settings
    liftIO $ withResource p $ \h ->
        send (setCompression s) h a >> receive s h

command :: Request k () () -> Client ()
command r = void (request r)

uncompressed :: Client a -> Client a
uncompressed m = do
    s <- asks settings
    local (\e -> e { settings = s { setCompression = noCompression } }) m

uncached :: Client a -> Client a
uncached = local (\e -> e { queryCache = Nothing })

compression :: Client Compression
compression = asks (setCompression . settings)

cache :: QueryString k a b -> QueryId k a b -> Client ()
cache (QueryString k) (QueryId v) = do
    c <- asks queryCache
    for_ c $ Cache.add k v

cacheLookup :: QueryString k a b -> Client (Maybe (QueryId k a b))
cacheLookup (QueryString k) = do
    c <- asks queryCache
    v <- maybe (return Nothing) (Cache.lookup k) c
    return (QueryId <$> v)

------------------------------------------------------------------------------
-- Internal

send :: (Tuple a) => Compression -> Handle -> Request k a b -> IO ()
send f h r = do
    let s = StreamId 1
    c <- case getOpCode r of
            OcStartup -> return noCompression
            OcOptions -> return noCompression
            _         -> return f
    a <- either (throw $ InternalError "request creation")
                return
                (pack c False s r)
    hPut h a

receive :: (Tuple a, Tuple b) => Settings -> Handle -> IO (Response k a b)
receive s h = do
    r <- intReceive (setCompression s) h
    case r of
        RsError _ e -> throw e
        RsEvent _ e -> do
            forkIO $ setOnEvent s e
            receive s h
        _           -> return r

intReceive :: (Tuple a, Tuple b) => Compression -> Handle -> IO (Response k a b)
intReceive a c = do
    b <- hGet c 8
    h <- case header b of
            Left  e -> throw $ InternalError ("response header reading: " ++ e)
            Right h -> return h
    case headerType h of
        RqHeader -> throw $ InternalError "unexpected request header"
        RsHeader -> do
            let len = lengthRepr (bodyLength h)
            x <- hGet c (fromIntegral len)
            case unpack a h x of
                Left  e -> throw $ InternalError ("response body reading: " ++ e)
                Right r -> return r

quoted :: LT.Text -> LT.Text
quoted s = "\"" <> LT.replace "\"" "\"\"" s <> "\""

