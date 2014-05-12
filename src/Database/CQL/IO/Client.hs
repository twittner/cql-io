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
    , shutdown
    , runClient
    , request
    , command
    , uncompressed
    , uncached
    , compression
    , cache
    , cacheLookup
    ) where

import Control.Applicative
import Control.Concurrent (forkIO)
import Control.Exception (throw, throwIO)
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Reader
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.IORef
import Data.Monoid ((<>))
import Data.Pool hiding (Pool)
import Data.Text.Lazy (Text)
import Data.Time
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Cache (Cache)
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Types
import System.Logger.Class hiding (Settings, defSettings)

import qualified Data.Text.Lazy             as LT
import qualified Data.Pool                  as P
import qualified Database.CQL.IO.Cache      as Cache
import qualified Database.CQL.IO.Connection as C
import qualified System.Logger              as Logger

type EventHandler = Event -> IO ()

data Settings = Settings
    { setVersion        :: CqlVersion
    , setCompression    :: Compression
    , setHost           :: String
    , setPort           :: Word16
    , setKeyspace       :: Maybe Keyspace
    , setIdleTimeout    :: NominalDiffTime
    , setMaxConnections :: Int
    , setMaxWaitQueue   :: Maybe Word
    , setPoolStripes    :: Int
    , setConnectTimeout :: Int
    , setRecvTimeout    :: Int
    , setSendTimeout    :: Int
    , cacheSize         :: Int
    , setOnEvent        :: EventHandler
    }

data Pool = Pool
    { settings   :: Settings
    , queryCache :: Maybe (Cache Text ByteString)
    , connPool   :: P.Pool Connection
    , logger     :: Logger.Logger
    , failures   :: IORef Word
    }

newtype Client a = Client
    { client :: ReaderT Pool IO a
    } deriving ( Functor
               , Applicative
               , Monad
               , MonadIO
               , MonadThrow
               , MonadMask
               , MonadCatch
               , MonadReader Pool
               )

instance MonadLogger Client where
    log l m = asks logger >>= \g -> Logger.log g l m

defSettings :: Settings
defSettings = let handler = const $ return () in
    Settings Cqlv300 noCompression "localhost" 9042 Nothing 60 60 Nothing 4 3000 5000 5000 1024 handler

mkPool :: MonadIO m => Logger -> Settings -> m Pool
mkPool g s = liftIO $ do
    a <- validateSettings
    Pool s <$> cacheInit
           <*> createPool (connOpen a)
                          connClose
                          (setPoolStripes s)
                          (setIdleTimeout s)
                          (setMaxConnections s)
           <*> pure g
           <*> newIORef 0
  where
    validateSettings = do
        addr <- C.resolve (setHost s) (setPort s)
        Supported ca _ <- supportedOptions addr
        let c = algorithm (setCompression s)
        unless (c == None || c `elem` ca) $
            throw $ UnsupportedCompression ca
        unless (cacheSize s >= 0) $
            throw InvalidCacheSize
        return addr

    cacheInit =
        if cacheSize s > 0
            then Just <$> Cache.new (cacheSize s)
            else return Nothing

    connOpen a = do
        Logger.debug g $ "Database.CQL.IO.Client.connOpen" .= setHost s
        c <- C.connect (setConnectTimeout s) a
        startup c
        for_ (setKeyspace s) $ useKeyspace c
        return c

    connClose c = do
        Logger.debug g $ "Database.CQL.IO.Client.connClose" .= setHost s
        C.close c

    startup con = do
        let cmp = setCompression s
            req = RqStartup (Startup (setVersion s) (algorithm cmp))
            sto = setSendTimeout s
            rto = setRecvTimeout s
        send sto cmp con (req :: Request () () ())
        res <- intReceive rto cmp con :: IO (Response () () ())
        case res of
            RsEvent _ e -> setOnEvent s e
            RsError _ e -> throw e
            _           -> return ()

    useKeyspace con ks = do
        let cmp    = setCompression s
            params = QueryParams One False () Nothing Nothing Nothing
            kspace = quoted (LT.fromStrict $ unKeyspace ks)
            sto = setSendTimeout s
            rto = setRecvTimeout s
        send sto cmp con $
            RqQuery (Query (QueryString $ "use " <> kspace) params)
        res <- intReceive rto cmp con :: IO (Response () () ())
        case res of
            RsResult _ (SetKeyspaceResult _) -> return ()
            RsError  _ e                     -> throw e
            _                                -> throw (UnexpectedResponse' res)

    supportedOptions a = bracket (C.connect (setConnectTimeout s) a) C.close $ \c -> do
        let sto = setSendTimeout s
            rto = setRecvTimeout s
        send sto noCompression c (RqOptions Options :: Request () () ())
        b <- intReceive rto noCompression c :: IO (Response () () ())
        case b of
            RsSupported _ x -> return x
            RsError     _ e -> throw e
            _               -> throw (UnexpectedResponse' b)

shutdown :: MonadIO m => Pool -> m ()
shutdown = liftIO . P.destroyAllResources . connPool

runClient :: MonadIO m => Pool -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    p <- ask
    let c = connPool p
        s = settings p
    case setMaxWaitQueue s of
        Nothing -> liftIO $ withResource c $ \h -> do
            send (setSendTimeout s) (setCompression s) h a
            receive (setRecvTimeout s) s h
        Just wq -> liftIO $ do
            r <- tryWithResource c (go s p)
            maybe (retry wq c s p) return r
  where
    go s p h = do
        atomicModifyIORef' (failures p) $ \n ->
            (if n > 0 then n - 1 else 0, ())
        send (setSendTimeout s) (setCompression s) h a
        receive (setRecvTimeout s) s h

    retry wq c s p = do
        k <- atomicModifyIORef' (failures p) $ \n -> (n + 1, n)
        unless (k < wq) $
            throwIO ConnectionsBusy
        withResource c (go s p)

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

send :: Tuple a => Int -> Compression -> Connection -> Request k a b -> IO ()
send t f h r = do
    let s = StreamId 1
    c <- case getOpCode r of
            OcStartup -> return noCompression
            OcOptions -> return noCompression
            _         -> return f
    a <- either (throw $ InternalError "request creation")
                return
                (pack c False s r)
    C.send t a h

receive :: (Tuple a, Tuple b) => Int -> Settings -> Connection -> IO (Response k a b)
receive t s h = do
    r <- intReceive t (setCompression s) h
    case r of
        RsError _ e -> throw e
        RsEvent _ e -> do
            void $ forkIO $ setOnEvent s e
            receive t s h
        _           -> return r

intReceive :: (Tuple a, Tuple b) => Int -> Compression -> Connection -> IO (Response k a b)
intReceive t a c = do
    b <- C.recv t 8 c
    h <- case header b of
            Left  e -> throw $ InternalError ("response header reading: " ++ e)
            Right h -> return h
    case headerType h of
        RqHeader -> throw $ InternalError "unexpected request header"
        RsHeader -> do
            let len = lengthRepr (bodyLength h)
            x <- C.recv t (fromIntegral len) c
            case unpack a h x of
                Left  e -> throw $ InternalError ("response body reading: " ++ e)
                Right r -> return r

quoted :: LT.Text -> LT.Text
quoted s = "\"" <> LT.replace "\"" "\"\"" s <> "\""

