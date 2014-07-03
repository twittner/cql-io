-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Database.CQL.IO.Client
    ( Pool
    , Client
    , mkPool
    , shutdown
    , runClient
    , request
    , command
    , uncompressed
    , compression
    ) where

import Control.Applicative
import Control.Concurrent (forkIO)
import Control.Exception (throw, throwIO)
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Reader
import Data.Foldable (for_)
import Data.IORef
import Data.Monoid ((<>))
import Data.Pool hiding (Pool)
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager, Milliseconds (..))
import Database.CQL.IO.Types
import System.Logger.Class hiding (Settings, defSettings)

import qualified Data.Text.Lazy             as LT
import qualified Data.Pool                  as P
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM
import qualified System.Logger              as Logger

data Pool = Pool
    { settings   :: Settings
    , connPool   :: P.Pool Connection
    , logger     :: Logger.Logger
    , failures   :: IORef Word64
    , timeouts   :: TimeoutManager
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

mkPool :: MonadIO m => Logger -> Settings -> m Pool
mkPool g s = liftIO $ do
    a <- validateSettings
    t <- TM.create (Ms 500)
    Pool s <$> createPool (connOpen t a)
                          connClose
                          (sPoolStripes s)
                          (sIdleTimeout s)
                          (sMaxConnections s)
           <*> pure g
           <*> newIORef 0
           <*> pure t
  where
    validateSettings = do
        addr <- C.resolve (sHost s) (sPort s)
        Supported ca _ <- supportedOptions addr
        let c = algorithm (sCompression s)
        unless (c == None || c `elem` ca) $
            throw $ UnsupportedCompression ca
        return addr

    connOpen t a = do
        Logger.debug g $ "Client.connOpen" .= sHost s
        c <- C.connect (sConnectTimeout s) a
        x <- TM.add t (Ms $ sSendRecvTimeout s * 2) (C.close c)
        connInit c `finally` TM.cancel x

    connInit c = flip onException (C.close c) $ do
        startup c
        for_ (sKeyspace s) $
            useKeyspace c
        return c

    connClose c = do
        Logger.debug g $ "Client.connClose" .= sHost s
        C.close c

    startup con = do
        let cmp = sCompression s
            req = RqStartup (Startup (sVersion s) (algorithm cmp))
        send cmp con (req :: Request () () ())
        res <- intReceive cmp con :: IO (Response () () ())
        case res of
            RsEvent _ e -> sOnEvent s e
            RsError _ e -> throw e
            _           -> return ()

    useKeyspace con ks = do
        let cmp    = sCompression s
            params = QueryParams One False () Nothing Nothing Nothing
            kspace = quoted (LT.fromStrict $ unKeyspace ks)
        send cmp con $ RqQuery (Query (QueryString $ "use " <> kspace) params)
        res <- intReceive cmp con :: IO (Response () () ())
        case res of
            RsResult _ (SetKeyspaceResult _) -> return ()
            RsError  _ e                     -> throw e
            _                                -> throw (UnexpectedResponse' res)

    supportedOptions a = bracket (C.connect (sConnectTimeout s) a) C.close $ \c -> do
        send noCompression c (RqOptions Options :: Request () () ())
        b <- intReceive noCompression c :: IO (Response () () ())
        case b of
            RsSupported _ x -> return x
            RsError     _ e -> throw e
            _               -> throw (UnexpectedResponse' b)

shutdown :: MonadIO m => Pool -> m ()
shutdown p = liftIO $ do
    P.destroyAllResources (connPool p)
    TM.destroy (timeouts p) True

runClient :: MonadIO m => Pool -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    p <- ask
    let c = connPool p
        s = settings p
        t = timeouts p
        g = logger p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> withResource c (transaction g s t)
        Just  q -> tryWithResource c (go g s t p) >>= maybe (retry g q c s t p) return
  where
    go g s t p h = do
        atomicModifyIORef' (failures p) $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction g s t h

    retry g q c s t p = do
        k <- atomicModifyIORef' (failures p) $ \n -> (n + 1, n)
        unless (k < q) $
            throwIO ConnectionsBusy
        withResource c (go g s t p)

    transaction g s t h = do
        x <- TM.add t (Ms $ sSendRecvTimeout s) $ do
            Logger.debug g $ msg (val "Client.request: timeout")
            C.close h
        finally (send (sCompression s) h a >> receive s h)
                (TM.cancel x)

command :: Request k () () -> Client ()
command r = void (request r)

uncompressed :: Client a -> Client a
uncompressed m = do
    s <- asks settings
    local (\e -> e { settings = s { sCompression = noCompression } }) m

compression :: Client Compression
compression = asks (sCompression . settings)

------------------------------------------------------------------------------
-- Internal

send :: Tuple a => Compression -> Connection -> Request k a b -> IO ()
send f h r = do
    let s = StreamId 1
    c <- case getOpCode r of
            OcStartup -> return noCompression
            OcOptions -> return noCompression
            _         -> return f
    a <- either (throw $ InternalError "request creation")
                return
                (pack c False s r)
    C.send a h

receive :: (Tuple a, Tuple b) => Settings -> Connection -> IO (Response k a b)
receive s h = do
    r <- intReceive (sCompression s) h
    case r of
        RsError _ e -> throw e
        RsEvent _ e -> do
            void $ forkIO $ sOnEvent s e
            receive s h
        _           -> return r

intReceive :: (Tuple a, Tuple b) => Compression -> Connection -> IO (Response k a b)
intReceive a c = do
    b <- C.recv 8 c
    h <- case header b of
            Left  e -> throw $ InternalError ("response header reading: " ++ e)
            Right h -> return h
    case headerType h of
        RqHeader -> throw $ InternalError "unexpected request header"
        RsHeader -> do
            let len = lengthRepr (bodyLength h)
            x <- C.recv (fromIntegral len) c
            case unpack a h x of
                Left  e -> throw $ InternalError ("response body reading: " ++ e)
                Right r -> return r

quoted :: LT.Text -> LT.Text
quoted s = "\"" <> LT.replace "\"" "\"\"" s <> "\""

