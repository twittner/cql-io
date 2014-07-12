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
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Monad.Reader
import Data.Foldable (for_)
import Data.IORef
import Data.Monoid ((<>))
import Data.Word
import Database.CQL.Protocol
import Database.CQL.IO.Protocol
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.IO.Types
import System.Logger.Class (MonadLogger (..), Logger, (.=))

import qualified Data.Text.Lazy             as LT
import qualified Database.CQL.IO.Pool       as P
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM
import qualified System.Logger              as Logger

data Pool = Pool
    { settings :: Settings
    , connPool :: P.Pool
    , logger   :: Logger.Logger
    , failures :: IORef Word64
    , timeouts :: TimeoutManager
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
    t <- TM.create 250
    a <- validateSettings t
    Pool s <$> P.create (connOpen t a)
                        connClose
                        g
                        (P.MaxRes  $ sMaxConnections s)
                        (P.MaxRef  $ sMaxStreams s)
                        (P.MaxTout $ sMaxTimeouts s)
                        (P.Stripes $ sPoolStripes s)
                        (sIdleTimeout s)
           <*> pure g
           <*> newIORef 0
           <*> pure t
  where
    validateSettings t = do
        addr <- C.resolve (sHost s) (sPort s)
        Supported ca _ <- supportedOptions t addr
        let c = algorithm (sCompression s)
        unless (c == None || c `elem` ca) $
            throwM $ UnsupportedCompression ca
        return addr

    connOpen t a = do
        Logger.debug g $ "client.connect" .= sHost s
        c <- C.connect s g a (sCompression s) (sOnEvent s)
        connInit t c

    connInit t c = flip onException (C.close c) $ do
        startup t c
        for_ (sKeyspace s) $
            useKeyspace t c
        return c

    connClose c = do
        Logger.debug g $ "client.close" .= sHost s
        C.close c

    startup t con = do
        let cmp = sCompression s
            req = RqStartup (Startup (sVersion s) (algorithm cmp))
        res <- C.request s t con (serialise cmp (req :: Request () () ()))
        case parse (sCompression s) res :: Response () () () of
            RsEvent _ e -> sOnEvent s e
            _           -> return ()

    useKeyspace t con ks = do
        let cmp    = sCompression s
            params = QueryParams One False () Nothing Nothing Nothing
            kspace = quoted (LT.fromStrict $ unKeyspace ks)
            req    = RqQuery (Query (QueryString $ "use " <> kspace) params)
        res <- C.request s t con (serialise cmp req)
        case parse (sCompression s) res :: Response () () () of
            RsResult _ (SetKeyspaceResult _) -> return ()
            other                            -> throwM (UnexpectedResponse' other)

    supportedOptions t a = do
        let acquire = C.connect s g a (sCompression s) (sOnEvent s)
            options = RqOptions Options :: Request () () ()
        bracket acquire C.close $ \c -> do
            res <- C.request s t c (serialise noCompression options)
            case parse (sCompression s) res :: Response () () () of
                RsSupported _ x -> return x
                other           -> throwM (UnexpectedResponse' other)

shutdown :: MonadIO m => Pool -> m ()
shutdown p = liftIO $ do
    P.destroy (connPool p)
    TM.destroy (timeouts p) True

runClient :: MonadIO m => Pool -> Client a -> m a
runClient p a = liftIO $ runReaderT (client a) p

request :: (Tuple a, Tuple b) => Request k a b -> Client (Response k a b)
request a = do
    p <- ask
    let c = connPool p
        s = settings p
        t = timeouts p
        f = failures p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> P.with c (transaction s t)
        Just  q -> P.tryWith c (go s t f) >>= maybe (retry q c s t f) return
  where
    go s t f h = do
        atomicModifyIORef' f $ \n -> (if n > 0 then n - 1 else 0, ())
        transaction s t h

    retry q c s t f = do
        k <- atomicModifyIORef' f $ \n -> (n + 1, n)
        unless (k < q) $
            throwM ConnectionsBusy
        P.with c (go s t f)

    transaction s t h =
        parse (sCompression s) <$> C.request s t h (serialise (sCompression s) a)

command :: Request k () () -> Client ()
command r = void (request r)

uncompressed :: Client a -> Client a
uncompressed m = do
    s <- asks settings
    local (\e -> e { settings = s { sCompression = noCompression } }) m

compression :: Client Compression
compression = asks (sCompression . settings)

