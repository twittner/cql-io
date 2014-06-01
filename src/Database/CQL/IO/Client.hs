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
import Database.CQL.IO.Protocol
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager, Milliseconds (..))
import Database.CQL.IO.Types
import System.Logger.Class (MonadLogger (..), Logger, (.=), msg, val)

import qualified Data.Text.Lazy             as LT
import qualified Data.Pool                  as P
import qualified Database.CQL.IO.Connection as C
import qualified Database.CQL.IO.Timeouts   as TM
import qualified System.Logger              as Logger

data Pool = Pool
    { settings :: Settings
    , connPool :: P.Pool Connection
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
        c <- C.connect (sConnectTimeout s) a (sCompression s) (sOnEvent s)
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
        res <- C.request con (serialise cmp (req :: Request () () ()))
        case parse (sCompression s) res :: Response () () () of
            RsEvent _ e -> sOnEvent s e
            _           -> return ()

    useKeyspace con ks = do
        let cmp    = sCompression s
            params = QueryParams One False () Nothing Nothing Nothing
            kspace = quoted (LT.fromStrict $ unKeyspace ks)
            req    = RqQuery (Query (QueryString $ "use " <> kspace) params)
        res <- C.request con (serialise cmp req)
        case parse (sCompression s) res :: Response () () () of
            RsResult _ (SetKeyspaceResult _) -> return ()
            other                            -> throw (UnexpectedResponse' other)

    supportedOptions a = do
        let acquire = C.connect (sConnectTimeout s) a (sCompression s) (sOnEvent s)
            options = RqOptions Options :: Request () () ()
        bracket acquire C.close $ \c -> do
            res <- C.request c (serialise noCompression options)
            case parse (sCompression s) res :: Response () () () of
                RsSupported _ x -> return x
                other           -> throw (UnexpectedResponse' other)

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
        f = failures p
    liftIO $ case sMaxWaitQueue s of
        Nothing -> withResource c (action s)
        Just  q -> tryWithResource c (go s f) >>= maybe (retry q c s f) return
  where
    go s f h = do
        atomicModifyIORef' f $ \n -> (if n > 0 then n - 1 else 0, ())
        action s h

    retry q c s f = do
        k <- atomicModifyIORef' f $ \n -> (n + 1, n)
        unless (k < q) $
            throwIO ConnectionsBusy
        withResource c (go s f)

    action s h = parse (sCompression s) <$> C.request h (serialise (sCompression s) a)

command :: Request k () () -> Client ()
command r = void (request r)

uncompressed :: Client a -> Client a
uncompressed m = do
    s <- asks settings
    local (\e -> e { settings = s { sCompression = noCompression } }) m

compression :: Client Compression
compression = asks (sCompression . settings)

