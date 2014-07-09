-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.IO.Pool
    ( Pool
    , MaxRes  (..)
    , MaxRef  (..)
    , Stripes (..)
    , create
    , destroy
    , purge
    , with
    , tryWith
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad hiding (forM_, mapM_)
import Data.Foldable (forM_, mapM_, find)
import Data.Function (on)
import Data.Hashable
import Data.IORef
import Prelude hiding (mapM_)
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Time.Clock (UTCTime, NominalDiffTime, diffUTCTime)
import Data.Vector (Vector, (!))
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Time (TimeCache)
import Database.CQL.IO.Types (Timeout, ignore)

import qualified Data.Sequence        as Seq
import qualified Data.Vector          as Vec
import qualified Database.CQL.IO.Time as Time

-----------------------------------------------------------------------------
-- API

data Resource = Resource
    { tstamp :: !UTCTime
    , refcnt :: !Int
    , value  :: !Connection
    } deriving Show

data Pool = Pool
    { createFn  :: IO Connection
    , destroyFn :: Connection -> IO ()
    , maxconns  :: Int
    , maxRefs   :: Int
    , nStripes  :: Int
    , idleTime  :: NominalDiffTime
    , tcache    :: TimeCache
    , stripes   :: Vector Stripe
    , finaliser :: IORef ()
    }

data Stripe = Stripe
    { conns :: TVar (Seq Resource)
    , inUse :: TVar Int
    }

newtype MaxRes  = MaxRes Int
newtype MaxRef  = MaxRef Int
newtype Stripes = Stripes Int

create :: IO Connection
       -> (Connection -> IO ())
       -> MaxRes
       -> MaxRef
       -> Stripes
       -> NominalDiffTime
       -> IO Pool
create mk del (MaxRes n) (MaxRef k) (Stripes s) t = do
    p <- Pool mk del n k s t
            <$> Time.create
            <*> Vec.replicateM s (Stripe <$> newTVarIO Seq.empty <*> newTVarIO 0)
            <*> newIORef ()
    r <- async $ reaper p
    void $ mkWeakIORef (finaliser p) (cancel r >> destroy p)
    return p

destroy :: Pool -> IO ()
destroy p = do
    Time.close (tcache p)
    purge p

with :: Pool -> (Connection -> IO a) -> IO a
with p f = do
    s <- stripe p
    mask $ \restore -> do
        r <- take1 p s
        x <- restore (f (value r)) `catches` handlers (put p s r) (destroyR p s r)
        put p s r
        return x

tryWith :: Pool -> (Connection -> IO a) -> IO (Maybe a)
tryWith p f = do
    s <- stripe p
    mask $ \restore -> do
        r <- tryTake1 p s
        case r of
            Just  v -> do
                x <- restore (f (value v)) `catches` handlers (put p s v) (destroyR p s v)
                put p s v
                return (Just x)
            Nothing -> return Nothing

purge :: Pool -> IO ()
purge p = Vec.forM_ (stripes p) $ \s ->
    atomically (swapTVar (conns s) Seq.empty) >>= mapM_ (ignore . destroyFn p . value)

-----------------------------------------------------------------------------
-- Internal

handlers :: IO () -> IO () -> [Handler a]
handlers putBack delete =
    [ Handler $ \(x :: Timeout)       -> putBack >> throwIO x
    , Handler $ \(x :: SomeException) -> delete  >> throwIO x
    ]

take1 :: Pool -> Stripe -> IO Resource
take1 p s = do
    r <- join . atomically $ do
        c <- readTVar (conns s)
        u <- readTVar (inUse s)
        let n       = Seq.length c
            r :< rr = Seq.viewl $ Seq.unstableSortBy (compare `on` refcnt) c
        if | u < maxconns p                -> mkNew p s u
           | n > 0 && refcnt r < maxRefs p -> use s r rr
           | otherwise                     -> retry
    case r of
        Left  x -> do
            atomically (modifyTVar (conns s) (|> x))
            return x
        Right x -> return x

tryTake1 :: Pool -> Stripe -> IO (Maybe Resource)
tryTake1 p s = do
    r <- join . atomically $ do
        c <- readTVar (conns s)
        u <- readTVar (inUse s)
        let n       = Seq.length c
            r :< rr = Seq.viewl $ Seq.unstableSortBy (compare `on` refcnt) c
        if | u < maxconns p                -> fmap Just <$> mkNew p s u
           | n > 0 && refcnt r < maxRefs p -> fmap Just <$> use s r rr
           | otherwise                     -> return (return Nothing)
    case r of
        (Just (Left  x)) -> do
            atomically (modifyTVar (conns s) (|> x))
            return (Just x)
        (Just (Right x)) -> return (Just x)
        Nothing          -> return Nothing

use :: Stripe -> Resource -> Seq Resource -> STM (IO (Either Resource Resource))
use s r rr = do
    writeTVar (conns s) $! rr |> r { refcnt = refcnt r + 1 }
    return (return (Right r))
{-# INLINE use #-}

mkNew :: Pool -> Stripe -> Int -> STM (IO (Either Resource Resource))
mkNew p s u = do
    writeTVar (inUse s) $! u + 1
    return $ Left <$> onException
        (Resource <$> Time.currentTime (tcache p) <*> pure 1 <*> createFn p)
        (atomically (modifyTVar (inUse s) (subtract 1)))
{-# INLINE mkNew #-}

put :: Pool -> Stripe -> Resource -> IO ()
put p s r = do
    now <- Time.currentTime (tcache p)
    let update x = x { tstamp = now, refcnt = refcnt x - 1 }
    atomically $ do
        rs <- readTVar (conns s)
        case find ((value r ==) . value) rs of
            Nothing -> writeTVar (conns s) $! rs |> update r
            Just r' -> writeTVar (conns s) $! Seq.filter ((value r /=) . value) rs |> update r'

destroyR :: Pool -> Stripe -> Resource -> IO ()
destroyR p s r = do
    atomically $ do
        rs <- readTVar (conns s)
        case find ((value r ==) . value) rs of
            Nothing -> return ()
            Just  _ -> do
                modifyTVar (inUse s) (subtract 1)
                writeTVar (conns s) $! Seq.filter ((value r /=) . value) rs
    ignore $ destroyFn p (value r)

reaper :: Pool -> IO ()
reaper p = forever $ do
    threadDelay 1000000
    now <- Time.currentTime (tcache p)
    let isStale r = refcnt r == 0 && now `diffUTCTime` tstamp r > idleTime p
    Vec.forM_ (stripes p) $ \s -> do
        x <- atomically $ do
                (stale, okay) <- Seq.partition isStale <$> readTVar (conns s)
                unless (Seq.null stale) $
                    writeTVar (conns s) okay
                return stale
        forM_ x $ \v -> ignore $ destroyFn p (value v)

stripe :: Pool -> IO Stripe
stripe p = (stripes p !) <$> ((`mod` hash (nStripes p)) . hash) <$> myThreadId
{-# INLINE stripe #-}

