-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.CQL.IO.Pool
    ( Pool
    , MaxRes (..)
    , MaxRef (..)
    , create
    , destroy
    , purge
    , with
    , tryWith
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad hiding (forM_)
import Data.Foldable (forM_, find)
import Data.Function (on)
import Data.IORef
import Data.Sequence (Seq, ViewL (..), (|>))
import Data.Time.Clock (UTCTime, NominalDiffTime, diffUTCTime)
import Database.CQL.IO.Connection (Connection)
import Database.CQL.IO.Time (TimeCache)
import Database.CQL.IO.Types (Timeout)

import qualified Data.Sequence        as Seq
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
    , idleTime  :: NominalDiffTime
    , conns     :: TVar (Seq Resource)
    , inUse     :: TVar Int
    , tcache    :: TimeCache
    , finaliser :: IORef ()
    }

newtype MaxRes = MaxRes Int
newtype MaxRef = MaxRef Int

create :: IO Connection
       -> (Connection -> IO ())
       -> MaxRes
       -> MaxRef
       -> NominalDiffTime
       -> IO Pool
create mk del (MaxRes n) (MaxRef k) t = do
    p <- Pool mk del n k t
            <$> newTVarIO Seq.empty
            <*> newTVarIO 0
            <*> Time.create
            <*> newIORef ()
    r <- forkIO $ reaper p
    void $ mkWeakIORef (finaliser p) (killThread r >> destroy p)
    return p

destroy :: Pool -> IO ()
destroy p = do
    Time.close (tcache p)
    purge p

with :: Pool -> (Connection -> IO a) -> IO a
with p f = mask $ \restore -> do
    r <- take1 p
    x <- restore (f (value r)) `catches` handlers (put p r) (destroyR p r)
    put p r
    return x

tryWith :: Pool -> (Connection -> IO a) -> IO (Maybe a)
tryWith p f = mask $ \restore -> do
    r <- tryTake1 p
    case r of
        Just  v -> do
            x <- restore (f (value v)) `catches` handlers (put p v) (destroyR p v)
            put p v
            return (Just x)
        Nothing -> return Nothing

purge :: Pool -> IO ()
purge p = do
    rs <- atomically $ swapTVar (conns p) Seq.empty
    forM_ rs $ ignore . destroyFn p . value

-----------------------------------------------------------------------------
-- Internal

handlers :: IO () -> IO () -> [Handler a]
handlers putBack delete =
    [ Handler $ \(x :: Timeout)       -> putBack >> throwIO x
    , Handler $ \(x :: SomeException) -> delete  >> throwIO x
    ]

take1 :: Pool -> IO Resource
take1 p = do
    r <- join . atomically $ do
        c <- readTVar (conns p)
        u <- readTVar (inUse p)
        let n       = Seq.length c
            r :< rr = Seq.viewl $ Seq.unstableSortBy (compare `on` refcnt) c
        if | u < maxconns p                -> mkNew p u
           | n > 0 && refcnt r < maxRefs p -> use p r rr
           | otherwise                     -> retry
    case r of
        Left  x -> do
            atomically (modifyTVar (conns p) (|> x))
            return x
        Right x -> return x

tryTake1 :: Pool -> IO (Maybe Resource)
tryTake1 p = do
    r <- join . atomically $ do
        c <- readTVar (conns p)
        u <- readTVar (inUse p)
        let n       = Seq.length c
            r :< rr = Seq.viewl $ Seq.unstableSortBy (compare `on` refcnt) c
        if | u < maxconns p                -> fmap Just <$> mkNew p u
           | n > 0 && refcnt r < maxRefs p -> fmap Just <$> use p r rr
           | otherwise                     -> return (return Nothing)
    case r of
        (Just (Left  x)) -> do
            atomically (modifyTVar (conns p) (|> x))
            return (Just x)
        (Just (Right x)) -> return (Just x)
        Nothing          -> return Nothing

use :: Pool -> Resource -> Seq Resource -> STM (IO (Either Resource Resource))
use p r rr = do
    writeTVar (conns p) $! rr |> r { refcnt = refcnt r + 1 }
    return (return (Right r))

mkNew :: Pool -> Int -> STM (IO (Either Resource Resource))
mkNew p u = do
    writeTVar (inUse p) $! u + 1
    return $ Left <$> onException
        (Resource <$> Time.currentTime (tcache p) <*> pure 1 <*> createFn p)
        (atomically (modifyTVar (inUse p) (subtract 1)))

put :: Pool -> Resource -> IO ()
put p r = do
    now <- Time.currentTime (tcache p)
    let update x = x { tstamp = now, refcnt = refcnt x - 1 }
    atomically $ do
        rs <- readTVar (conns p)
        case find ((value r ==) . value) rs of
            Nothing -> writeTVar (conns p) $! rs |> update r
            Just r' -> writeTVar (conns p) $! Seq.filter ((value r /=) . value) rs |> update r'

destroyR :: Pool -> Resource -> IO ()
destroyR p r = do
    atomically $ do
        rs <- readTVar (conns p)
        case find ((value r ==) . value) rs of
            Nothing -> return ()
            Just  _ -> do
                modifyTVar (inUse p) (subtract 1)
                writeTVar (conns p) $! Seq.filter ((value r /=) . value) rs
    ignore $ destroyFn p (value r)

reaper :: Pool -> IO ()
reaper p = forever $ do
    threadDelay 1000000
    now <- Time.currentTime (tcache p)
    let isStale r = refcnt r == 0 && now `diffUTCTime` tstamp r > idleTime p
    x <- atomically $ do
            (stale, okay) <- Seq.partition isStale <$> readTVar (conns p)
            unless (Seq.null stale) $
                writeTVar (conns p) okay
            return stale
    forM_ x $ \v -> ignore $ destroyFn p (value v)

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())

