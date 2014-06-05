-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Timeouts
    ( TimeoutManager
    , create
    , destroy
    , Action
    , Milliseconds (..)
    , add
    , cancel
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import Database.CQL.IO.Types (Milliseconds (..))

data TimeoutManager = TimeoutManager
    { elements  :: Ref [Action]
    , roundtrip :: !Int
    }

data Action = Action
    { action :: IO ()
    , state  :: IORef State
    }

data State = Wait !Int | Canceled

create :: Milliseconds -> IO TimeoutManager
create (Ms n) = do
    e <- spawn [] $ \(Ref r) -> do
        threadDelay (n * 1000)
        prev <- atomicModifyIORef' r $ \x -> ([], x)
        next <- prune prev id
        atomicModifyIORef' r $ \x -> (next x, ())
    return $ TimeoutManager e n
  where
    prune []     front = return front
    prune (a:aa) front = do
        s <- atomicModifyIORef' (state a) $ \x -> (newState x, x)
        case s of
            Wait 0 -> do
                ignore (action a)
                prune aa front
            Canceled -> prune aa front
            _        -> prune aa (front . (a:))

    newState (Wait k) = Wait (k - 1)
    newState s        = s

destroy :: TimeoutManager -> Bool -> IO ()
destroy tm exec = mask_ $ term (elements tm) >>= when exec . mapM_ f
  where
    f e = readIORef (state e) >>= \s -> case s of
        Wait  _ -> ignore (action e)
        _       -> return ()

add :: TimeoutManager -> Milliseconds -> IO () -> IO Action
add tm (Ms n) a = do
    r <- Action a <$> newIORef (Wait $ n `div` roundtrip tm)
    atomicModifyIORef' (unref $ elements tm) $ \rr -> (r:rr, ())
    return r

cancel :: Action -> IO ()
cancel a = atomicWriteIORef (state a) Canceled

-----------------------------------------------------------------------------
-- Internal

newtype Ref a = Ref { unref :: IORef a }

spawn :: a -> (Ref a -> IO ()) -> IO (Ref a)
spawn a f = do
    r <- Ref <$> newIORef a
    _ <- forkIO $ forever (mask_ (f r)) `catch` finish
    return r
  where
    finish :: AsyncException -> IO ()
    finish = const $ return ()

term :: Ref a -> IO a
term (Ref r) = atomicModifyIORef r $ \x -> (throw ThreadKilled, x)

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())

