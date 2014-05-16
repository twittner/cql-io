-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- | A 'TimeoutManager' keeps track of a set of actions which are executed
-- after their individual timeout occured.
--
-- This module is heavily inspired by Warp's
-- <http://hackage.haskell.org/package/warp-2.1.5.1 internal timeout manager>.
module Database.CQL.IO.Timeouts
    ( -- * Timeout Manager
      TimeoutManager
    , create
    , destroy

      -- * Action
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

newtype Milliseconds = Ms Int deriving (Eq, Show)

data TimeoutManager = TimeoutManager
    { elements  :: Ref [Action]
    , roundtrip :: !Int
    }

data Action = Action
    { action :: IO ()
    , state  :: IORef State
    }

data State = Wait !Int | Canceled

-- | Create a timeout manager which checks every given number of
-- milliseconds if an action should be executed.
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

-- | Stops a 'TimeoutManager' and optionally executes all remaining
-- actions which haven't been canceled if the boolean parameter is @True@.
destroy :: TimeoutManager -> Bool -> IO ()
destroy tm exec = mask_ $ term (elements tm) >>= when exec . mapM_ f
  where
    f e = readIORef (state e) >>= \s -> case s of
        Wait  _ -> ignore (action e)
        _       -> return ()

-- | Add some action to be executed after the given number of milliseconds.
--
-- Note that the accuracy is limited by the manager's \"resolution\", i.e.
-- the number of milliseconds given in 'create'.
add :: TimeoutManager -> Milliseconds -> IO () -> IO Action
add tm (Ms n) a = do
    r <- Action a <$> newIORef (Wait $ n `div` roundtrip tm)
    atomicModifyIORef' (unref $ elements tm) $ \rr -> (r:rr, ())
    return r

-- | Cancels this action, i.e. it will not be executed by this manager.
cancel :: Action -> IO ()
cancel a = atomicWriteIORef (state a) Canceled

-----------------------------------------------------------------------------
-- Internal

newtype Ref a = Ref { unref :: IORef a }

-- Create a 'Ref' with the given initial state and a function which is
-- applied to this 'Ref' in a 'forever' loop with asynchronous exceptions
-- masked. The only way to end this loop is to apply 'term' to this 'Ref'.
spawn :: a -> (Ref a -> IO ()) -> IO (Ref a)
spawn a f = do
    r <- Ref <$> newIORef a
    _ <- forkIO $ forever (mask_ (f r)) `catch` finish
    return r
  where
    finish :: AsyncException -> IO ()
    finish = const $ return ()

-- 'term' replaces the contents of the given 'Ref' with exception throwing
-- code. This will end the asynchronous loop created in 'spawn'.
term :: Ref a -> IO a
term (Ref r) = atomicModifyIORef r $ \x -> (throw ThreadKilled, x)

ignore :: IO () -> IO ()
ignore a = catch a (const $ return () :: SomeException -> IO ())

