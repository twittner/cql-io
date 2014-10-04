-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Signal
    ( Signal
    , signal
    , connect
    , emit
    , (|->)
    ) where

import Control.Applicative
import Data.IORef

newtype Signal a = Sig (IORef [a -> IO ()])

signal :: IO (Signal a)
signal = Sig <$> newIORef []

connect :: Signal a -> (a -> IO ()) -> IO ()
connect (Sig s) f = atomicModifyIORef' s $ \x -> (f:x, ())

infixl 2 |->
(|->) :: Signal a -> (a -> IO ()) -> IO ()
(|->) = connect

emit :: Signal a -> a -> IO ()
emit (Sig s) a = readIORef s >>= mapM_ ($ a)
