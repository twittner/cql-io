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

import Control.Concurrent.STM
import Control.Applicative

newtype Signal a = Sig (TVar [a -> IO ()])

signal :: IO (Signal a)
signal = Sig <$> newTVarIO []

connect :: Signal a -> (a -> IO ()) -> IO ()
connect (Sig s) f = atomically $ modifyTVar s (f:)

infixl 2 |->
(|->) :: Signal a -> (a -> IO ()) -> IO ()
(|->) = connect

emit :: Signal a -> a -> IO ()
emit (Sig s) a = readTVarIO s >>= mapM_ ($ a)
