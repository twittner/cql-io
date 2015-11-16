-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Client
    ( Client
    , ClientState
    ) where

import Control.Monad.Reader (ReaderT (..))
import Control.Applicative
import Prelude

data ClientState

newtype Client a = Client
    { client :: ReaderT ClientState IO a
    }

instance Functor Client
instance Applicative Client
instance Monad Client
