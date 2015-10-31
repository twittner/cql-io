-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Cluster.Token where

import Control.Lens (Lens')
import Data.Int (Int64)
import Database.CQL.Protocol

import qualified Data.Text.Lazy             as Lazy
import qualified Data.Text.Lazy.Builder     as Builder
import qualified Data.Text.Lazy.Builder.Int as Builder
import qualified Data.Text.Read             as Reader

-- Token --------------------------------------------------------------------

newtype Token = Token Int64 deriving (Eq, Ord, Show)

instance Cql Token where
    ctype = Tagged TextColumn

    toCql (Token t) =
        CqlText (Lazy.toStrict . Builder.toLazyText $ Builder.decimal t)

    fromCql (CqlText t) =
        either (const $ fail "fromCql: Token: read error")
               (return . Token . fst)
               (Reader.signed Reader.decimal t)
    fromCql _ = fail "fromCql: Token: expected 'text'"

-- Token Range --------------------------------------------------------------

data TokenRange = TokenRange
    { _rangeBegin :: !Token
    , _rangeEnd   :: !Token
    } deriving (Eq, Ord, Show)

firstToken :: Lens' TokenRange Token
firstToken f ~(TokenRange a b) = fmap (\x -> TokenRange x b) (f a)
{-# INLINE firstToken #-}

lastToken :: Lens' TokenRange Token
lastToken f ~(TokenRange a b) = fmap (\x -> TokenRange a x) (f b)
{-# INLINE lastToken #-}
