-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Cluster.Token
    ( -- * Token
      Token
    , minToken
    , maxToken
    , hash
    , fromText

      -- * Token Range
    , TokenRange
    , new
    , fromSortedList
    , firstToken
    , lastToken
    , isEmpty
    , isWrapped
    , contains
    ) where

import Control.Lens (Lens')
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text (Text)
import Database.CQL.Protocol

import qualified Data.Set                           as Set
import qualified Data.Text.Lazy                     as Lazy
import qualified Data.Text.Lazy.Builder             as Builder
import qualified Data.Text.Lazy.Builder.Int         as Builder
import qualified Data.Text.Read                     as Reader
import qualified Database.CQL.IO.Cluster.MurmurHash as Murmur

-- Token --------------------------------------------------------------------

newtype Token = Token Int64 deriving (Eq, Ord, Show)

minToken, maxToken :: Token
minToken = Token minBound
maxToken = Token maxBound

hash :: ByteString -> Token
hash = Token . Murmur.hash

fromText :: Text -> Maybe Token
fromText = either (const Nothing) (Just . Token . fst) . Reader.signed Reader.decimal

instance Cql Token where
    ctype = Tagged TextColumn

    toCql (Token t) = CqlText (Lazy.toStrict . Builder.toLazyText $ Builder.decimal t)

    fromCql (CqlText t) = maybe (fail "fromCql: Token: read error") return (fromText t)
    fromCql _           = fail "fromCql: Token: expected 'text'"

-- Token Range --------------------------------------------------------------

-- | Token range is @]firstToken, lastToken]@ and empty if @firstToken ==
-- lastToken@ unless @firstToken == minToken@ which means the whole token
-- ring is covered.
data TokenRange = TokenRange
    { _rangeBegin :: !Token
    , _rangeEnd   :: !Token
    } deriving (Eq, Ord, Show)

new :: Token -> Token -> TokenRange
new = TokenRange

fromSortedList :: [Token] -> Set.Set TokenRange
fromSortedList []     = mempty
fromSortedList (x:xs) = Set.fromList $ go x (x:xs) id
  where
    go a (b:c:rest) !acc = go a rest (acc . (new b c :))
    go a (b:[])     !acc = acc . (new b a :) $ []
    go _ []         !acc = acc []

isEmpty :: TokenRange -> Bool
isEmpty r = _rangeBegin r == _rangeEnd r && _rangeBegin r /= minToken

isWrapped :: TokenRange -> Bool
isWrapped r = _rangeBegin r > _rangeEnd r && _rangeEnd r /= minToken

contains :: Token -> TokenRange -> Bool
contains t r =
    let isAfterBegin = t > _rangeBegin r
        isBeforeEnd  = _rangeBegin r == minToken || t <= _rangeEnd r
    in
        if isWrapped r then
            isAfterBegin || isBeforeEnd
        else
            isAfterBegin && isBeforeEnd

firstToken :: Lens' TokenRange Token
firstToken f ~(TokenRange a b) = fmap (\x -> TokenRange x b) (f a)
{-# INLINE firstToken #-}

lastToken :: Lens' TokenRange Token
lastToken f ~(TokenRange a b) = fmap (\x -> TokenRange a x) (f b)
{-# INLINE lastToken #-}
