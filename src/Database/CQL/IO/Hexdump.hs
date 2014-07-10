-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Hexdump (hexdump, hexdumpBuilder) where

import Data.ByteString.Builder
import Data.ByteString.Lazy (ByteString)
import Data.Int
import Data.Monoid
import Data.Word
import System.IO (nativeNewline, Newline (..))

import qualified Data.ByteString.Lazy as L
import qualified Data.List            as List

width, groups :: Int64
width  = 16
groups = 4

hexdump :: ByteString -> ByteString
hexdump = toLazyByteString . hexdumpBuilder

hexdumpBuilder :: ByteString -> Builder
hexdumpBuilder = mconcat
    . List.intersperse newline
    . map toLine
    . zipWith (,) [1 ..]
    . chunks width

chunks :: Int64 -> ByteString -> [ByteString]
chunks n b = List.unfoldr step b
  where
    step "" = Nothing
    step c  = Just $! L.splitAt n c

toLine :: (Word16, ByteString) -> Builder
toLine (n, b) = let k = L.length b in
       word16HexFixed n
    <> ":  "
    <> mconcat (List.intersperse space (map toGroup (chunks groups b)))
    <> spaces ((width - k) * (groups - 1) + pad k + 2)
    <> lazyByteString (toAscii b)

toGroup :: ByteString -> Builder
toGroup = L.foldr (\x y -> word8HexFixed x <> space <> y) mempty

toAscii :: ByteString -> ByteString
toAscii = L.map (\w -> if w > 0x1F && w < 0x7F then w else 0x2E)

space, newline :: Builder
space   = byteString " "
newline = case nativeNewline of
    LF   -> byteString "\n"
    CRLF -> byteString "\r\n"

spaces :: Int64 -> Builder
spaces n = lazyByteString $ L.replicate n 0x20

pad :: Int64 -> Int64
pad n = (width - n) `div` groups
