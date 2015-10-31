-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.Cluster.MurmurHash (hash) where

import Data.ByteString (ByteString)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.Int
import Foreign.C.String
import Foreign.C.Types
import System.IO.Unsafe (unsafePerformIO)

hash :: ByteString -> Int64
hash xs = let h = murmur3 xs in if h == minBound then maxBound else h

{-# NOINLINE murmur3 #-}
murmur3 :: ByteString -> Int64
murmur3 xs = unsafePerformIO $
    unsafeUseAsCStringLen xs $ \(ptr, n) -> c_murmur3 ptr (fromIntegral n) 0

foreign import ccall unsafe "_hs_murmur_hash3_x64_128" c_murmur3
    :: CString -> CInt -> CInt -> IO Int64
