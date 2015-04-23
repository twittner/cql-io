-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Database.CQL.IO.PrepQuery
    ( PrepQuery
    , prepared
    , queryString

    , PreparedQueries
    , new
    , lookupQueryId
    , lookupQueryString
    , insert
    , delete
    , queryStrings
    ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Monad
import Crypto.Hash.SHA1
import Data.ByteString (ByteString)
import Data.Text.Lazy (Text)
import Data.Text.Lazy.Encoding (encodeUtf8)
import Data.Foldable (for_)
import Data.Map.Strict (Map)
import Data.String
import Database.CQL.Protocol hiding (Map)
import Database.CQL.IO.Types (HashCollision (..))
import Prelude

import qualified Data.Map.Strict as M

-----------------------------------------------------------------------------
-- Prepared Query

-- | Representation of a prepared query.
-- Actual preparation is handled transparently by the driver.
data PrepQuery k a b = PrepQuery
    { pqStr :: !(QueryString k a b)
    , pqId  :: !PrepQueryId
    }

instance IsString (PrepQuery k a b) where
    fromString = prepared . fromString

newtype PrepQueryId = PrepQueryId ByteString deriving (Eq, Ord)

prepared :: QueryString k a b -> PrepQuery k a b
prepared q = PrepQuery q $ PrepQueryId (hashlazy . encodeUtf8 . unQueryString $ q)

queryString :: PrepQuery k a b -> QueryString k a b
queryString = pqStr

-----------------------------------------------------------------------------
-- Map of prepared queries to their query ID and query string

newtype QST = QST { unQST :: Text }
newtype QID = QID { unQID :: ByteString } deriving (Eq, Ord)

data PreparedQueries = PreparedQueries
    { queryMap :: !(TVar (Map PrepQueryId (QID, QST)))
    , qid2Str  :: !(TVar (Map QID QST))
    }

new :: IO PreparedQueries
new = PreparedQueries <$> newTVarIO M.empty <*> newTVarIO M.empty

lookupQueryId :: PrepQuery k a b -> PreparedQueries -> STM (Maybe (QueryId k a b))
lookupQueryId q m = do
    qm <- readTVar (queryMap m)
    return $ QueryId . unQID . fst <$> M.lookup (pqId q) qm

lookupQueryString :: QueryId k a b -> PreparedQueries -> STM (Maybe (QueryString k a b))
lookupQueryString q m = do
    qm <- readTVar (qid2Str m)
    return $ QueryString . unQST <$> M.lookup (QID $ unQueryId q) qm

insert :: PrepQuery k a b -> QueryId k a b -> PreparedQueries -> STM ()
insert q i m = do
    qq <- M.lookup (pqId q) <$> readTVar (queryMap m)
    for_ qq (verify . snd)
    modifyTVar' (queryMap m) $
        M.insert (pqId q) (QID $ unQueryId i, QST $ unQueryString (pqStr q))
    modifyTVar' (qid2Str  m) $
        M.insert (QID $ unQueryId i) (QST $ unQueryString (pqStr q))
  where
    verify qs =
        unless (unQST qs == unQueryString (pqStr q)) $ do
            let a = unQST qs
            let b = unQueryString (pqStr q)
            throwSTM (HashCollision a b)

delete :: PrepQuery k a b -> PreparedQueries -> STM ()
delete q m = do
    qid <- M.lookup (pqId q) <$> readTVar (queryMap m)
    modifyTVar' (queryMap m) $ M.delete (pqId q)
    case qid of
        Nothing -> return ()
        Just  i -> modifyTVar' (qid2Str m) $ M.delete (fst i)

queryStrings :: PreparedQueries -> STM [Text]
queryStrings m = map (unQST . snd) . M.elems <$> readTVar (queryMap m)
