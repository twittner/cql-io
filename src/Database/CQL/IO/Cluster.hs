-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE OverloadedStrings #-}

module Database.CQL.IO.Cluster where

import Control.Applicative
import Control.Lens ((^.))
import Control.Monad.Catch
import Data.Word
import Database.CQL.IO.Cluster.Discovery
import Database.CQL.IO.Cluster.Event
import Database.CQL.IO.Cluster.Host
import Database.CQL.IO.Cluster.Policies
import Database.CQL.IO.Connection hiding (request)
import Database.CQL.IO.Pool
import Database.CQL.IO.Settings
import Database.CQL.IO.Timeouts (TimeoutManager)
import Database.CQL.Protocol hiding (Map)
import Network.Socket (SockAddr, PortNumber (..))
import System.Logger.Class hiding (create, Settings, settings)

import qualified Database.CQL.IO.Connection as C
import qualified System.Logger              as Logger

eventHandler :: Settings -> Logger -> TimeoutManager -> Policy -> Event -> IO ()
eventHandler s g t p e = print e >> case e of
    StatusEvent   Up   sa        -> handler p (HostUp sa)
    StatusEvent   Down sa        -> handler p (HostDown sa)
    TopologyEvent RemovedNode sa -> handler p (HostRemoved sa)
    TopologyEvent NewNode     sa -> do
        host <- Host sa True Nothing Nothing <$> mkPool s g t sa
        handler p (HostAdded host)
    SchemaEvent   _              -> return ()

mkPool :: Settings -> Logger -> TimeoutManager -> SockAddr -> IO Pool
mkPool s g t a = do
    create (connOpen g t s) (connClose g) g (s^.poolSettings) (s^.connSettings.maxStreams)
  where
    connOpen lgr tmg set = do
        let cst = set^.connSettings
        c <- C.connect cst tmg (set^.protoVersion) lgr a (const $ return ())
        Logger.debug lgr $ "client.connect" .= show c
        C.startup c `onException` connClose lgr c
        return c

    connClose lgr con = do
        Logger.debug lgr $ "client.close" .= show con
        C.close con

mkHost :: Settings -> Logger -> TimeoutManager -> Word16 -> Peer -> IO Host
mkHost s g t p h = do
    let a = inet2SockAddr (PortNum p) (peerRPC h)
    Host a True (Just $ peerDC h) (Just $ peerRack h) <$> mkPool s g t a
