{-# LANGUAGE OverloadedStrings #-}

module Snowplow.Tracker
    ( Tracker
    , trackPageView
    , createTracker
    ) where

import           Data.Foldable
import qualified Data.Text as T
import           Control.Monad.Trans
import           Control.Concurrent (ThreadId)
import           Control.Concurrent.MVar

import           Network.HTTP.Conduit (ManagerSettings)     -- TODO: remove. Tracker should not have any transport logic

import           Iglu.Core
import           Snowplow.Event
import           Snowplow.Emitter
import           Snowplow.Internal

import Data.UUID
import System.Random


type Context = SelfDescribingJson

type SelfDescribingEvent = SelfDescribingJson

-- | Data structure holding tracker preferences and essential functions
data Tracker = Tracker {
  aid            :: Maybe String,      -- ^ app_id 
  encodeContexts :: Bool,              -- ^ Whether contexts should be base64-encoded
  emitters       :: [Emitter]          -- ^ Queue state
}

-- | Create value holding information how tracker wor
createTracker :: Maybe String          -- ^ app_id 
              -> Bool                  -- ^ Whether contexts should be base64-encoded
              -> [Emitter]
              -> Tracker
createTracker appId encodeContexts emitters = Tracker appId encodeContexts emitters

-- | Core track function with all common logic, such as putting to emitter's queue
track :: Tracker -> SnowplowEvent -> IO ()
track (Tracker aid encode emitters) event = do
  dtm <- getTimestamp           -- This may be erased if ttm is set
  eid <- randomIO :: IO UUID
  let timestampedEvent = event { deviceCreatedTimestamp = Just dtm, eventId = eid, appId = aid }
  let eventWithContexts = normalizeEvent encode timestampedEvent
  let queues = fmap emitterQueue emitters 
  traverse_ (addQ timestampedEvent) queues

trackPageView :: Tracker -> String -> Maybe String -> Maybe String -> [Context] -> Maybe Integer -> IO ()
trackPageView    tracker pageUrl pageTitle referrer contexts trueTimestamp = do
    ev <- emptyEvent
    track tracker $ ev { 
      eventType = PageView, 
      url = Just pageUrl, 
      pageTitle = pageTitle, 
      pageReferrer = referrer,
      contexts = contexts,
      trueTimestamp = trueTimestamp }

trackSelfDescribingEvent :: SelfDescribingEvent -> [Context] -> Maybe Integer -> IO ()
trackSelfDescribingEvent event contexts trueTimestamp = undefined
