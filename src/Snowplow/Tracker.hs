{-# LANGUAGE OverloadedStrings #-}

module Snowplow.Tracker
    ( Tracker
    , Buffering (..)
    , trackPageView
    , createTracker
    ) where

import           Data.Maybe (fromMaybe)
import           Data.Ratio (numerator)
import           Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Text as T
import           Control.Monad (forever, when)
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource
import           Control.Concurrent hiding (readChan, writeChan)
import           Control.Concurrent.MVar
import           Control.Concurrent.BoundedChan
import           Network.HTTP.Conduit
import           Network.HTTP.Types.Status

import           Iglu.Core
import           Snowplow.Event

import Data.UUID
import System.Random

-- TODO:
-- * True timstamps [X]
-- * POST/buffering
-- * Resend logic
-- * Emitter function
-- * Documentation
-- * Cabal, CI/CD
-- * Other tracking functions
--   - trackSelfDescribingEvent
--   - trackEcommerceTransaction
--   - trackEcommerceTransactionItem
--   - trackStructEvent
-- * Load test
-- * Extract real page URI/title from Spock controller


type Context = SelfDescribingJson

type SelfDescribingEvent = SelfDescribingJson

data Buffering = Get | Post Int

-- |Data structure holding tracker preferences and essential functions
data Tracker = Tracker {
  encodeContexts :: Bool,
  collectorUri   :: String,
  manager        :: Manager,
  queue          :: BoundedChan SnowplowEvent,
  buffering      :: Buffering,
  emitterThread  :: ThreadId
}

data Q = SingleEvent (BoundedChan SnowplowEvent) 
       | BufferedEvents Int (MVar Int) (BoundedChan SnowplowEvent) (BoundedChan SnowplowEvent)

add :: SnowplowEvent -> Q -> IO ()
add event queue = put event
  where put e = case queue of
                  SingleEvent chan -> writeChan chan event
                  BufferedEvents fill capacity buffer chan -> checkAndPut capacity fill
        checkAndPut fill capacity = do
          s <- readMVar fill
          when (s < capacity) writeChan 
          -- if (s < capacity) then (return ()) else (return ())
          

-- |Create Emitter thread, pooling new events, converting them to requests and sending
createEmitter :: Manager                   -- Conduit.HTTP Manager
              -> BoundedChan SnowplowEvent -- Queue with Snowplow Events
              -> T.Text 
              -> Buffering
              -> IO ThreadId
createEmitter manager queue collectorUri buffering = forkIO $ forever pull
  where pull = do
          event <- readChan queue
          stm   <- getTimestamp
          let request = eventToRequest collectorUri $ event { deviceSentTimestamp = Just stm }
          retry <- case request of 
            Nothing -> return False
            Just r  -> fmap shouldRetry $ send manager r
          when retry $ writeChan queue event

-- |Create value holding information how tracker wor
createTracker :: Bool                  -- ^ Whether contexts should be base64-encoded
              -> Maybe ManagerSettings -- ^ Optional HTTP.Conduit Manager settings
              -> Int                   -- ^ Queue capacity
              -> String                -- ^ Collector URI 
              -> Buffering             -- ^ Tracker buffering settings
              -> IO Tracker
createTracker encodeContexts managerSettings queueCapacity collectorUri buffering = do
  trackerChannel <- newBoundedChan queueCapacity
  manager        <- newManager $ fromMaybe tlsManagerSettings managerSettings
  emitterThread  <- createEmitter manager trackerChannel (T.pack collectorUri) buffering
  return $ Tracker encodeContexts collectorUri manager trackerChannel buffering emitterThread 

-- |Send HTTP request
send :: Manager -> Request -> IO Status
send manager request = runResourceT $ do
  response <- http request manager
  return $ responseStatus response 

shouldRetry :: Status -> Bool
shouldRetry status = status /= status200

eventToRequest :: T.Text -> SnowplowEvent -> Maybe Request -- should be MonadThrow
eventToRequest collectorEndpoint event = fmap setQuery initialUrl
  where fullUrl    = T.concat [collectorEndpoint, T.pack "/i?"]
        initialUrl = parseUrl $ T.unpack fullUrl 
        setQuery   = setQueryString (eventToUrl event)

eventsToPostRequest :: T.Text -> [SnowplowEvent] -> Maybe Request 
eventsToPostRequest collectorEndpoint events = undefined

-- Tracking functions

track :: Tracker -> SnowplowEvent -> IO ()
track (Tracker encode curl _ queue _ _) event = do
  dtm <- getTimestamp           -- This may be erased if ttm is set
  eid <- randomIO :: IO UUID
  let timestampedEvent = event { deviceCreatedTimestamp = Just dtm, eventId = eid }
  let eventWithContexts = normalizeEvent encode timestampedEvent
  writeChan queue eventWithContexts

trackPageView :: Tracker -> String -> Maybe String -> Maybe String -> [Context] -> Maybe Integer -> IO ()
trackPageView    tracker pageUrl pageTitle referrer contexts trueTimestamp = track tracker $ emptyEvent { 
  eventType = PageView, 
  url = Just pageUrl, 
  pageTitle = pageTitle, 
  pageReferrer = referrer,
  contexts = contexts,
  trueTimestamp = trueTimestamp }

trackSelfDescribingEvent :: SelfDescribingEvent -> [Context] -> Maybe Integer -> IO ()
trackSelfDescribingEvent event contexts trueTimestamp = undefined

-- Aux functions

-- Get timestamp in milliseconds
getTimestamp :: IO Integer
getTimestamp = (`div` 1000) . numerator .  toRational . (* 1000000) <$> getPOSIXTime
