{-# LANGUAGE OverloadedStrings #-}

module Snowplow.Emitter
    ( Q (..)
    , Emitter (..)
    , Buffering (..)
    , createEmitter
    , createQ
    , addQ
    , delay
    ) where

import           Data.Functor
import           Data.Maybe (fromMaybe)
import qualified Data.Text as T
import           Control.Monad (forever)
import           Control.Monad.Trans.Resource
import           Control.Concurrent hiding (readChan, writeChan, writeList2Chan)
import           Control.Concurrent.BoundedChan
import           Network.HTTP.Simple
import           Network.HTTP.Conduit
import           Network.HTTP.Types.Status
import           System.Random

import           Snowplow.Event
import           Snowplow.Internal


-- | Event-buffering preference
data Buffering = Get | Post Int

-- | Batch of events, that can be a single event for GET or multiple for POST
-- Last step before assigning deviceSentTimestamp and sending
data EventBatch = GetR SnowplowEvent | PostR [SnowplowEvent]

-- | Events batch with seconds to wait until retry
data Retry = Retry EventBatch Integer Int       -- batch, originalSendTime, retryNum

-- TODO: Change readyQueue to EventBatch

type ReadyQueue = BoundedChan EventBatch

type RetryQueue = BoundedChan Retry

-- | Events Queue that is either single-events for GET or buffered batches for POST
data Q =
    -- | Single-request queue (for GET-requests)
    SingleEvent (BoundedChan SnowplowEvent)
    -- | Batch-request queue (for POST-requests)
    | BufferedEvents Int (MVar Int) (BoundedChan SnowplowEvent) ReadyQueue RetryQueue -- capacity, cardinality, currentQueue, readyQueue, retryQueue

-- retryQueue :: BoundedChan

data Emitter = Emitter { emitterQueue :: Q, threadId :: ThreadId, backoffTimer :: MVar Int }

-- | Create Emitter thread, pooling new events, converting them to requests and sending
createEmitter :: T.Text                    -- ^ Collector URL 
              -> Int                       -- ^ Queue capacity 
              -> Buffering
              -> Maybe ManagerSettings
              -> IO Emitter
createEmitter collectorUri queueCapacity buffering managerSettings = do
  manager <- newManager $ fromMaybe tlsManagerSettings managerSettings
  emitterQ <- createQ queueCapacity buffering
  timer <- newMVar 0
  threadIdReady <- forkIO $ forever $ pullReady manager emitterQ 
  threadIdRetry <- forkIO $ forever $ pullRetry manager emitterQ
  return $ Emitter emitterQ threadIdReady timer
    where pullReady manager emitterQ = do
            batch <- pullQ emitterQ
            stm <- getTimestamp
            let request = eventToRequest collectorUri batch -- $ event { deviceSentTimestamp = Just stm }
            retry <- case request of 
              Nothing -> return False
              Just r -> fmap shouldRetry $ send manager r
            if retry 
              then eventToRetry (retryQueue emitterQ) batch 
              else pure ()
          pullRetry manager emitterQ = do 
            Retry batch originalSendTime attempt <- readChan (retryQueue emitterQ)
            stm <- getTimestamp
            let request = eventToRequest collectorUri batch
            retry <- case request of 
              Nothing -> return False
              Just r -> fmap shouldRetry $ send manager r
            if retry 
              then retryToRetry (retryQueue emitterQ) (Retry batch stm (attempt + 1))
              else pure ()
          retryQueue emitterQ = case emitterQ of
            SingleEvent _ -> error "TODO"
            BufferedEvents _ _ _ _ queue -> queue
            

eventToRetry :: RetryQueue -> EventBatch -> IO ()
eventToRetry queue batch = do
  timestamp <- getTimestamp
  writeChan queue $ Retry batch timestamp 1

retryToRetry :: RetryQueue -> Retry -> IO ()
retryToRetry queue (Retry batch originalSendTime attempt) = do
  timestamp <- getTimestamp
  if attempt > 10
    then putStrLn "Events dropped"
    else writeChan queue $ Retry batch timestamp (attempt + 1)

-- | Tracker's queue constructor
createQ :: Int -> Buffering -> IO Q
createQ _ buf = case buf of
  Get -> do
    chan <- newBoundedChan 10
    return $ SingleEvent chan
  Post capacity -> do
    cardinality <- newMVar 0
    current <- newBoundedChan 10
    readyQueue <- newBoundedChan 10
    retryQueue <- newBoundedChan 10
    return $ BufferedEvents capacity cardinality current readyQueue retryQueue

-- | Pull queue for data for HTTP requests
pullQ :: Q -> IO EventBatch
pullQ q = case q of
  SingleEvent queue -> do
    event <- readChan queue
    stm   <- getTimestamp
    return $ GetR $ event { deviceSentTimestamp = Just stm }    -- TODO: push back
  BufferedEvents _ _ _ readyQueue _ -> readChan readyQueue

-- | Put event into emitters's queue. 
addQ :: SnowplowEvent -> Q -> IO ()
addQ event queue = put event
  where put e = case queue of
                  SingleEvent chan -> writeChan chan event
                  BufferedEvents capacity cardinality currentQueue readyQueue _ -> checkAndPut capacity cardinality currentQueue readyQueue
        checkAndPut capacity cardinality currentQueue readyQueue = do
          cardinality' <- readMVar cardinality
          if cardinality' <= capacity 
            then putToCurrentQueue cardinality currentQueue event 
            else putToReadyQueue cardinality currentQueue readyQueue event

-- | Return events to queue after unsuccessful send
addToBatchQueue :: EventBatch -> Q -> IO ()
addToBatchQueue eventBatch queue = case queue of
  SingleEvent chan -> error "TODO: add readyQueue for GET"
  BufferedEvents _ _ _ readyQueue _ -> writeChan readyQueue eventBatch

-- | Put element into channel and increment counter
putToCurrentQueue :: MVar Int -> BoundedChan a -> a -> IO ()
putToCurrentQueue cardinality currentQueue a = do
  writeChan currentQueue a
  modifyMVar_ cardinality $ pure . (+1)

-- | Seal a batch, reset counter and put element into freed channel
putToReadyQueue :: MVar Int -> BoundedChan SnowplowEvent -> ReadyQueue -> SnowplowEvent -> IO ()
putToReadyQueue cardinality currentQueue readyQueue a = do
  cardinality' <- readMVar cardinality
  shuffleQueues cardinality'
  modifyMVar_ cardinality $ const (pure 0)
  putToCurrentQueue cardinality currentQueue a
  where shuffleQueues cardinality = do
          queue <- takeFromChan cardinality
          writeChan readyQueue $ PostR queue
        takeFromChan :: Int -> IO [SnowplowEvent]
        takeFromChan cardinality = sequence $ take cardinality $ repeat (readChan currentQueue)

shouldRetry :: Status -> Bool
shouldRetry status = status /= status200

-- | Send HTTP request
send :: Manager -> Request -> IO Status
send manager request = runResourceT $ do
  response <- http request manager
  return $ responseStatus response 

-- | Build a request from URL and event
eventToRequest :: MonadThrow m => T.Text -> EventBatch -> m Request
eventToRequest collectorEndpoint batch = case batch of
  GetR event -> fmap setQuery initialUrl
    where fullUrl    = T.concat [collectorEndpoint, T.pack "/i?"]
          initialUrl = parseRequest $ ("GET " ++ T.unpack fullUrl)
          setQuery   = setQueryString (eventToUrl event)
  PostR events -> fmap setBody initialRequest
    where url            = T.concat [collectorEndpoint, T.pack "/tp2"]
          initialRequest = parseRequest $ ("POST " ++ T.unpack url)
          setBody        = setRequestBodyJSON events

-- | Get delay before resend based on attempt's number
delay :: Int -> IO Int
delay attempt = do
  seed <- randomRIO (exp / 2, exp) :: IO Float
  return $ round $ seed * 1000
  where exp = fromIntegral $ attempt * attempt

-- | Run the callback after the given number of milliseconds.
-- After given amount of time it should appear in ReadyQueue
sendAfterDelay :: Q -> Int -> IO () -> IO ()
sendAfterDelay q t f = void $ forkIO (threadDelay t >> f)
