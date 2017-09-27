{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Data.Text as T

import           Control.Monad (forever)
import           Control.Concurrent (forkIO)
import           System.Exit

import           Snowplow.Tracker
import           Snowplow.Emitter
import           Snowplow.Event

main :: IO ()
main = do 
  emitter <- createEmitter (T.pack "http://localhost:8080") 10 (Post 2) Nothing 
  print $ "emitter created "
  let tracker = createTracker (Just "test-app") False [emitter]
  trackByInput tracker


trackByInput :: Tracker -> IO ()
trackByInput tracker = forever $ pull 
    where pull = do
            url <- getLine
            if url == "exit" then exitSuccess else trackPageView tracker url Nothing Nothing [] Nothing
