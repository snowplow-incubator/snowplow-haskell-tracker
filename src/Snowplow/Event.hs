{-# LANGUAGE OverloadedStrings #-}

module Snowplow.Event 
    ( SnowplowEvent (..)
    , EventType (..)
    , TrackerEvent (..)
    , emptyEvent
    , eventToUrl
    , normalizeEvent
    ) where

import Data.ByteString
import qualified Data.ByteString.Base64.Lazy as BL
import qualified Data.ByteString.Lazy as L
import qualified Data.Text as T
import qualified Data.Text.Lazy.Encoding as LE
import qualified Data.HashMap.Strict as HS

import System.Random
import Data.UUID
import Data.Text.Encoding
import Data.Vector hiding ((++))
import Data.Scientific (coefficient)

import Data.Aeson

import Iglu.Core

-- import Paths_snowplow_haskell_tracker (version)
-- import Data.Version (showVersion)


type Context = SelfDescribingJson

type SelfDescribingEvent = SelfDescribingJson

data EventType = PageView | Unstructured deriving (Show, Eq)

instance ToJSON EventType where
  toJSON e = String $ case e of
    PageView -> "pv"
    Unstructured -> "ue"

-- Data structure representing all available fields accroding to Snowplow Tracker Protocol
data SnowplowEvent = SnowplowEvent {
  appId                  :: Maybe String,
  eventType              :: EventType,
  eventId                :: UUID,
  trackerVersion         :: Maybe String,
  contexts               :: [Context],
  encodedContexts        :: [Context],

  url                    :: Maybe String,
  pageTitle              :: Maybe String,
  pageReferrer           :: Maybe String,

  deviceCreatedTimestamp :: Maybe Integer,
  deviceSentTimestamp    :: Maybe Integer,
  trueTimestamp          :: Maybe Integer
} deriving (Show, Eq)

data TrackerEvent = Single SnowplowEvent | Buffered [SnowplowEvent]

-- TODO: this should be refined type
instance ToJSON SnowplowEvent where
  toJSON e = object [
    "aid"  .= appId e,
    "e"    .= eventType e,
    "eid"  .= toString (eventId e),
    "tv"   .= trackerVersion e,
    "co"   .= nonEmpty (contexts e),
    "cx"   .= (fmap LE.decodeUtf8 (base64encode (encodedContexts e))),    -- I want to make it list

    "url"  .= url e,
    "page" .= pageTitle e,
    "refr" .= pageReferrer e,

    "dtm"  .= deviceCreatedTimestamp e,
    "stm"  .= deviceSentTimestamp e,
    "ttm"  .= trueTimestamp e
    ]

base64encode :: [Context] -> Maybe L.ByteString -- TODO: this should transform into base64
base64encode [] = Nothing
base64encode contexts = Just $ BL.encode $ encode $ wrapContexts contexts

nonEmpty :: [a] -> Maybe [a]
nonEmpty [] = Nothing
nonEmpty as = Just as

emptyEvent :: IO SnowplowEvent
emptyEvent = do
  eid <- randomIO :: IO UUID
  return $ SnowplowEvent { 
  appId                  = Nothing,
  eventId                = eid,
  eventType              = Unstructured,
  trackerVersion         = Just $ "haskell-0.1.0.0",  -- Paths_snowplow_haskell_tracker crash linker; TODO: fix with docker
  contexts               = [],
  encodedContexts        = [],

  url                    = Nothing,
  pageTitle              = Nothing,
  pageReferrer           = Nothing,

  deviceCreatedTimestamp = Nothing,
  deviceSentTimestamp    = Nothing,
  trueTimestamp          = Nothing
}

eventToUrl :: SnowplowEvent -> [(ByteString, Maybe ByteString)]
eventToUrl = urlizePayload . normalizeGetPayload . cleanup . toJSON

contextSchema :: SchemaRef
contextSchema = SchemaRef {
  vendor = "com.snowplowanalytics.snowplow",
  name = "contexts",
  format = "jsonschema",
  schemaVer = SchemaVer { model = 1, revision = 0, addition = 1 }
}

postSchema :: SchemaRef
postSchema = SchemaRef {
  vendor = "com.snowplowanalytics.snowplow",
  name = "payload_data",
  format = "jsonschema",
  schemaVer = SchemaVer { model = 1, revision = 0, addition = 4 }
}

wrapContexts :: [Context] -> Context
wrapContexts contexts = SelfDescribingJson {
  schema = contextSchema,
  jsonData = Array $ fmap toJSON $ fromList contexts
}

-- |Make all contexts plain or base64-encoded, depending on `encode` argument
-- TODO: this should have refined type
normalizeEvent :: Bool -> SnowplowEvent -> SnowplowEvent
normalizeEvent encode event = 
  if encode then timestampedEvent { encodedContexts = newContexts, contexts = [] }
  else timestampedEvent { contexts = newContexts, encodedContexts = [] }
  where
    newContexts = encodedContexts event ++ contexts event
    timestampedEvent = case trueTimestamp event of 
      Just t -> event { deviceCreatedTimestamp = Nothing }
      Nothing -> event

-- |Remove all unpopulated (`null`) fields and transform into `HashMap`
cleanup :: Value -> HS.HashMap T.Text Value
cleanup json = case json of
  Object hm -> filterNulls hm
  _ -> error "cleanup function accepts only JSON objects"
  where filterNulls = HS.filterWithKey predicate
        predicate _ v = case v of
            Null -> False
            _ -> True

normalizeGetPayload :: HS.HashMap T.Text Value -> HS.HashMap T.Text T.Text
normalizeGetPayload = HS.map encode 
  where encode v = case v of
            String s -> s
            Number n -> T.pack $ show $ coefficient n
            s -> error $ T.unpack $ "Cannot stringify " `T.append` T.pack (show s)
  
urlizePayload :: HS.HashMap T.Text T.Text -> [(ByteString, Maybe ByteString)]
urlizePayload hm = fmap pack $ HS.toList hm
  where pack pair = (encodeUtf8 $ fst pair, Just $ encodeUtf8 $ snd pair)

