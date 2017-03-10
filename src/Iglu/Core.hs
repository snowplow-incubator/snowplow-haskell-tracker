{-# LANGUAGE OverloadedStrings #-}

module Iglu.Core
  ( SchemaVer (..)
  , SchemaRef (..)
  , SelfDescribingJson (..)
  ) where

import Data.Text
import qualified Data.HashMap.Strict as HS

import Data.Aeson

data SchemaVer = SchemaVer { 
  model :: Int,
  revision :: Int,
  addition :: Int
} deriving (Show, Eq)

instance ToJSON SchemaVer where
  toJSON = String . schemaVerToString

schemaVerToString :: SchemaVer -> Text
schemaVerToString ver = intercalate "-" $ fmap (pack . show) [model ver, revision ver, addition ver]


data SchemaRef = SchemaRef {
  vendor :: String,
  name :: String,
  format :: String,
  version :: SchemaVer
} deriving (Show, Eq)

schemaRefToPath :: SchemaRef -> Text
schemaRefToPath ref = pack (vendor ref) `append` "/" `append` pack (name ref) `append` "/" `append` pack (format ref) `append` "/" `append` schemaVerToString (version ref)

schemaRefToUri :: SchemaRef -> Text
schemaRefToUri ref = "iglu:" `append` schemaRefToPath ref

instance ToJSON SchemaRef where
  toJSON = String . schemaRefToUri


data SelfDescribingJson = SelfDescribingJson {
  schema :: SchemaRef,
  jsonData :: Value
} deriving (Show, Eq)

instance ToJSON SelfDescribingJson where
  toJSON j = Object $ HS.fromList [
    ("schema", toJSON $ schema j),
    ("data",   jsonData j)
    ]

