module Snowplow.Internal 
     ( getTimestamp 
     ) where

import Data.Ratio (numerator)
import Data.Time.Clock.POSIX (getPOSIXTime)

-- Get timestamp in milliseconds
getTimestamp :: IO Integer
getTimestamp = (`div` 1000) . numerator .  toRational . (* 1000000) <$> getPOSIXTime
