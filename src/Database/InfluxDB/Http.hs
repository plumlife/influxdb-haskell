{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Database.InfluxDB.Http
  ( Config(..)
  , Credentials(..), rootCreds
  , Server(..), localServer
  , TimePrecision(..)

  -- * Writing Data
  , formatLine
  , formatLines
  -- ** Updating Points
  , post, postWithPrecision
  , SeriesT, PointT, Line(..)
  , writeSeries
  , writeSeriesData
  , withSeries
  , writePoints

  -- ** Deleting Points
  , deleteSeries

  -- * Querying Data
  , query
  , Stream(..)
  , queryChunked

  -- * Administration & Security
  -- ** Creating and Dropping Databases
  , listDatabases
  , createDatabase
  , dropDatabase


  -- ** Security
  -- *** Database user
  , listUsers
  , addUser
  , updateUserPassword
  , deleteUser
  , grantAdminPrivilegeTo
  , revokeAdminPrivilegeFrom

  -- ** Other API
  , ping
  ) where

import Control.Applicative
import Control.Monad.Identity
import Control.Monad.Writer
import Data.DList (DList)
import Data.IORef
import Data.Maybe (fromJust)
import Data.Proxy
import Data.Text (Text)
import Data.Vector (Vector)
import Data.Word (Word32)
import Network.URI (escapeURIString, isAllowedInURI)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BL
import qualified Data.DList as DL
import Data.Int
import qualified Data.List as L
import Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Text.Printf (printf)
import Prelude

import Control.Monad.Catch (Handler(..))
import Control.Retry
import Data.Aeson ((.=))
import Data.Aeson.TH (deriveToJSON)
import Data.Default.Class (Default(def))
import qualified Data.Aeson as A
import qualified Data.Aeson.Encode as AE
import qualified Data.Aeson.Parser as AP
import qualified Data.Aeson.Types as AT
import qualified Data.Attoparsec.ByteString as P
import qualified Data.Attoparsec.ByteString.Lazy as PL
import qualified Network.HTTP.Client as HC

import Database.InfluxDB.Decode
import Database.InfluxDB.Encode
import Database.InfluxDB.Types
import Database.InfluxDB.Types.Internal (stripPrefixOptions)
import Database.InfluxDB.Stream (Stream(..))
import qualified Database.InfluxDB.Stream as S

-- | Configurations for HTTP API client.
data Config = Config
  { configCreds :: !Credentials
  , configServerPool :: !(IORef ServerPool)
  , configHttpManager :: !HC.Manager
  }

-- | Default credentials.
rootCreds :: Credentials
rootCreds = Credentials
  { credsUser = "root"
  , credsPassword = "root"
  }

-- | Default server location.
localServer :: Server
localServer = Server
  { serverHost = "localhost"
  , serverPort = 8086
  , serverSsl = False
  }

data TimePrecision
  = SecondsPrecision
  | MillisecondsPrecision
  | MicrosecondsPrecision

timePrecString :: TimePrecision -> String
timePrecString SecondsPrecision = "s"
timePrecString MillisecondsPrecision = "ms"
timePrecString MicrosecondsPrecision = "u"

-----------------------------------------------------------
-- Writing Data

data Line = Line {
  lineMeasurement :: Text,
  lineTags :: Map Text Text,
  lineFields :: Map Text Value,
  linePrecision :: Maybe Int64
}

formatLine :: Line -> BL.ByteString
formatLine line = BL.concat[BL.fromStrict $ TE.encodeUtf8 $ lineMeasurement line,formatedTags, " ", formatedValues, maybe "" (\ x -> BL.fromStrict $ BS8.concat[" ", BS8.pack $ show x]) $ linePrecision line]
  where
    formatedTags =
      case M.null $ lineTags line of
        True -> ""
        False -> BL.concat $ [","] ++ (L.intersperse "," $ fmap (\ (key,value) -> BL.fromStrict $ TE.encodeUtf8 $ T.concat [key,"=",value] ) $ M.toList $ lineTags line)
    formatedValues = BL.concat $ L.intersperse "," $ fmap (\ (key,value) -> BL.concat[BL.fromStrict $ TE.encodeUtf8 key,"=",BL.fromStrict $ formatValue value]) $ M.toList $ lineFields line

    formatValue (Int val) = BS8.pack $ concat[show val,"i"]
    formatValue (String val) = BS8.pack $ concat ["\"",T.unpack val,"\""]
    formatValue (Float val) = BS8.pack $ show val
    formatValue (Bool True) = BS8.pack "t"
    formatValue (Bool False) = BS8.pack "f"

formatLines :: [Line] -> BL.ByteString
formatLines x = BL.concat $ fmap formatLine x


-- | Post a bunch of writes for (possibly multiple) series into a database.
post
  :: Config
  -> Text
  -> Line
  -> IO ()
post config databaseName d =
  postGeneric config databaseName [d]

-- | Post a bunch of writes for (possibly multiple) series into a database like
-- 'post' but with time precision.
postWithPrecision
  :: Config
  -> Text -- ^ Database name
  -> [Line]
  -> IO ()
postWithPrecision config databaseName =
  postGeneric config databaseName

postGeneric
  :: Config
  -> Text -- ^ Database name
  -> [Line]
  -> IO ()
postGeneric Config {..} databaseName write = do
  void $ httpLbsWithRetry configServerPool
    (makeRequest write)
    configHttpManager
  return ()
  where
    makeRequest series = def
      { HC.method = "POST"
      , HC.requestBody = HC.RequestBodyLBS $ formatLines series
      , HC.path = "/write"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&db=%s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
      }
    Credentials {..} = configCreds

-- | Monad transformer to batch up multiple writes of series to speed up
-- insertions.
newtype SeriesT m a = SeriesT (WriterT (DList Series) m a)
  deriving
    ( Functor, Applicative, Monad, MonadIO, MonadTrans
    , MonadWriter (DList Series)
    )

-- | Monad transformer to batch up multiple writes of points to speed up
-- insertions.
newtype PointT p m a = PointT (WriterT (DList (Vector Value)) m a)
  deriving
    ( Functor, Applicative, Monad, MonadIO, MonadTrans
    , MonadWriter (DList (Vector Value))
    )

runSeriesT :: Monad m => SeriesT m a -> m (a, [Series])
runSeriesT (SeriesT w) = do
  (a, series) <- runWriterT w
  return (a, DL.toList series)

-- | Write a single series data.
writeSeries
  :: (Monad m, ToSeriesData a)
  => Text
  -- ^ Series name
  -> a
  -- ^ Series data
  -> SeriesT m ()
writeSeries name = writeSeriesData name . toSeriesData

-- | Write a single series data.
writeSeriesData
  :: Monad m
  => Text
  -- ^ Series name
  -> SeriesData
  -- ^ Series data
  -> SeriesT m ()
writeSeriesData name a = tell . DL.singleton $ Series
  { seriesName = name
  , seriesData = a
  }

-- | Write a bunch of data for a single series. Columns for the points don't
-- need to be specified because they can be inferred from the type of @a@.
withSeries
  :: forall m a. (Monad m, ToSeriesData a)
  => Text
  -- ^ Series name
  -> PointT a m ()
  -> SeriesT m ()
withSeries name (PointT w) = do
  (_, values) <- lift $ runWriterT w
  tell $ DL.singleton Series
    { seriesName = name
    , seriesData = SeriesData
        { seriesDataColumns = toSeriesColumns (Proxy :: Proxy a)
        , seriesDataPoints = DL.toList values
        }
    }

-- | Write a data into a series.
writePoints
  :: (Monad m, ToSeriesData a)
  => a
  -> PointT a m ()
writePoints = tell . DL.singleton . toSeriesPoints

deleteSeries
  :: Config
  -> Text -- ^ Database name
  -> Text -- ^ Series name
  -> IO ()
deleteSeries config databaseName seriesName = runRequest_ config request
  where
    request = def
      { HC.method = "DELETE"
      , HC.path = escapeString $ printf "/db/%s/series/%s"
          (T.unpack databaseName)
          (T.unpack seriesName)
      , HC.queryString = escapeString $ printf "u=%s&p=%s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds config

-----------------------------------------------------------
-- Querying Data

-- | Query a specified database.
--
-- The query format is specified in the
-- <http://influxdb.org/docs/query_language/ InfluxDB Query Language>.
query
  :: Config
  -> Text -- ^ Database name
  -> Text -- ^ Query text
  -> IO Results
query config databaseName q = do
  runRequest config request
  where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&db=%s&q=%s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
          (T.unpack q)
      }
    Credentials {..} = configCreds config

-- | Construct streaming output
responseStream :: A.FromJSON a => HC.BodyReader -> IO (Stream IO a)
responseStream body = demandPayload $ \payload ->
  if BS.null payload
    then return Done
    else decode $ parseAsJson payload
  where
    demandPayload k = HC.brRead body >>= k
    decode (P.Done leftover value) = case A.fromJSON value of
      A.Success a -> return $ Yield a $ if BS.null leftover
        then responseStream body
        else decode $ parseAsJson leftover
      A.Error message -> jsonDecodeError message
    decode (P.Partial k) = demandPayload (decode . k)
    decode (P.Fail _ _ message) = jsonDecodeError message
    parseAsJson = P.parse A.json

-- | Query a specified database like 'query' but in a streaming fashion.
queryChunked
  :: FromSeries a
  => Config
  -> Text -- ^ Database name
  -> Text -- ^ Query text
  -> (Stream IO a -> IO b)
  -- ^ Action to handle the resulting stream of series
  -> IO b
queryChunked Config {..} databaseName q f =
  withPool configServerPool request $ \request' ->
    HC.withResponse request' configHttpManager $
      responseStream . HC.responseBody >=> S.mapM parse >=> f
  where
    parse series = case fromSeries series of
      Left reason -> seriesDecodeError reason
      Right a -> return a
    request = def
      { HC.path = escapeString $ printf "/db/%s/series"
          (T.unpack databaseName)
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=%s&chunked=true"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack q)
      }
    Credentials {..} = configCreds

-----------------------------------------------------------
-- Administration & Security

-- | List existing databases.
listDatabases :: Config -> IO [Database]-- [Database]
listDatabases Config {..} = do
  response <- httpLbsWithRetry configServerPool request configHttpManager
  let parsed :: Maybe Results = A.decode $ HC.responseBody response
  case parsed of
    Just x ->
      return $ L.concat $ fmap (\ y ->
        case serieswrapperSeries y of
          Just z -> L.concat $ fmap (fmap Database . convertList . L.concat . newseriesValues) z
          Nothing -> []) $ resultsResults x
    Nothing -> return []
    where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SHOW DATABASES"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds

    convertList :: [Value] -> [Text]
    convertList = fmap (\ (String x) -> x) . L.filter predicate

    predicate (String _) = True
    predicate _ = False


-- | Create a new database. Requires cluster admin privileges.
createDatabase :: Config -> Text -> IO ()
createDatabase config name = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=CREATE DATABASE %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack name)
      }
    Credentials {..} = configCreds config

-- | Drop a database. Requires cluster admin privileges.
dropDatabase
  :: Config
  -> Text -- ^ Database name
  -> IO ()
dropDatabase config databaseName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=DROP DATABASE %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
      }
    Credentials {..} = configCreds config

-- | List database users.
listUsers
  :: Config
  -> IO [User]
listUsers config@Config {..} = do
  response <- httpLbsWithRetry configServerPool request configHttpManager
  let parsed :: Maybe Results = A.decode $ HC.responseBody response
  case parsed of
    Just x ->
      return $ L.concat $ fmap (\ y ->
        case serieswrapperSeries y of
          Just z -> L.concat $ fmap (fmap(\ (a,b) -> User a b) . fmap convertList . newseriesValues) z
          Nothing -> []) $ resultsResults x
    Nothing -> return []
    where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SHOW USERS"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds

    convertList :: [Value] -> (Text,Bool)
    convertList [String x, Bool y] = (x,y)

-- | Add an user to the database users.
addUser
  :: Config
  -> Text -- ^ User name
  -> Text -- ^ Password
  -> IO ()
addUser config name password = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=CREATE USER %s WITH PASSWORD '%s'"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack name)
          (T.unpack $ T.replace "'" "\'" password)
      }
    Credentials {..} = configCreds config

-- | Delete an user from the database users.
deleteUser
  :: Config
  -> Text -- ^ User name
  -> IO ()
deleteUser config userName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=DROP USER %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

-- | Update password for the database user.
updateUserPassword
  :: Config
  -> Text -- ^ User name
  -> Text -- ^ New password
  -> IO ()
updateUserPassword config userName password =
  runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SET PASSWORD FOR %s = '%s'"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
          (T.unpack $ T.replace "'" "\'" password)
      }
    Credentials {..} = configCreds config

-- | Give admin privilege to the user.
grantAdminPrivilegeTo
  :: Config
  -> Text -- ^ User name
  -> IO ()
grantAdminPrivilegeTo config userName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=GRANT ALL PRIVILEGES TO %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

-- | Remove admin privilege from the user.
revokeAdminPrivilegeFrom
  :: Config
  -> Text -- ^ User name
  -> IO ()
revokeAdminPrivilegeFrom config userName =
  runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=REVOKE ALL PRIVILEGES FROM %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

ping :: Config -> IO ()
ping config = runRequest config request
  where
    request = def
      { HC.path = "/ping"
      }

-----------------------------------------------------------

httpLbsWithRetry
  :: IORef ServerPool
  -> HC.Request
  -> HC.Manager
  -> IO (HC.Response BL.ByteString)
httpLbsWithRetry pool request manager =
  withPool pool request $ \request' ->
    HC.httpLbs request' manager

withPool
  :: IORef ServerPool
  -> HC.Request
  -> (HC.Request -> IO a)
  -> IO a
withPool pool request f = do
  retryPolicy <- serverRetryPolicy <$> readIORef pool
  recovering retryPolicy handlers $ do
    server <- activeServer pool
    f $ makeRequest server
  where
    makeRequest Server {..} = request
      { HC.host = escapeText serverHost
      , HC.port = serverPort
      , HC.secure = serverSsl
      }
    handlers =
      [ const $ Handler $ \e -> case e of
        HC.FailedConnectionException {} -> retry
        HC.FailedConnectionException2 {} -> retry
        HC.InternalIOException {} -> retry
        HC.ResponseTimeout {} -> retry
        _ -> return False
      ]
    retry = True <$ failover pool

escapeText :: Text -> BS.ByteString
escapeText = escapeString . T.unpack

escapeString :: String -> BS.ByteString
escapeString = BS8.pack . escapeURIString isAllowedInURI

decodeJsonResponse
  :: A.FromJSON a
  => HC.Response BL.ByteString
  -> IO a
decodeJsonResponse response =
  case A.eitherDecode (HC.responseBody response) of
    Left reason -> jsonDecodeError reason
    Right a -> return a

runRequest :: A.FromJSON a => Config -> HC.Request -> IO a
runRequest Config {..} req = do
  response <- httpLbsWithRetry configServerPool req configHttpManager
  decodeJsonResponse response

runRequest_ :: Config -> HC.Request -> IO ()
runRequest_ Config {..} req =
  void $ httpLbsWithRetry configServerPool req configHttpManager

-----------------------------------------------------------
