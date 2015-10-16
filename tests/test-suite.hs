{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
import Control.Applicative
import Control.Exception as E
import Control.Monad.Trans
import Data.Aeson (decode)
import Data.Aeson.Types (parseMaybe)
import Data.Int
import Data.List (find)
import qualified Data.Map as M
import Data.Maybe (isJust)
import Data.Monoid
import Data.Text (Text)
import Data.Unique
import Data.Word
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V

import Test.Tasty.HUnit
import Test.Tasty.TH
import Test.Tasty.QuickCheck hiding (reason)
import qualified Network.HTTP.Client as HC

import Database.InfluxDB
import Database.InfluxDB.Types(NewSeries(..),Results(..),parseDatabase)
import Database.InfluxDB.TH
import qualified Database.InfluxDB.Stream as S

prop_fromValue_toValue_identity_Value :: Value -> Bool
prop_fromValue_toValue_identity_Value = fromValueToValueIdentity

prop_fromValue_toValue_identity_Bool :: Bool -> Bool
prop_fromValue_toValue_identity_Bool = fromValueToValueIdentity

prop_fromValue_toValue_identity_Int :: Int -> Bool
prop_fromValue_toValue_identity_Int = fromValueToValueIdentity

prop_fromValue_toValue_identity_Int8 :: Int8 -> Bool
prop_fromValue_toValue_identity_Int8 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Int16 :: Int16 -> Bool
prop_fromValue_toValue_identity_Int16 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Int32 :: Int32 -> Bool
prop_fromValue_toValue_identity_Int32 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Int64 :: Int64 -> Bool
prop_fromValue_toValue_identity_Int64 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Word8 :: Word8 -> Bool
prop_fromValue_toValue_identity_Word8 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Word16 :: Word16 -> Bool
prop_fromValue_toValue_identity_Word16 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Word32 :: Word32 -> Bool
prop_fromValue_toValue_identity_Word32 = fromValueToValueIdentity

prop_fromValue_toValue_identity_Double :: Double -> Bool
prop_fromValue_toValue_identity_Double = fromValueToValueIdentity

prop_fromValue_toValue_identity_Text :: T.Text -> Bool
prop_fromValue_toValue_identity_Text = fromValueToValueIdentity

prop_fromValue_toValue_identity_LazyText :: TL.Text -> Bool
prop_fromValue_toValue_identity_LazyText = fromValueToValueIdentity

prop_fromValue_toValue_identity_String :: String -> Bool
prop_fromValue_toValue_identity_String = fromValueToValueIdentity

prop_fromValue_toValue_identity_Maybe_Int :: Maybe Int -> Bool
prop_fromValue_toValue_identity_Maybe_Int = fromValueToValueIdentity

-------------------------------------------------

instance Arbitrary Value where
  arbitrary = oneof
    [ Int <$> arbitrary
    , Float <$> arbitrary
    , String <$> arbitrary
    , Bool <$> arbitrary
    , pure Null
    ]

instance Arbitrary T.Text where
  arbitrary = T.pack <$> arbitrary

instance Arbitrary TL.Text where
  arbitrary = TL.pack <$> arbitrary

fromValueToValueIdentity :: (Eq a, FromValue a, ToValue a) => a -> Bool
fromValueToValueIdentity a = fromValue (toValue a) == Right a

-------------------------------------------------

case_ex1 :: Assertion
case_ex1 = do
  let line = Line "measurement" M.empty (M.singleton "value" $ Float 12) Nothing
  assertBool "Values do not match" $ "measurement value=12.0" == formatLine line

case_ex2 :: Assertion
case_ex2 = do
  let line = Line "measurement" M.empty (M.singleton "value" $ Float 12) (Just 1439587925)
  assertBool "Values do not match" $ "measurement value=12.0 1439587925" == formatLine line

case_ex3 :: Assertion
case_ex3 = do
  let line = Line "measurement" (M.singleton "foo" "bar") (M.singleton "value" $ Float 12) Nothing
  assertBool "Values do not match" $ "measurement,foo=bar value=12.0" == formatLine line

case_ex4 :: Assertion
case_ex4 = do
  let line = Line "measurement" (M.singleton "foo" "bar") (M.singleton "value" $ Float 12) (Just 1439587925)
  assertBool "Values do not match" $ "measurement,foo=bar value=12.0 1439587925" == formatLine line

case_ex5 :: Assertion
case_ex5 = do
  let line = Line "measurement" (M.union (M.singleton "foo" "bar") (M.singleton "bat" "baz")) (M.union (M.singleton "value" $ Float 12) (M.singleton "otherval" $ Float 21)) (Just 1439587925)
  assertBool "Values do not match" $ "measurement,bat=baz,foo=bar otherval=21.0,value=12.0 1439587925" == formatLine line

case_parse_empty_response :: Assertion
case_parse_empty_response = do
  let r :: (Maybe Results) = decode "{\"results\":[{}]}"
  assertBool "Decode Failed" (isJust r)

case_parse_response :: Assertion
case_parse_response = do
  let r :: Maybe Results = decode "{\"results\":[{\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"test_109\"],[\"test_85\"]]}]}]}"
  assertBool "Decode Failed" $ isJust r



case_post :: Assertion
case_post = runTest $ \config ->
  withTestDatabase config $ \database -> do
    name <- liftIO newName
    let line = Line name M.empty (M.singleton "value" (Float 42.0)) Nothing
    post config database [line]
    ss :: Results <- query config database $ "select value from " <> name
    print ss

case_post_multiple :: Assertion
case_post_multiple = runTest $ \config ->
  withTestDatabase config $ \database -> do
    name <- liftIO newName
    let line = Line name M.empty (M.singleton "value" (Float 42.0)) Nothing
        line2 = Line name M.empty (M.singleton "value" (Float 40.0)) Nothing
    post config database [line,line2]
    ss :: Results <- query config database $ "select value from " <> name
    print ss


case_listDatabases :: Assertion
case_listDatabases = runTest $ \config ->
  withTestDatabase config $ \name -> do
    databases <- listDatabases config
    assertBool ("No such database: " ++ T.unpack name) $
      any ((name ==) . databaseName) databases

case_create_then_drop_database :: Assertion
case_create_then_drop_database = runTest $ \config -> do
  name <- newName
  dropDatabaseIfExists config name
  createDatabase config name
  listDatabases config >>= \databases ->
    assertBool ("No such database: " ++ T.unpack name) $
      any ((name ==) . databaseName) databases
  dropDatabase config name
  listDatabases config >>= \databases ->
    assertBool ("Found a dropped database: " ++ T.unpack name) $
      all ((name /=) . databaseName) databases

case_add_then_delete_database_users :: Assertion
case_add_then_delete_database_users = runTest $ \config ->
  withTestDatabase config $ \name -> do
    newUserName <- newName
    addUser config newUserName "somePassword"
    let newCreds = rootCreds
          { credsUser = newUserName
          , credsPassword = "somePassword" }
        newConfig = config { configCreds = newCreds }
    listUsers config >>= \users ->
      assertBool ("No such user: " <> T.unpack newUserName) $
        any ((newUserName ==) . userName) users
    deleteUser config newUserName
    listUsers config >>= \users ->
      assertBool ("Found a deleted user: " <> T.unpack newUserName) $
        all ((newUserName /=) . userName) users

case_update_database_user_password :: Assertion
case_update_database_user_password = runTest $ \config ->
  withTestDatabase config $ \name -> do
    newUserName <- newName
    addUser config newUserName "somePassword"
    listUsers config >>= \users ->
      assertBool ("No such user: " <> T.unpack newUserName) $
        any ((newUserName ==) . userName) users
    updateUserPassword config newUserName "otherPassword"
    deleteUser config newUserName

case_grant_revoke_database_user :: Assertion
case_grant_revoke_database_user = runTest $ \config ->
  withTestDatabase config $ \name -> do
    newUserName <- newName
    addUser config newUserName "somePassword"
    listUsers config >>= \users ->
      assertBool ("No such user: " <> T.unpack newUserName) $
        any ((newUserName ==) . userName) users
    grantAdminPrivilegeTo config newUserName
    listUsers config >>= \users ->
      case find ((newUserName ==) . userName) users of
        Nothing -> assertFailure $ "No such user: " <> T.unpack newUserName
        Just user -> assertBool
          ("User is not privileged: " <> T.unpack newUserName)
          (userIsAdmin user)
    revokeAdminPrivilegeFrom config newUserName
    listUsers config >>= \users ->
      case find ((newUserName ==) . userName) users of
        Nothing -> assertFailure $ "No such user: " <> T.unpack newUserName
        Just user -> assertBool
          ("User is still privileged: " <> T.unpack newUserName)
          (not $ userIsAdmin user)
    deleteUser config newUserName

-------------------------------------------------

dropDatabaseIfExists :: Config -> Text -> IO ()
dropDatabaseIfExists config name =
  dropDatabase config name
    `catchAll` \_ -> return ()

-------------------------------------------------

runTest :: (Config -> IO a) -> IO a
runTest f = do
  pool <- newServerPool localServer []
  HC.withManager settings (f . Config rootCreds pool)
  where
    settings = HC.defaultManagerSettings

newName :: IO Text
newName = do
  uniq <- newUnique
  return $ T.pack $ "test_" ++ show (hashUnique uniq)

withTestDatabase :: Config -> (Text -> IO a) -> IO a
withTestDatabase config = bracket acquire release
  where
    acquire = do
      name <- newName
      dropDatabaseIfExists config name
      createDatabase config name
      return name
    release = dropDatabase config

catchAll :: IO a -> (SomeException -> IO a) -> IO a
catchAll = E.catch

assertStatusCodeException :: Show a => IO a -> IO ()
assertStatusCodeException io = do
  r <- try io
  case r of
    Left e -> case fromException e of
      Just HC.StatusCodeException {} -> return ()
      _ ->
        assertFailure $ "Expect a StatusCodeException, but got " ++ show e
    Right ss -> assertFailure $ "Expect an exception, but got " ++ show ss

-------------------------------------------------

main :: IO ()
main = $defaultMainGenerator
