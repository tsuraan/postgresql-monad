{-# LANGUAGE GeneralizedNewtypeDeriving, ScopedTypeVariables, EmptyDataDecls
           , OverloadedStrings #-}
module Database.PostgreSQL.Simple.Monad
( RxConnection
, PostgresM
, ReadOnly
, ReadWrite
, SimpleError(..)
, rxOrig
, wrapRo
, wrapRo'
, wrapRw
, runRw
, runRo
, debug
, abortNotFound
, abortWithMessage
, roQuery
, roQuery_
, ro
, query
, query_
, returning
, execute
, execute_
, executeMany
, onNotFound
, onIntegrityViolation
, onRestrictViolation
, onNotNullViolation
, onForeignKeyViolation
, onUniqueViolation
, onCheckViolation
, onExclusionViolation
) where

import qualified Control.Monad.Error as Error
import Control.Applicative  ( (<$>) )
import Control.Exception    ( SomeException, Handler(..), catch, catches, mask )
import Control.Monad        ( replicateM )
import Control.Monad.Error  ( MonadError, ErrorT, Error(..) )
import Control.Monad.Reader ( ReaderT, runReaderT, asks )
import Control.Monad.Trans  ( liftIO )
import Data.Int             ( Int64 )
import Data.String          ( fromString )
import System.Random        ( randomRIO )
import Prelude hiding ( catch )

import qualified Database.PostgreSQL.Simple as Simple
import Database.PostgreSQL.Simple ( SqlError, Connection, Query, ToRow, FromRow )

data SimpleError = SqlError SqlError
                 | UnknownError SomeException
                 | NotFound
                 | Aborted String
                 deriving ( Show )

instance Error SimpleError where
  strMsg = Aborted

data ReadOnly
data ReadWrite

data RxConnection rx = RxConnection
  { rxConn :: Connection
  -- used with wrapRo' so we can get the ReadWrite back from the ReadOnly
  , rxOrig :: Maybe (RxConnection ReadWrite)
  }

newtype PostgresMReader rx = State { readerConn :: RxConnection rx }

newtype PostgresM rx a =
  M { fromM :: ErrorT SimpleError (ReaderT (PostgresMReader rx) IO) a }
  deriving ( Monad, MonadError SimpleError )

wrapRo :: Connection -> RxConnection ReadOnly
wrapRo conn = RxConnection conn Nothing

wrapRo' :: RxConnection ReadWrite -> RxConnection ReadOnly
wrapRo' orig = RxConnection (rxConn orig) (Just orig)

runRo :: RxConnection ReadOnly -> PostgresM ReadOnly a
      -> IO (Either SimpleError a)
runRo conn (M act) = runReaderT (Error.runErrorT act) (State conn)

wrapRw :: Connection -> IO (Either String (RxConnection ReadWrite))
wrapRw simpleConn = do
  [Simple.Only rdOnly] <- Simple.query_ simpleConn "SHOW transaction_read_only"
  if rdOnly == ("on"::String)
    then
      return $ Left "Cannot wrap a read-only connection in a read-write RxConn"
    else
      return $ Right $ RxConnection simpleConn Nothing

runRw :: RxConnection ReadWrite -> PostgresM ReadWrite a
      -> IO (Either SimpleError a)
runRw conn (M act) =
  mask $ \restore -> do
    Simple.begin $ rxConn conn
    result <-
      catch (restore $ runReaderT (Error.runErrorT act) (State conn))
            (\(e :: SomeException) -> return $ Left $ UnknownError e)
    case result of
      Left _  -> Simple.rollback $ rxConn conn
      Right _ -> Simple.commit $ rxConn conn
    return result
    
debug :: String -> PostgresM rx ()
debug msg = M $ liftIO $ putStrLn msg

abortNotFound :: PostgresM rx a
abortNotFound = M $ Error.throwError NotFound

abortWithMessage :: String -> PostgresM rx a
abortWithMessage msg = M $ Error.throwError $ Aborted msg

onNotFound :: PostgresM ReadWrite a -> PostgresM ReadWrite a
           -> PostgresM ReadWrite a
onNotFound = onChecked isNotFound
  where
  isNotFound NotFound = True
  isNotFound _ = False

onIntegrityViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                     -> PostgresM ReadWrite a
onIntegrityViolation = onChecked isIntegrity
  where
  isIntegrity (SqlError _) = True
  isIntegrity _ = False

onRestrictViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                    -> PostgresM ReadWrite a
onRestrictViolation = onChecked isRestrict
  where
  isRestrict (SqlError s) = Simple.sqlState s == "23001"
  isRestrict _ = False

onNotNullViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                   -> PostgresM ReadWrite a
onNotNullViolation = onChecked isNotNull
  where
  isNotNull (SqlError s) = Simple.sqlState s == "23502"
  isNotNull _ = False

onForeignKeyViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                      -> PostgresM ReadWrite a
onForeignKeyViolation = onChecked isForeignKey
  where
  isForeignKey (SqlError s) = Simple.sqlState s == "23503"
  isForeignKey _ = False

onUniqueViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                  -> PostgresM ReadWrite a
onUniqueViolation = onChecked isUnique
  where
  isUnique (SqlError s) = Simple.sqlState s == "23505"
  isUnique _ = False

onCheckViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                 -> PostgresM ReadWrite a
onCheckViolation = onChecked isCheck
  where
  isCheck (SqlError s) = Simple.sqlState s == "23514"
  isCheck _ = False

onExclusionViolation :: PostgresM ReadWrite a -> PostgresM ReadWrite a
                     -> PostgresM ReadWrite a
onExclusionViolation = onChecked isExclusion
  where
  isExclusion (SqlError s) = Simple.sqlState s == "23P01"
  isExclusion _ = False

onChecked :: (SimpleError -> Bool) -> PostgresM ReadWrite a
          -> PostgresM ReadWrite a -> PostgresM ReadWrite a
onChecked useFallback desired fallback = do
  spname <- genSavepoint
  let sp = fromString $ "SAVEPOINT " ++ spname
      rb = fromString $ "ROLLBACK TO SAVEPOINT " ++ spname
      re = fromString $ "RELEASE SAVEPOINT " ++ spname

  _ <- execute_ sp
  Error.catchError (do res <- desired
                       _ <- execute_ re
                       return res)
                   (\e -> if useFallback e
                            then do _ <- execute_ rb
                                    fallback
                            else Error.throwError e)

  where
  genSavepoint :: PostgresM ReadWrite String
  genSavepoint = M $ liftIO $ map toEnum <$> replicateM 10 (randomRIO (97, 122))

roQuery :: (ToRow q, FromRow r) => Query -> q -> PostgresM rx [r]
roQuery q args = run' (\conn -> Simple.query conn q args)

roQuery_ :: FromRow r => Query -> PostgresM rx [r]
roQuery_ q = run' (\conn -> Simple.query_ conn q)

ro :: PostgresM ReadOnly a -> PostgresM ReadWrite a
ro action = M $ do
  conn <- wrapRo' `fmap` asks readerConn
  res <- liftIO $ runReaderT (Error.runErrorT $ fromM action) (State conn)
  case res of
    Left err -> Error.throwError err
    Right val -> return val

query :: (ToRow q, FromRow r) => Query -> q -> PostgresM ReadWrite [r]
query q args = run' (\conn -> Simple.query conn q args)

query_ :: FromRow r => Query -> PostgresM ReadWrite [r]
query_ q = run' (\conn -> Simple.query_ conn q)

returning :: (ToRow q, FromRow r) => Query -> [q] -> PostgresM ReadWrite [r]
returning q args = run' (\conn -> Simple.returning conn q args)

execute :: ToRow q => Query -> q -> PostgresM ReadWrite Int64
execute q args = run' (\conn -> Simple.execute conn q args)

execute_ :: Query -> PostgresM ReadWrite Int64
execute_ q = run' (\conn -> Simple.execute_ conn q)

executeMany :: ToRow q => Query -> [q] -> PostgresM ReadWrite Int64
executeMany q args = run' (\conn -> Simple.executeMany conn q args)

run' :: (Connection -> IO a) -> PostgresM rx a
run' action = M $ do
  conn <- rxConn `fmap` asks readerConn
  result <- liftIO $ catches (Right `fmap` action conn)
                      [ Handler (\(ex :: SqlError) -> return $ Left $ SqlError ex)
                      , Handler (\(ex :: SomeException) -> return $ Left $ UnknownError ex)
                      ]
  case result of
    Right res -> return res
    Left err -> Error.throwError err

