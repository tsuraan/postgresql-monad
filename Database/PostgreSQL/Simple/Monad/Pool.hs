{-# LANGUAGE FlexibleContexts, FlexibleInstances, OverloadedStrings, ScopedTypeVariables #-}
module Database.PostgreSQL.Simple.Monad.Pool
( Pool
, Connection
, init
, addServer
, getReadWrite
, getReadOnly
, connectionConn
, relinquishReadWrite
, relinquishReadOnly
) where

import qualified Data.ByteString.Char8 as ByteStringC
import qualified Data.Set as Set
import Control.Concurrent.STM ( atomically, retry, orElse )
import Control.Concurrent.STM.TMVar
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM.TVar
import Control.Monad          ( forever )
import Data.ByteString        ( ByteString )
import Data.List              ( partition )
import Data.Set               ( Set )
import Control.Applicative    ( (<$>), (<*>) )
import Control.Exception      ( catch )
import Control.Concurrent     ( forkIO, threadDelay )
import System.Random          ( randomRIO )
import System.Timeout         ( timeout )
-- import Debug.Trace            ( trace )

import qualified Database.PostgreSQL.Simple.Monad as SM
import qualified Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Monad ( RxConnection, ReadOnly, ReadWrite )
import Database.PostgreSQL.Simple ( SqlError )

import Prelude hiding ( init, catch )

data Connector a = Connector
  { connectorAction :: IO (Maybe (RxConnection a))
  , connectorPoolId :: ByteString
  , connectorServer :: ByteString
  , connectorIndex  :: Int
  }

data Connection a = Connection
  { connectionCTor :: Connector a
  , connectionConn :: RxConnection a
  }

data EiConn = RoConn (Connection ReadOnly)
            | RwConn (Connection ReadWrite)

data Pool = Pool
  { poolId           :: ByteString
  , poolReadersPer   :: Int
  , poolWritersPer   :: Int
  , poolServers      :: TVar (Set ByteString)
  , poolRoConnectors :: TChan (Connector ReadOnly)
  , poolRoConns      :: TVar  [Connection ReadOnly]
  , poolRwConnectors :: TChan (Connector ReadWrite)
  , poolRwConns      :: TVar  [Connection ReadWrite]
  , poolCheckedOut   :: TVar  [EiConn]
  }

init :: Int -> Int -> IO Pool
init writers readers = do
  uid <- ByteStringC.pack <$> (sequence $ replicate 10 $ randomRIO ('a', 'z'))
  pool <- Pool uid writers readers
    <$> newTVarIO Set.empty
    <*> newTChanIO
    <*> newTVarIO []
    <*> newTChanIO
    <*> newTVarIO []
    <*> newTVarIO []
  _ <- forkIO $ doConnects pool
  return pool

addServer :: Pool -> ByteString -> IO ()
addServer pool connStr = atomically $ do
  known <- readTVar $ poolServers pool
  if Set.member connStr known
    then return ()
    else do
      writeTVar (poolServers pool) (Set.insert connStr known)
      mapM_ (writeTChan $ poolRoConnectors pool)
            (mkRoConnectors pool connStr)
      mapM_ (writeTChan $ poolRwConnectors pool)
            (mkRwConnectors pool connStr)

getReadWrite :: Pool -> IO (Either String (Connection ReadWrite))
getReadWrite = getRx poolRwConns RwConn

getReadOnly :: Pool -> IO (Either String (Connection ReadOnly))
getReadOnly = getRx poolRoConns RoConn

relinquishReadWrite :: Pool -> Connection ReadWrite -> IO ()
relinquishReadWrite = relinquishRx RwConn poolRwConns poolRwConnectors

relinquishReadOnly :: Pool -> Connection ReadOnly -> IO ()
relinquishReadOnly = relinquishRx RoConn poolRoConns poolRoConnectors

relinquishRx :: SM.CanLive (RxConnection a)
             => (Connection a -> EiConn)
             -> (Pool -> TVar [Connection a])
             -> (Pool -> TChan (Connector a))
             -> Pool
             -> Connection a
             -> IO ()
relinquishRx rxConn conns connectors pool conn = do
  if poolId pool /= connectorPoolId (connectionCTor conn)
    then error "BUG: Attempted to return a connection to the wrong pool"
    else do
      alive <- SM.alive $ connectionConn conn
      mMsg <- atomically $ do
        out <- readTVar $ poolCheckedOut pool
        msg <- case partition (== rxConn conn) out of
          ([], _) ->
            return $ Just "Did not find connection in checked out listing"
          (_, xs) -> do
            writeTVar (poolCheckedOut pool) xs
            return Nothing
        if alive
          then modifyTVar (conns pool) (conn:)
          else writeTChan (connectors pool) (connectionCTor conn)
        return msg

      case mMsg of
        Nothing -> return ()
        Just msg -> putStrLn msg


getRx :: (Pool -> TVar [b]) -> (b -> EiConn) -> Pool -> IO (Either String b)
getRx poolTxConns eiConn pool = do
  connT <- newEmptyTMVarIO
  _ <- timeout (5 * seconds) (do
      atomically $ do
        conns <- readTVar $ poolTxConns pool
        case conns of
          a:b -> do
            writeTVar (poolTxConns pool) b
            modifyTVar (poolCheckedOut pool) (eiConn a:)
            putTMVar connT a
          [] ->
            retry)

  mVal <- atomically $ tryTakeTMVar connT
  case mVal of
    Just conn -> return $ Right conn
    Nothing -> return $ Left "timeout"

mkRwConnectors :: Pool -> ByteString -> [Connector ReadWrite]
mkRwConnectors pool connStr =
  [ Connector conn (poolId pool) connStr idx
  | idx <- [1..poolWritersPer pool] ]
  where
  conn :: IO (Maybe (RxConnection ReadWrite))
  conn =
    catch (do c <- PGS.connectPostgreSQL connStr
              wrapped <- SM.wrapRw c
              case wrapped of
                Left _ -> do
                  PGS.close c
                  return Nothing
                Right w -> return $ Just w
                )
          (\(_ :: SqlError) -> return Nothing)

mkRoConnectors :: Pool -> ByteString -> [Connector ReadOnly]
mkRoConnectors pool connStr =
  [ Connector conn (poolId pool) connStr idx
  | idx <- [1..poolReadersPer pool] ]
  where
  conn :: IO (Maybe (RxConnection ReadOnly))
  conn =
    catch (Just . SM.wrapRo <$> PGS.connectPostgreSQL connStr)
          (\(_ :: SqlError) -> return Nothing)

doConnects :: Pool -> IO ()
doConnects pool = forever $ getNext >>= tryConnect
  where
  getNext =
    atomically $ (Left <$> readTChan (poolRoConnectors pool))
                 `orElse`
                 (Right <$> readTChan (poolRwConnectors pool))

  tryConnect (Left ro) = c' poolRoConnectors poolRoConns ro
  tryConnect (Right rw) = c' poolRwConnectors poolRwConns rw

  c' rxCtors rxConns rx = do
    mConn <- connectorAction rx
    case mConn of
      Nothing -> do
        _ <- forkIO $ do
          threadDelay $ 60 * seconds
          atomically $ writeTChan (rxCtors pool) rx
        return ()

      Just conn ->
        let ction = Connection rx conn
        in atomically $ modifyTVar (rxConns pool) (ction:)

instance Show (Connector x) where
  show ctor =
    "Connector<" ++ (show $ connectorPoolId ctor) ++
             "," ++ (show $ connectorServer ctor) ++
             "," ++ (show $ connectorIndex ctor) ++
             ">"

instance Eq (Connector x) where
  a == b =
    (connectorPoolId a) == (connectorPoolId b) &&
    (connectorServer a) == (connectorServer b) &&
    (connectorIndex a) == (connectorIndex b)

instance Eq (Connection x) where
  a == b = connectionCTor a == connectionCTor b

instance Eq EiConn where
  RoConn a == RoConn b = a == b
  RwConn a == RwConn b = a == b
  _ == _ = False

seconds :: Int
seconds = 1000000
