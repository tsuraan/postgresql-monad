{-# LANGUAGE FlexibleInstances, OverloadedStrings #-}
module Database.PostgreSQL.Simple.Monad.Pool
( Pool
, init
, showIO
, insert
, insertRaw
, getReadWrite
, getReadOnly
) where

import Control.Concurrent.STM ( atomically, retry )
import Control.Concurrent.STM.TMVar
import Control.Monad          ( forM_ )
import Data.List              ( partition )
import Debug.Trace            ( trace )

import qualified Database.PostgreSQL.Simple.Monad as SM
import Database.PostgreSQL.Simple.Monad ( RxConnection, ReadOnly, ReadWrite )
import Database.PostgreSQL.Simple ( Connection, Only(..), query_ )

import Prelude hiding ( init )

data Conn = RO (RxConnection ReadOnly)
          | RW (RxConnection ReadWrite)

instance Show Conn where
  show (RO _) = "RO Postgres Connection"
  show (RW _) = "RW Postgres Connection"

newtype Pool = Pool (TMVar [Conn])

class PoolItem a where
  toConn :: a -> Conn

instance PoolItem (RxConnection ReadOnly) where
  toConn x = case SM.rxOrig x of
              Nothing -> RO x
              Just y -> toConn y

instance PoolItem (RxConnection ReadWrite) where
  toConn x = case SM.rxOrig x of
              Nothing -> RW x
              Just y -> toConn y

init :: IO Pool
init = Pool `fmap` newTMVarIO []

showIO :: Pool -> IO ()
showIO (Pool conns) = do
  putStrLn "Pool Contents:"
  conns' <- atomically $ readTMVar conns
  forM_ conns' (\o -> putStrLn $ "  " ++ show o)

insert :: PoolItem i => Pool -> i -> IO ()
insert pool i = insert' pool $ toConn i

insertRaw :: Pool -> Connection -> IO ()
insertRaw pool conn = do
  [Only rdOnly] <- query_ conn "SHOW transaction_read_only"
  if rdOnly == ("on"::String)
    then insert pool $ SM.wrapRo conn
    else do
      Right rw <- SM.wrapRw conn
      insert pool rw

insert' :: Pool -> Conn -> IO ()
insert' (Pool conns) conn = do
  atomically $ do
    lst <- takeTMVar conns
    putTMVar conns $ lst ++ [conn]

getReadWrite :: Pool -> IO (RxConnection ReadWrite)
getReadWrite (Pool conns) = do
  atomically $ do
    lst <- takeTMVar conns
    let (rs, ws) = splitRW lst
    case ws of
      (RW c):rst -> do
        putTMVar conns $ rs ++ rst
        return c
      [] -> do
        putTMVar conns lst
        retry
      _ -> do
        putTMVar conns lst
        trace ("getReadWrite: Impossible data:" ++show (rs,ws))
              retry

getReadOnly :: Pool -> IO (RxConnection ReadOnly)
getReadOnly (Pool conns) = do
  atomically $ do
    lst <- takeTMVar conns
    case splitRW lst of
      ((RO c):rst, ws) -> do
        putTMVar conns $ rst ++ ws
        return c
      ([], (RW c):rst) -> do
        putTMVar conns $ rst
        return $ SM.wrapRo' c
      ([], []) -> do
        putTMVar conns []
        retry
      o@(rs, ws) -> do
        putTMVar conns $ rs++ws
        trace ("getReadOnly: Impossible data:" ++ show o)
              retry

{- Split a list of Cons into its reader (RO) and writer (RW) connections -}
splitRW :: [Conn] -> ([Conn], [Conn])
splitRW conns = partition isRO conns
  where
  isRO (RO _) = True
  isRO (RW _) = False

