{-# LANGUAGE OverloadedStrings #-}
{-|
  This is a simple motivating example for why postgresql-transactions may be
  a useful thing to have.  Prepare the database with the following:

  BEGIN;
  CREATE TABLE Users( userid SERIAL NOT NULL PRIMARY KEY
                    , username TEXT NOT NULL UNIQUE);

  CREATE TABLE Messages( msgid SERIAL NOT NULL PRIMARY KEY
                       , msg TEXT NOT NULL UNIQUE);

  CREATE TABLE Logs( logid SERIAL NOT NULL PRIMARY KEY
                   , logged TIMESTAMP NOT NULL DEFAULT now()
                   , userid INT NOT NULL REFERENCES Users
                   , msgid INT NOT NULL REFERENCES Messages
                   );
  COMMIT;
-}

import Database.PostgreSQL.Simple.Monad
import Database.PostgreSQL.Simple ( connectPostgreSQL, Only(..) ) 
import Data.ByteString ( ByteString )
import Control.Exception

getUserId :: ByteString -> PostgresM ReadOnly Int
getUserId username = do 
  rows <- roQuery "SELECT userid FROM Users WHERE username=?"
                  (Only username)
  case rows of
    [] -> abortNotFound
    (Only userid):_ -> return userid

mkUserId :: ByteString -> PostgresM ReadWrite Int
mkUserId username = go
  where
  go = (ro $ getUserId username) `onNotFound` (mk' `onUniqueViolation` go)
  
  mk' = do
    [Only userid] <- query
      "INSERT INTO Users(username) VALUES(?) RETURNING userid" 
      (Only username)
    return userid 

getMsgId :: ByteString -> PostgresM ReadOnly Int
getMsgId msg = do
  rows <- roQuery "SELECT msgid FROM Messages where msg=?" (Only msg)
  case rows of
    [] -> abortNotFound
    (Only msgid):_ -> return msgid
     
mkMsgId :: ByteString -> PostgresM ReadWrite Int
mkMsgId msg = go
  where
  go = (ro $ getMsgId msg) `onNotFound` (mk' `onUniqueViolation` go)

  mk' = do
    [Only msgid] <- query
      "INSERT INTO Messages(msg) VALUES(?) RETURNING msgid"
      (Only msg) 
    return msgid
     
logMsg :: ByteString -> ByteString -> PostgresM ReadWrite Int 
logMsg username msg = do
    userid <- mkUserId username
    msgid  <- mkMsgId msg 
    [Only logid] <- query "INSERT INTO Logs(userid, msgid) VALUES(?, ?) \ 
                          \RETURNING logid"
                          (userid, msgid)
    return logid

main = do
  c <- connectPostgreSQL "host=localhost"
  Right w <- wrapRw c
  Right jimid <- runRw w $ mkUserId "jim"
  putStrLn ("jim id is " ++ (show jimid))

  Right msgid1 <- runRw w $ logMsg "al" "login"
  print msgid1
  Right msgid2 <- runRw w $ logMsg "bob" "login"
  print msgid2
  Right msgid3 <- runRw w $ logMsg "al" "read email"
  print msgid3
  Right msgid4 <- runRw w $ logMsg "al" "logout"
  print msgid4
  Right msgid5 <- runRw w $ logMsg "bob" "logout"
  print msgid5

