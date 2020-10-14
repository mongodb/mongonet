#!/bin/bash

echo ### RUNNING INTEGRATION TESTS
# If you do no set MONGO_DIR on the command line it will default to the value below
MONGO_DIR=${MONGO_DIR:-"/opt/mongo/64/3.4.0/bin"}
export MONGO_PORT=30000 # base port
HOSTNAME=`hostname -f`

pkill mongod
sleep 5

rm -rf dbpath || true
mkdir -p dbpath/1 dbpath/2 dbpath/3 || true
$MONGO_DIR/mongod --port $MONGO_PORT --dbpath `pwd`/dbpath/1 --logpath `pwd`/dbpath/1/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest
$MONGO_DIR/mongod --port $((MONGO_PORT+1)) --dbpath `pwd`/dbpath/2 --logpath `pwd`/dbpath/2/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest
$MONGO_DIR/mongod --port $((MONGO_PORT+2)) --dbpath `pwd`/dbpath/3 --logpath `pwd`/dbpath/3/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest

$MONGO_DIR/mongo --port $MONGO_PORT --eval "rs.initiate({'_id': 'proxytest', 'protocolVersion': 1, 'writeConcernMajorityJournalDefault': false, 'members': [{_id:0, host:'$HOSTNAME:$MONGO_PORT'},{_id:1, host:'$HOSTNAME:$((MONGO_PORT+1))',priority:0},{_id:2, host:'$HOSTNAME:$((MONGO_PORT+2))',priority:0}]})"

sleep 10 # let replica set reach steady state

cd ..
go test -test.v -run TestProxySanityMongosMode

