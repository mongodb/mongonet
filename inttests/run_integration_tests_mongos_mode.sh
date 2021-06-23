#!/bin/bash
set -vx

echo ### RUNNING INTEGRATION TESTS
# If you do no set MONGO_DIR on the command line it will default to the value below
MONGO_DIR=${MONGO_DIR:-"/opt/mongo/64/3.4.0/bin"}
export MONGO_PORT=30000 # base port
HOSTNAME="localhost"

pkill mongod
sleep 5
pkill -9 mongod
sleep 5

rm -rf dbpath || true
mkdir -p dbpath/1 dbpath/2 dbpath/3 dbpath/4 dbpath/5 dbpath/6 || true
$MONGO_DIR/mongod --port $MONGO_PORT --dbpath `pwd`/dbpath/1 --logpath `pwd`/dbpath/1/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest
$MONGO_DIR/mongod --port $((MONGO_PORT+1)) --dbpath `pwd`/dbpath/2 --logpath `pwd`/dbpath/2/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest
$MONGO_DIR/mongod --port $((MONGO_PORT+2)) --dbpath `pwd`/dbpath/3 --logpath `pwd`/dbpath/3/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest

$MONGO_DIR/mongo --port $MONGO_PORT --eval "rs.initiate({'_id': 'proxytest', 'protocolVersion': 1, 'members': [{_id:0, host:'$HOSTNAME:$MONGO_PORT'},{_id:1, host:'$HOSTNAME:$((MONGO_PORT+1))',priority:0},{_id:2, host:'$HOSTNAME:$((MONGO_PORT+2))',priority:0}]})"

MONGO_PORT=40000
$MONGO_DIR/mongod --port $MONGO_PORT --dbpath `pwd`/dbpath/4 --logpath `pwd`/dbpath/4/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest2
$MONGO_DIR/mongod --port $((MONGO_PORT+1)) --dbpath `pwd`/dbpath/5 --logpath `pwd`/dbpath/5/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest2
$MONGO_DIR/mongod --port $((MONGO_PORT+2)) --dbpath `pwd`/dbpath/6 --logpath `pwd`/dbpath/6/mongod.log --fork --setParameter enableTestCommands=1 --replSet proxytest2

$MONGO_DIR/mongo --port $MONGO_PORT --eval "rs.initiate({'_id': 'proxytest2', 'protocolVersion': 1, 'members': [{_id:0, host:'$HOSTNAME:$MONGO_PORT'},{_id:1, host:'$HOSTNAME:$((MONGO_PORT+1))',priority:0},{_id:2, host:'$HOSTNAME:$((MONGO_PORT+2))',priority:0}]})"


sleep 10 # let replica sets reach steady state

go test -test.v -run TestProxyMongosMode
go test -test.v -run TestCommon
