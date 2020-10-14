#!/bin/bash

echo ### RUNNING INTEGRATION TESTS
# If you do no set MONGO_DIR on the command line it will default to the value below
MONGO_DIR=${MONGO_DIR:-"/opt/mongo/64/3.4.0/bin"}
export MONGO_PORT=30000

pkill mongod
sleep 5

mkdir dbpath || true
$MONGO_DIR/mongod --port $MONGO_PORT --dbpath `pwd`/dbpath --logpath `pwd`/dbpath/mongod.log --fork --setParameter enableTestCommands=1

cd ..
go test -test.v -run TestProxySanity

