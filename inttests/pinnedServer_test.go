package inttests

import (
	"testing"
	"time"

	"context"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func TestPinnedServer(t *testing.T) {
	mongoPort, proxyPort, hostname := util.GetTestHostAndPorts()
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false, nil)
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}
	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}
	go proxy.Run()

	goctx := context.Background()
	// insert some docs
	client, err := util.GetTestClient(hostname, mongoPort, util.Cluster, false, "test", goctx)
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(goctx)
	if err = insertDummyDocs(client, 100, goctx); err != nil {
		t.Fatal(err)
	}
	defer cleanup(client, goctx)
	time.Sleep(time.Second) // let data replicate

	proxyClient, err := util.GetTestClient(hostname, proxyPort, util.Cluster, false, "testProxy1", goctx)
	if err != nil {
		panic(err)
	}
	defer proxyClient.Disconnect(goctx)

	dbSec := proxyClient.Database("test2")
	cur, err := dbSec.Collection("foo", options.Collection().SetReadPreference(readpref.Secondary())).Find(goctx, bson.D{}, options.Find().SetBatchSize(10))
	if err != nil {
		t.Fatal(err)
	}
	docsFound := 0
	i := cur.RemainingBatchLength()
	for i > 0 {
		cur.Next(goctx)
		i--
		docsFound++

	}
	// perform a getMore
	for cur.Next(goctx) {
		docsFound++
	}
	if docsFound != 100 {
		t.Fatalf("expected to find 100 docs but found %v", docsFound)
	}
}
