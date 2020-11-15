package mongonet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
)

func insertDummyDocs(host string, proxyPort, numOfDocs int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "insertDummyDocs")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	dbName, collName := "test2", "foo"

	defer client.Disconnect(ctx)

	coll := client.Database(dbName).Collection(collName)
	// insert some docs
	docs := make([]interface{}, numOfDocs)
	for i := 0; i < numOfDocs; i++ {
		docs[i] = bson.D{{"x", i}}
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("initial insert failed. err: %v", err)
	}
	return nil
}

func cleanup(host string, proxyPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "cleanup")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	dbName, collName := "test2", "foo"

	defer client.Disconnect(ctx)

	coll := client.Database(dbName).Collection(collName)
	return coll.Drop(ctx)
}

func runFind(logger *slogger.Logger, host string, proxyPort, workerNum int, mode MongoConnectionMode) (time.Duration, bool, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"

	client, err := getTestClient(host, proxyPort, mode, false, fmt.Sprintf("worker-%v", workerNum))
	if err != nil {
		return 0, false, fmt.Errorf("failed to get a test client. err=%v", err)
	}
	goctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(goctx); err != nil {
		return 0, false, fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(goctx)
	doc := bson.D{}
	coll := client.Database(dbName).Collection(collName)

	cur, err := coll.Find(goctx, bson.D{{"x", workerNum}})
	if err != nil {
		return 0, false, fmt.Errorf("failed to run find. err=%v", err)
	}
	cur.Next(goctx)
	if err := cur.Decode(&doc); err != nil {
		return 0, false, fmt.Errorf("failed to decode find. err=%v", err)
	}
	if len(doc) <= 0 {
		return 0, false, fmt.Errorf("doc x:%v not found", workerNum)
	}
	i := BSONIndexOf(doc, "x")
	v, _, err := GetAsInt(doc[i])
	if err != nil {
		return 0, false, fmt.Errorf("failed to inspect %v. err=%v", doc, err)
	}
	if v != workerNum {
		return 0, false, fmt.Errorf("fetched wrong doc %v for worker=%v", doc, workerNum)
	}
	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func privateConnectionPerformanceTesterFindOne(mode MongoConnectionMode, maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T) {
	Iterations := 20
	mongoPort, proxyPort, hostname := getHostAndPorts()
	t.Logf("using proxy port=%v, pool size=%v", proxyPort, maxPoolSize)
	hostToUse := hostname
	if mode == Direct {
		hostToUse = "localhost"
	}
	serverPort := proxyPort
	preSetupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return disableFailPoint(hostname, mongoPort, mode)
	}
	setupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return insertDummyDocs(hostname, serverPort, 1000, mode)
	}

	testFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int, mode MongoConnectionMode) (elapsed time.Duration, success bool, err error) {
		return runFind(logger, hostname, serverPort, workerNum, mode)
	}

	cleanupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return cleanup(hostname, serverPort, mode)
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort, maxPoolSize, DefaultMaxPoolIdleTimeSec, mode, true)
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

	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(proxy.NewLogger("tester"),
		hostToUse, mongoPort, proxyPort, mode,
		Iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	if failedCount > 0 {
		t.Errorf("failed workers %v", failedCount)
	}
	if avgLatencyMs > targetAvgLatencyMs {
		t.Errorf("average latency %v > %v", avgLatencyMs, targetAvgLatencyMs)
	}
	if maxLatencyMs > targetMaxLatencyMs {
		t.Errorf("max latency %v > %v", maxLatencyMs, targetMaxLatencyMs)
	}
	if len(results) == 0 {
		t.Errorf("no successful runs!")
	}
	t.Logf("ALL DONE workers=%v, successful runs=%v, avg=%vms, max=%vms, failures=%v, percentiles=%v\nresults=%v", workers, len(results), avgLatencyMs, maxLatencyMs, failedCount, percentiles, results)
}

func TestProxyConnectionPerformanceFindOneMongodMode(t *testing.T) {
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 5, 50, 100, t)
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 20, 250, 1500, t)
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 50, 1500, 2000, t)
	privateConnectionPerformanceTesterFindOne(Direct, 0, 75, 1500, 3000, t)

}

func TestProxyConnectionPerformanceFindOneMongosModeFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 5, 50, 200, t)
}

func TestProxyConnectionPerformanceFindOneMongosModeTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 20, 100, 500, t)
}

func TestProxyConnectionPerformanceFindOneMongosModeSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 60, 200, 1500, t)
}
