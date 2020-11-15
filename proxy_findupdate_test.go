package mongonet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
)

func runFindUpdate(logger *slogger.Logger, host string, proxyPort, workerNum int) (time.Duration, bool, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"

	client, err := getTestClient(host, proxyPort, false, fmt.Sprintf("worker-%v", workerNum))
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
	_, err = coll.UpdateOne(goctx, bson.D{{"x", workerNum}}, bson.D{{"$set", bson.D{{"i", workerNum}}}})
	if err != nil {
		return 0, false, fmt.Errorf("failed to update the doc. err=%v", err)
	}
	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func privateConnectionPerformanceTesterFindUpdate(maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T) {
	Iterations := 20
	mongoPort, proxyPort, _ := getHostAndPorts()
	t.Logf("using proxy port=%v, pool size=%v", proxyPort, maxPoolSize)
	hostToUse := "localhost"

	serverPort := proxyPort
	preSetupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int) error {
		return nil
	}
	setupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int) error {
		return insertDummyDocs(hostname, serverPort, 1000)
	}

	testFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int) (elapsed time.Duration, success bool, err error) {
		return runFindUpdate(logger, hostname, serverPort, workerNum)
	}

	cleanupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int) error {
		return cleanup(hostname, serverPort)
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort)
	pc.LogLevel = slogger.DEBUG
	proxy := NewProxy(pc)

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(proxy.NewLogger("tester"),
		hostToUse, mongoPort, proxyPort,
		Iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	if err != nil {
		t.Errorf("failed to run tests. err=%v", err)
	}

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

func TestProxyMongodModeConnectionPerformanceFindUpdateFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindUpdate(0, 5, 50, 200, t)
}

func TestProxyMongodModeConnectionPerformanceFindUpdateTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindUpdate(0, 20, 100, 500, t)
}

func TestProxyMongodModeConnectionPerformanceFindUpdateSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindUpdate(0, 60, 200, 1500, t)
}
