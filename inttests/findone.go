package inttests

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func RunProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, mode util.MongoConnectionMode, goals []ConnectionPerformanceTestGoal,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {
	for _, goal := range goals {
		if err := runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort, hostname, logger, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, mode, mongoClientFactory, proxyClientFactory); err != nil {
			return err
		}
	}
	return nil
}

func runFind(logger *slogger.Logger, client *mongo.Client, workerNum int, ctx context.Context, opts *options.FindOptions) (time.Duration, bool, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"
	doc := bson.D{}
	coll := client.Database(dbName).Collection(collName)

	cur, err := coll.Find(ctx, bson.D{{"x", workerNum}}, opts)
	if err != nil {
		return 0, false, fmt.Errorf("failed to run find. err=%v", err)
	}
	cur.Next(ctx)
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

func runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {
	preSetupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return util.DisableFailPoint(client, ctx)
	}
	setupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return insertDummyDocs(client, 1000, ctx)
	}

	testFunc := func(logger *slogger.Logger, client *mongo.Client, workerNum, iteration int, ctx context.Context) (elapsed time.Duration, success bool, err error) {
		return runFind(logger, client, workerNum, ctx, nil)
	}

	cleanupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return cleanup(client, ctx)
	}
	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(logger,
		hostname, mongoPort, proxyPort, mode,
		mongoClientFactory,
		proxyClientFactory,
		iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	return analyzeResults(err, workers, failedCount, avgLatencyMs, targetAvgLatencyMs, maxLatencyMs, targetMaxLatencyMs, results, percentiles, logger)
}

func runProxyConnectionFindOneWithMaxTimeMs(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {

	util.EnableFailPointForCommand(mongoPort, []string{"find"}, 50, 15)
	var client *mongo.Client
	ctx, cancelFunc := context.WithTimeout(context.Background(), util.ClientTimeoutSecForTests)
	defer cancelFunc()
	client, err := proxyClientFactory(hostname, proxyPort, mode, false, fmt.Sprintf("Test maxTimeMS"), ctx)
	if err != nil {
		logger.Logf(slogger.ERROR, "failed to init connection for cleanup. err=%v", err)
		return err
	}
	defer client.Disconnect(ctx)
	maxtime  := time.Millisecond * 10
	opts := options.FindOptions{MaxAwaitTime: &maxtime}
	_, _, err = runFind(logger, client, 1, ctx, &opts)
	util.DisableFailPoint(client, ctx)
	if err == nil || !strings.Contains(err.Error(), "MaxTimeMSExpired") {
		return fmt.Errorf("expected maxtimeMS error but got %v", err)
	}
	util.EnableFailPointForCommand(mongoPort, []string{"isMaster"}, 0, 1)
	maxtime  = time.Millisecond * 10000
	opts = options.FindOptions{MaxAwaitTime: &maxtime}
	_, _, err = runFind(logger, client, 1, ctx, &opts)
	if err != nil && strings.Contains(err.Error(), "MaxTimeMSExpired") {
		util.DisableFailPoint(client, ctx)
		return fmt.Errorf("expected maxtimeMS error but got %v", err)
	}
	maxtime  = time.Millisecond * 9
	opts = options.FindOptions{MaxAwaitTime: &maxtime}
	_, _, err = runFind(logger, client, 1, ctx, &opts)
	util.DisableFailPoint(client, ctx)
	if err != nil && strings.Contains(err.Error(), "MaxTimeMSExpired") {
		return fmt.Errorf("expected maxtimeMS error but got %v", err)
	}


	return nil
}
