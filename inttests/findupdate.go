package inttests

import (
	"context"
	"fmt"
	"time"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func RunProxyConnectionPerformanceFindUpdate(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, mode util.MongoConnectionMode, goals []ConnectionPerformanceTestGoal,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {
	for _, goal := range goals {
		if err := runProxyConnectionPerformanceFindUpdate(iterations, mongoPort, proxyPort, hostname, logger, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, mode, mongoClientFactory, proxyClientFactory); err != nil {
			return err
		}
	}
	return nil
}

func runFindUpdate(logger *slogger.Logger, client *mongo.Client, workerNum int, ctx context.Context) (time.Duration, bool, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"
	doc := bson.D{}
	coll := client.Database(dbName).Collection(collName)

	cur, err := coll.Find(ctx, bson.D{{"x", workerNum}})
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
	_, err = coll.UpdateOne(ctx, bson.D{{"x", workerNum}}, bson.D{{"$set", bson.D{{"i", workerNum}}}})
	if err != nil {
		return 0, false, fmt.Errorf("failed to update the doc. err=%v", err)
	}
	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func runProxyConnectionPerformanceFindUpdate(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode,
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
		return runFindUpdate(logger, client, workerNum, ctx)
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
