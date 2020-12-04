package inttests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	LocalDbName        = "testlocal"
	RemoteConnCollName = "test"
)

func RunProxyConnectionPerformanceRemoteConns(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, mode util.MongoConnectionMode, goals []ConnectionPerformanceTestGoal,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {
	for _, goal := range goals {
		if err := runProxyConnectionPerformanceRemoteConns(iterations, mongoPort, proxyPort, hostname, logger, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, mode, mongoClientFactory, proxyClientFactory); err != nil {
			return err
		}
	}
	return nil
}

func findOne(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	doc := bson.D{}
	res := coll.FindOne(goctx, bson.D{})
	if res.Err() != nil {
		return res.Err()
	}
	if err := res.Decode(&doc); err != nil {
		return err
	}
	ix := mongonet.BSONIndexOf(doc, "a")
	val, _, err := mongonet.GetAsInt(doc[ix])
	if err != nil {
		return err
	}
	if coll.Database().Name() == util.RemoteDbNameForTests {
		if val != 2 {
			return fmt.Errorf("got unexpected value=%v", val)
		}
	} else {
		if val != 1 {
			return fmt.Errorf("got unexpected value=%v", val)
		}
	}

	return nil
}

func runRemoteConns(logger *slogger.Logger, client *mongo.Client, workerNum int, ctx context.Context) (time.Duration, bool, error) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()

	localColl := client.Database(LocalDbName).Collection(RemoteConnCollName)
	remoteColl := client.Database(util.RemoteDbNameForTests).Collection(RemoteConnCollName)

	// we'd like to simulate a workload in which 50% of the connections are local and 50% are remote
	coll := localColl
	if workerNum%2 == 0 {
		coll = remoteColl
	}

	if err := findOne(logger, coll, ctx); err != nil {
		return 0, false, err
	}

	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func runProxyConnectionPerformanceRemoteConns(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode,
	mongoClientFactory util.ClientFactoryFunc,
	proxyClientFactory util.ClientFactoryFunc,
) error {
	preSetupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		client2, err := mongoClientFactory(hostname, 40000, util.Cluster, false, "presetup", ctx)
		if err != nil {
			return err
		}
		defer client2.Disconnect(ctx)
		if err := util.DisableFailPoint(client2, ctx); err != nil {
			return err
		}
		return util.DisableFailPoint(client, ctx)
	}
	setupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		localColl := client.Database(LocalDbName).Collection(RemoteConnCollName)
		client2, err := mongoClientFactory(hostname, 40000, util.Cluster, false, "presetup", ctx)
		if err != nil {
			return err
		}
		defer client2.Disconnect(ctx)
		remoteColl := client2.Database(util.RemoteDbNameForTests).Collection(RemoteConnCollName)
		if _, err := localColl.InsertOne(ctx, bson.D{{"a", 1}}); err != nil {
			return err
		}
		if _, err := remoteColl.InsertOne(ctx, bson.D{{"a", 2}}); err != nil {
			return err
		}
		return nil
	}

	testFunc := func(logger *slogger.Logger, client *mongo.Client, workerNum, iteration int, ctx context.Context) (elapsed time.Duration, success bool, err error) {
		return runRemoteConns(logger, client, workerNum, ctx)
	}

	cleanupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return nil
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
