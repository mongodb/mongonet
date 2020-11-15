package mongonet

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
)

/*
	DoConcurrencyTestRun - can be used by external applications to test out concurrency and pooling performance
	see full example on RunProxyConnectionPerformanceFindOne
*/
func DoConcurrencyTestRun(logger *slogger.Logger,
	hostname string, mongoPort, proxyPort int, mode MongoConnectionMode,
	iterations, workers int,
	preSetupFunc func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error,
	setupFunc func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error,
	testFunc func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int, mode MongoConnectionMode) (elapsed time.Duration, success bool, err error),
	cleanupFunc func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error,
) (results []int64, failedCount int32, maxLatencyMs, avgLatencyMs int64, percentiles map[int]int, err error) {
	resLock := sync.RWMutex{}
	if preSetupFunc != nil {
		err = preSetupFunc(logger, hostname, mongoPort, proxyPort, mode)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to run pre-setup. err=%v", err)
			return
		}
	}

	if setupFunc != nil {
		err = setupFunc(logger, hostname, mongoPort, proxyPort, mode)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to run setup. err=%v", err)
			return
		}
	}

	defer cleanupFunc(logger, hostname, mongoPort, proxyPort, mode)

	var wg sync.WaitGroup
ITERATIONS:
	for j := 0; j < iterations; j++ {
		logger.Logf(slogger.INFO, "*** starting iteration %v", j)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(num int, wg *sync.WaitGroup) {
				defer wg.Done()
				runtime.Gosched()
				var err error
				var success bool
				var elapsed time.Duration
				logger.Logf(slogger.DEBUG, "running worker-%v", num)
				if testFunc != nil {
					elapsed, success, err = testFunc(logger, hostname, mongoPort, proxyPort, num, j, mode)
					logger.Logf(slogger.INFO, "worker-%v success=%v, elapsed=%v, err=%v", num, success, elapsed, err)
					if !success {
						logger.Logf(slogger.WARN, "worker-%v failed! err=%v", num, err)
					}
				}
				if success {
					resLock.Lock()
					results = append(results, elapsed.Milliseconds())
					resLock.Unlock()
				}
				if !success {
					atomic.AddInt32(&failedCount, 1)
				}
			}(i, &wg)
		}
		wg.Wait()
		logger.Logf(slogger.INFO, "*** finished iteration %v", j)
		if atomic.LoadInt32(&failedCount) > 0 {
			logger.Logf(slogger.INFO, "*** iteration %v has failures. breaking", j)
			break ITERATIONS
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(results) == 0 {
		return
	}
	sum := int64(0)
	resLock.RLock()
	sortedResults := make(sort.IntSlice, len(results))
	for i, val := range results {
		if val > maxLatencyMs {
			maxLatencyMs = val
		}
		sum += val
		sortedResults[i] = int(val)
	}
	resLock.RUnlock()
	failed := atomic.LoadInt32(&failedCount)
	if failed != int32(workers*iterations) {
		avgLatencyMs = int64(float64(sum) / (float64(workers*iterations) - float64(failed)))
	}

	// this is assuming that we're not dealing with huge datasets
	sort.Sort(sortedResults)
	percentiles = make(map[int]int, 5)
	percentiles[50] = sortedResults[int(float32(len(sortedResults))*0.5)]
	percentiles[80] = sortedResults[int(float32(len(sortedResults))*0.8)]
	percentiles[90] = sortedResults[int(float32(len(sortedResults))*0.9)]
	percentiles[95] = sortedResults[int(float32(len(sortedResults))*0.95)]
	percentiles[99] = sortedResults[int(float32(len(sortedResults))*0.95)]
	return
}

type ConnectionPerformanceTestGoal struct {
	Workers      int
	AvgLatencyMs int64
	MaxLatencyMs int64
}

func RunProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, mode MongoConnectionMode) error {
	if err := runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort, hostname, logger, 5, 50, 200, mode); err != nil {
		return err
	}
	if err := runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort, hostname, logger, 20, 100, 500, mode); err != nil {
		return err
	}
	return runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort, hostname, logger, 60, 200, 1500, mode)
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

func runProxyConnectionPerformanceFindOne(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode MongoConnectionMode) error {
	preSetupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return disableFailPoint(hostname, mongoPort, mode)
	}
	setupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return insertDummyDocs(hostname, proxyPort, 1000, mode)
	}

	testFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int, mode MongoConnectionMode) (elapsed time.Duration, success bool, err error) {
		return runFind(logger, hostname, proxyPort, workerNum, mode)
	}

	cleanupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return cleanup(hostname, proxyPort, mode)
	}
	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(logger,
		hostname, mongoPort, proxyPort, mode,
		iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	if err != nil {
		return fmt.Errorf("failed to run tests. err=%v", err)
	}
	if failedCount > 0 {
		return fmt.Errorf("failed workers %v", failedCount)
	}
	if avgLatencyMs > targetAvgLatencyMs {
		return fmt.Errorf("average latency %v > %v", avgLatencyMs, targetAvgLatencyMs)
	}
	if maxLatencyMs > targetMaxLatencyMs {
		return fmt.Errorf("max latency %v > %v", maxLatencyMs, targetMaxLatencyMs)
	}
	if len(results) == 0 {
		return fmt.Errorf("no successful runs")
	}
	logger.Logf(slogger.INFO, "ALL DONE workers=%v, successful runs=%v, avg=%vms, max=%vms, failures=%v, percentiles=%v\nresults=%v", workers, len(results), avgLatencyMs, maxLatencyMs, failedCount, percentiles, results)
	return nil
}
