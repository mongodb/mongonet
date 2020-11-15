package mongonet

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
)

/*
	DoConcurrencyTestRun - can be used by external applications to test out concurrency and pooling performance
	see full example on proxy_findone_test.go:TestProxyConnectionPerformanceMongodMode
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
			return
		}
	}

	if setupFunc != nil {
		err = setupFunc(logger, hostname, mongoPort, proxyPort, mode)
		if err != nil {
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
