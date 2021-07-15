package inttests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/mongo"
)

/*
	DoConcurrencyTestRun - can be used by external applications to test out concurrency and pooling performance
	preSetUpFunc will use a client over the underlying mongo
	setupFunc, testFunc and cleanupFunc will use clients over the proxy
	see full example on RunProxyConnectionPerformanceFindOne
*/
func DoConcurrencyTestRun(logger *slogger.Logger,
	hostname string, mongoPort, proxyPort int, mode util.MongoConnectionMode,
	mongoClientFactory func(host string, port int, mode util.MongoConnectionMode, secondaryReads bool, appName string, ctx context.Context) (*mongo.Client, error),
	proxyClientFactory func(host string, port int, mode util.MongoConnectionMode, secondaryReads bool, appName string, ctx context.Context) (*mongo.Client, error),
	iterations, workers int,
	preSetupFunc func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error,
	setupFunc func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error,
	testFunc func(logger *slogger.Logger, client *mongo.Client, workerNum, iteration int, ctx context.Context) (elapsed time.Duration, success bool, err error),
	cleanupFunc func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error,
) (results []int64, failedCount int32, maxLatencyMs, avgLatencyMs int64, percentiles map[int]int, err error) {
	resLock := sync.RWMutex{}
	if preSetupFunc != nil {
		var client *mongo.Client
		ctx, cancelFunc := context.WithTimeout(context.Background(), util.ClientTimeoutSecForTests)
		defer cancelFunc()
		client, err = mongoClientFactory(hostname, mongoPort, mode, false, "presetup", ctx)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to init connection for pre-setup. err=%v", err)
			return
		}
		defer client.Disconnect(ctx)
		err = preSetupFunc(logger, client, ctx)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to run pre-setup. err=%v", err)
			return
		}
	}

	if setupFunc != nil {
		var client *mongo.Client
		ctx, cancelFunc := context.WithTimeout(context.Background(), util.ClientTimeoutSecForTests)
		defer cancelFunc()
		client, err = proxyClientFactory(hostname, proxyPort, mode, false, "setup", ctx)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to init connection for setup. err=%v", err)
			return
		}
		defer client.Disconnect(ctx)
		err = setupFunc(logger, client, ctx)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to run setup. err=%v", err)
			return
		}
	}

	defer func() {
		var client *mongo.Client
		ctx, cancelFunc := context.WithTimeout(context.Background(), util.ClientTimeoutSecForTests)
		defer cancelFunc()
		client, err = proxyClientFactory(hostname, proxyPort, mode, false, "cleanup", ctx)
		if err != nil {
			logger.Logf(slogger.ERROR, "failed to init connection for cleanup. err=%v", err)
			return
		}
		defer client.Disconnect(ctx)
		cleanupFunc(logger, client, ctx)
	}()

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
					var client *mongo.Client
					ctx, cancelFunc := context.WithTimeout(context.Background(), util.ClientTimeoutSecForTests)
					defer cancelFunc()
					client, err = proxyClientFactory(hostname, proxyPort, mode, false, fmt.Sprintf("worker-%v", num), ctx)
					if err != nil {
						logger.Logf(slogger.ERROR, "failed to init connection for cleanup. err=%v", err)
						return
					}
					defer client.Disconnect(ctx)
					elapsed, success, err = testFunc(logger, client, num, j, ctx)
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

func RunIntTest(mode util.MongoConnectionMode, maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T,
	testFunc func(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode,
		mongoClientFactory util.ClientFactoryFunc,
		proxyClientFactory util.ClientFactoryFunc,
	) error,
	blockCommands map[string]time.Duration,
) {
	callerName := getCallerName()

	runtime.SetBlockProfileRate(100)
	runtime.SetMutexProfileFraction(5)

	defer writeOutAllProfiles(callerName)

	Iterations := 10
	mongoPort, proxyPort, hostname := util.GetTestHostAndPorts()
	t.Logf("using proxy port=%v, pool size=%v", proxyPort, maxPoolSize)
	hostToUse := hostname
	if mode == util.Direct {
		hostToUse = "localhost"
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort, maxPoolSize, DefaultMaxPoolIdleTimeSec, mode, false, blockCommands)
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	if mode == util.Cluster {
		if err := proxy.AddRemoteConnection(util.RemoteRsName, "mongodb://localhost:40000,localhost:40001,localhost:40002", "testproxy", false, ServerSelectionTimeoutSecForTests, maxPoolSize, DefaultMaxPoolIdleTimeSec, DefaultConnectionPoolHeartbeatIntervalMs, nil); err != nil {
			t.Fatal(err)
		}
		defer proxy.ClearRemoteConnection(util.RemoteRsName, 10)
	}
	go proxy.Run()

	if err := testFunc(Iterations, mongoPort, proxyPort, hostToUse, proxy.NewLogger("tester"), workers, targetAvgLatencyMs, targetMaxLatencyMs, mode, util.GetTestClient, util.GetTestClient); err != nil {
		t.Fatal(err)
	}
}

func getCallerName() string {
	callerPc, _, _, ok := runtime.Caller(2)
	if !ok {
		panic("could not get caller info")
	}
	callerFunc := runtime.FuncForPC(callerPc)
	if callerFunc == nil {
		panic("could not get func for caller PC")
	}
	fullCallerName := callerFunc.Name()
	periodIndex := strings.LastIndex(fullCallerName, ".")
	return fullCallerName[periodIndex+1:]
}

func writeOutAllProfiles(filenamePrefix string) {
	for _, profile := range pprof.Profiles() {
		writeOutProfile(filenamePrefix, profile)
	}
}

func writeOutProfile(filenamePrefix string, profile *pprof.Profile) {
	profileName := profile.Name()
	_ = os.Mkdir("profiles", 0755)
	path := filepath.Join("profiles", filenamePrefix+"-"+profileName)
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("Failed to create %v ; err = %v\n", path, err)
		return
	}
	defer f.Close()
	err = profile.WriteTo(f, 0)
	if err != nil {
		fmt.Printf("Failed to write profile %v to %v ; err = %v", profileName, path, err)
		return
	}
}

func analyzeResults(err error, workers int, failedCount int32, avgLatencyMs, targetAvgLatencyMs, maxLatencyMs, targetMaxLatencyMs int64, results []int64, percentiles map[int]int, logger *slogger.Logger) error {
	defer logger.Logf(slogger.INFO, "ALL DONE workers=%v, successful runs=%v, avg=%vms, max=%vms, failures=%v, percentiles=%v\nresults=%v", workers, len(results), avgLatencyMs, maxLatencyMs, failedCount, percentiles, results)
	if err != nil {
		return fmt.Errorf("ERROR failed to run tests. err=%v", err)
	}
	if failedCount > 0 {
		return fmt.Errorf("ERROR failed workers %v", failedCount)
	}
	if avgLatencyMs > targetAvgLatencyMs {
		return fmt.Errorf("ERROR average latency %v > %v", avgLatencyMs, targetAvgLatencyMs)
	}
	if maxLatencyMs > targetMaxLatencyMs {
		return fmt.Errorf("ERROR max latency %v > %v", maxLatencyMs, targetMaxLatencyMs)
	}
	if len(results) == 0 {
		return fmt.Errorf("ERROR no successful runs")
	}
	return nil
}
