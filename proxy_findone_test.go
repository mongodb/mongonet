package mongonet

import (
	"testing"

	"github.com/mongodb/slogger/v2/slogger"
)

func privateConnectionPerformanceTesterFindOne(mode MongoConnectionMode, maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T) {
	Iterations := 20
	mongoPort, proxyPort, hostname := getHostAndPorts()
	t.Logf("using proxy port=%v, pool size=%v", proxyPort, maxPoolSize)
	hostToUse := hostname
	if mode == Direct {
		hostToUse = "localhost"
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort, maxPoolSize, DefaultMaxPoolIdleTimeSec, mode, false)
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

	if err := runProxyConnectionPerformanceFindOne(Iterations, mongoPort, proxyPort, hostToUse, proxy.NewLogger("tester"), workers, targetAvgLatencyMs, targetMaxLatencyMs, mode, getTestClient); err != nil {
		t.Error(err)
	}
}

func TestProxyMongosModeConnectionPerformanceFindOneFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 5, 50, 200, t)
}

func TestProxyMongosModeConnectionPerformanceFindOneTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 20, 100, 500, t)
}

func TestProxyMongosModeConnectionPerformanceFindOneSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 60, 200, 1500, t)
}

func TestProxyMongodModeConnectionPerformanceFindOneFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Direct, 0, 5, 50, 200, t)
}

func TestProxyMongodModeConnectionPerformanceFindOneTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Direct, 0, 20, 100, 500, t)
}

func TestProxyMongodModeConnectionPerformanceFindOneSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterFindOne(Direct, 0, 60, 200, 1500, t)
}
