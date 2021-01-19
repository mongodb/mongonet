package inttests

import (
	"testing"
	"time"

	"github.com/mongodb/mongonet/util"
)

func TestProxyMongosModeConnectionPerformanceFindOne(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 50,
			MaxLatencyMs: 300,
		},
		{
			Workers:      20,
			AvgLatencyMs: 100,
			MaxLatencyMs: 500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 300,
			MaxLatencyMs: 1500,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Cluster, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceFindOne, nil)
	}

}

func TestProxyMongodModeConnectionPerformanceFindOne(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 50,
			MaxLatencyMs: 300,
		},
		{
			Workers:      20,
			AvgLatencyMs: 100,
			MaxLatencyMs: 500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 300,
			MaxLatencyMs: 1500,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Direct, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceFindOne, nil)
	}
}

func TestFindWithMaxTimeMS(t *testing.T) {
	goal := ConnectionPerformanceTestGoal{
		Workers:      5,
		AvgLatencyMs: 50,
		MaxLatencyMs: 200,
	}
	blockFind := map[string]time.Duration{"find": 10 * time.Millisecond}
	RunIntTest(util.Cluster, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionFindOneWithMaxTimeMs, blockFind)
}
