package inttests

import (
	"testing"

	"github.com/mongodb/mongonet/util"
)

func TestProxyMongosModeConnectionPerformanceFindUpdateRandomDur(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 650,
			MaxLatencyMs: 1200,
		},
		{
			Workers:      20,
			AvgLatencyMs: 700,
			MaxLatencyMs: 1500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 900,
			MaxLatencyMs: 2000,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Cluster, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceFindUpdateRandomDur)
	}

}

func TestProxyMongodModeConnectionPerformanceFindUpdateRandomDur(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 650,
			MaxLatencyMs: 1200,
		},
		{
			Workers:      20,
			AvgLatencyMs: 700,
			MaxLatencyMs: 1500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 900,
			MaxLatencyMs: 2000,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Direct, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceFindUpdateRandomDur)
	}
}
