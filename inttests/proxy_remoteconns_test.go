package inttests

import (
	"testing"

	"github.com/mongodb/mongonet/util"
)

func TestProxyMongosModeConnectionPerformanceRemoteConns(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 50,
			MaxLatencyMs: 200,
		},
		{
			Workers:      20,
			AvgLatencyMs: 100,
			MaxLatencyMs: 500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 200,
			MaxLatencyMs: 1500,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Cluster, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceRemoteConns)
	}
}

func TestProxyMongosModeConnectionPerformanceRetryOnRemoteConns(t *testing.T) {
	goals := []ConnectionPerformanceTestGoal{
		{
			Workers:      5,
			AvgLatencyMs: 50,
			MaxLatencyMs: 200,
		},
		{
			Workers:      20,
			AvgLatencyMs: 100,
			MaxLatencyMs: 500,
		},
		{
			Workers:      60,
			AvgLatencyMs: 200,
			MaxLatencyMs: 1500,
		},
	}
	for _, goal := range goals {
		RunIntTest(util.Cluster, 0, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, t, runProxyConnectionPerformanceRetryOnRemoteConns)
	}
}
