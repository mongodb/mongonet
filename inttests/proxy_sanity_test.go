package inttests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-test/deep"
	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	ParallelClients                = 5
	InterruptedAtShutdownErrorCode = 11600
)

func enableFailPointCloseConnection(mongoPort int) error {
	// cannot fail "find" because it'll prevent certain driver machinery when operating against replica sets to work properly
	client, err := util.GetTestClient("localhost", mongoPort, util.Direct, false, "enableFailPointCloseConnection")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", []string{"update"}},
			{"closeConnection", true},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func enableFailPointErrorCode(mongoPort, errorCode int) error {
	// cannot fail "find" because it'll prevent certain driver machinery when operating against replica sets to work properly
	client, err := util.GetTestClient("localhost", mongoPort, util.Direct, false, "enableFailPointErrorCode")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", []string{"update"}},
			{"errorCode", errorCode},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func runCommandFailOp(host string, proxyPort int, mode util.MongoConnectionMode, secondaryReads bool) error {
	client, err := util.GetTestClient(host, proxyPort, mode, secondaryReads, "runCommandFailOp")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	res := client.Database("test").RunCommand(ctx, bson.D{{"createIndexes", "bla"}})
	return res.Err()
}

func runInsertFindUpdate(host string, proxyPort, iteration int, shouldFail bool, mode util.MongoConnectionMode, secondaryReads bool) error {
	client, err := util.GetTestClient(host, proxyPort, mode, secondaryReads, fmt.Sprintf("runInsertFindUpdate-iteration%v", iteration))
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	coll := client.Database("test").Collection(fmt.Sprintf("bar_%v", iteration))

	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop collection: %v", err)
	}

	docIn := bson.D{{"foo", int32(17)}}
	if _, err := coll.InsertOne(ctx, docIn); err != nil {
		return fmt.Errorf("can't insert: %v", err)
	}
	if secondaryReads {
		time.Sleep(5 * time.Second) // let doc replicate
	}
	docOut := bson.D{}
	fopts := options.FindOne().SetProjection(bson.M{"_id": 0})
	err = coll.FindOne(ctx, bson.D{}, fopts).Decode(&docOut)
	if err != nil {
		return fmt.Errorf("can't find: %v", err)
	}
	if len(docIn) != len(docOut) {
		return fmt.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
	}
	if diff := deep.Equal(docIn[0], docOut[0]); diff != nil {
		return fmt.Errorf("docs don't match: %v", diff)
	}
	_, err = coll.UpdateOne(ctx, bson.D{}, bson.D{{"$set", bson.D{{"foo", int32(18)}}}})
	if err != nil {
		if shouldFail {
			return nil
		}
		return err
	}
	if shouldFail {
		return fmt.Errorf("expected update to fail but it didn't")
	}
	return nil
}

func runSanityOps(host string, proxyPort, parallelism int, shouldFail bool, t *testing.T, mode util.MongoConnectionMode, secondaryReads bool) int32 {
	var wg sync.WaitGroup
	var failing int32
	if parallelism == 1 {
		err := runInsertFindUpdate(host, proxyPort, 0, shouldFail, mode, secondaryReads)
		if err != nil {
			t.Error(err)
			return 1
		}
		return 0
	}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			start := time.Now()
			t.Logf("*** %v started running worker %v", start, iteration)
			err := runInsertFindUpdate(host, proxyPort, iteration, shouldFail, mode, secondaryReads)
			if err != nil {
				t.Error(err)
				atomic.AddInt32(&failing, 1)
			}
			t.Logf("*** %v finished running worker %v in %v. err=%v", time.Now(), iteration, time.Since(start), err)
		}(i)
	}
	wg.Wait()
	return failing
}

func disableFailPoint(mongoPort int) error {
	client, err := util.GetTestClient("localhost", mongoPort, util.Direct, false, "disableFailPoint")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	return util.DisableFailPoint(client, ctx)
}

func privateSanityTestMongodMode(secondaryMode bool, t *testing.T) {
	mongoPort, proxyPort, _ := util.GetHostAndPorts()
	t.Logf("using proxy port %v", proxyPort)

	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := getProxyConfig("localhost", mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Direct, false)
	privateSanityTester(t, pc, "localhost", proxyPort, mongoPort, ParallelClients, util.Direct, secondaryMode)
}

func privateSanityTestMongosMode(secondaryMode bool, t *testing.T) {
	mongoPort, proxyPort, hostname := util.GetHostAndPorts()
	t.Logf("using proxy port %v", proxyPort)
	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false)
	privateSanityTester(t, pc, hostname, proxyPort, mongoPort, 5, util.Cluster, secondaryMode)
}

// backing mongo must be started with --setParameter enableTestCommands=1
func privateSanityTester(t *testing.T, pc ProxyConfig, host string, proxyPort, mongoPort, parallelism int, mode util.MongoConnectionMode, secondaryReads bool) {
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	currConns := int64(0)
	if conns := proxy.GetConnectionsCreated(); conns != 0 {
		t.Fatalf("expected connections created to equal 0 but was %v", conns)
	}

	if cleared := proxy.GetPoolCleared(); cleared != 0 {
		t.Fatalf("expected pool cleared to equal 0 but was %v", cleared)
	}

	failing := runSanityOps(host, proxyPort, parallelism*2, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}
	if cleared := proxy.GetPoolCleared(); cleared != 0 {
		t.Fatalf("expected pool cleared to equal 0 but was %v", cleared)
	}

	conns := proxy.GetConnectionsCreated()
	if conns == 0 {
		t.Fatalf("expected connections created to increase but were still %v", conns)
	}
	currConns = conns

	t.Log("*** run ops again to confirm connections are reused")
	failing = runSanityOps(host, proxyPort, parallelism, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}
	conns = proxy.GetConnectionsCreated()
	if conns-int64(currConns) > (int64(parallelism) * 2) {
		t.Fatalf("expected connections created to not significantly increase (%v), but got %v", currConns, conns)
	}
	currConns = conns
	if cleared := proxy.GetPoolCleared(); cleared != 0 {
		t.Fatalf("expected pool cleared to equal 0 but was %v", cleared)
	}

	t.Log("*** induce a non-network problem")
	if err := runCommandFailOp(host, proxyPort, mode, secondaryReads); err == nil {
		t.Fatalf("expected command to fail but it suceeded")
	}
	if cleared := proxy.GetPoolCleared(); cleared != 0 {
		t.Fatalf("expected pool cleared to equal 0 but was %v", cleared)
	}

	t.Log("*** enable error code response failpoint and run ops")
	enableFailPointErrorCode(mongoPort, InterruptedAtShutdownErrorCode)
	failing = runSanityOps(host, proxyPort, parallelism, true, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}
	cleared := proxy.GetPoolCleared()
	if cleared == 0 {
		t.Fatalf("expected pool cleared to be gt 0 but was 0")
	}

	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}

	t.Log("*** enable close connection failpoint and run ops")
	enableFailPointCloseConnection(mongoPort)
	failing = runSanityOps(host, proxyPort, parallelism, true, t, mode, secondaryReads)

	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}
	if cleared2 := proxy.GetPoolCleared(); cleared2 < cleared {
		t.Fatalf("expected pool cleared to be gt previous value but was %v", cleared2)
	}

	conns = proxy.GetConnectionsCreated()
	if conns == currConns {
		t.Fatalf("expected connections created to increase from (%v), but got %v", currConns, conns)
	}
	currConns = conns

	t.Log("*** disable failpoint and run ops")
	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}

	failing = runSanityOps(host, proxyPort, parallelism, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}

	conns = proxy.GetConnectionsCreated()
	if conns == currConns {
		t.Fatalf("expected connections created to increase from (%v), but got %v", currConns, conns)
	}
}

func TestProxyMongosModeSanityPrimary(t *testing.T) {
	privateSanityTestMongosMode(false, t)
}

func TestProxyMongosModeSanitySecondary(t *testing.T) {
	privateSanityTestMongosMode(true, t)
}

func TestProxyMongodModeSanityPrimary(t *testing.T) {
	privateSanityTestMongodMode(false, t)
}

// this is expected to go through the same code as primary mode since Mongod mode essentially ignores read preference from client
func TestProxyMongodModeSanitySecondary(t *testing.T) {
	privateSanityTestMongodMode(true, t)
}
