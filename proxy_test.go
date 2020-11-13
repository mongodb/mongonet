package mongonet

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	ServerSelectionTimeoutSecForTests = 5
	ParallelClients                   = 5
)

type MyFactory struct {
	mode      MongoConnectionMode
	mongoPort int
	proxyPort int
}

func (myf *MyFactory) NewInterceptor(ps *ProxySession) (ProxyInterceptor, error) {
	return &MyInterceptor{ps, myf.mode, myf.mongoPort, myf.proxyPort}, nil
}

type MyResponseInterceptor struct {
	mode      MongoConnectionMode
	mongoPort int
	proxyPort int
}

func fixIsMasterCluster(doc bson.D) (SimpleBSON, error) {
	newDoc := bson.D{}
	newDoc = append(newDoc, bson.E{"ismaster", true})
	newDoc = append(newDoc, bson.E{"msg", "isdbgrid"})
	newDoc = append(newDoc, bson.E{"ok", 1})
	for _, elem := range doc {
		switch elem.Key {
		case "maxBsonObjectSize", "maxMessageSizeBytes", "maxWriteBatchSize", "localTime", "logicalSessionTimeoutMinutes", "connectionId", "maxWireVersion", "minWireVersion", "topologyVersion", "operationTime", "$clusterTime":
			newDoc = append(newDoc, elem)
		}
	}
	return SimpleBSONConvert(newDoc)
}

func fixHostNamesValue(elem interface{}, old, new int) interface{} {
	switch val := elem.(type) {
	case string:
		if strings.Contains(val, fmt.Sprintf(":%v", old)) {
			return strings.ReplaceAll(val, fmt.Sprintf(":%v", old), fmt.Sprintf(":%v", new))
		}
		if strings.Contains(val, fmt.Sprintf(":%v", old+1)) {
			return strings.ReplaceAll(val, fmt.Sprintf(":%v", old+1), fmt.Sprintf(":%v", new+1))
		}
		if strings.Contains(val, fmt.Sprintf(":%v", old+2)) {
			return strings.ReplaceAll(val, fmt.Sprintf(":%v", old+2), fmt.Sprintf(":%v", new+2))
		}
	case bson.D:
		return fixHostNames(val, old, new)
	case primitive.A:
		for j, elem2 := range val {
			val[j] = fixHostNamesValue(elem2, old, new)
		}
		return elem
	case []interface{}:
		for j, elem2 := range val {
			val[j] = fixHostNamesValue(elem2, old, new)
		}
		return elem
	case []bson.D:
		for j, elem2 := range val {
			val[j] = fixHostNamesValue(elem2, old, new).(bson.D)
		}
	}
	return elem
}

func fixHostNames(doc bson.D, old, new int) bson.D {
	for i, elem := range doc {
		doc[i].Value = fixHostNamesValue(elem.Value, old, new)
	}
	return doc
}

func fixIsMasterDirect(doc bson.D, mongoPort, proxyPort int) (SimpleBSON, error) {
	doc = fixHostNames(doc, mongoPort, proxyPort)
	return SimpleBSONConvert(doc)
}

func (mri *MyResponseInterceptor) InterceptMongoToClient(m Message) (Message, error) {
	switch mm := m.(type) {
	case *ReplyMessage:
		var err error
		var n SimpleBSON
		doc, err := mm.Docs[0].ToBSOND()
		if err != nil {
			return mm, err
		}
		if mri.mode == Cluster {
			n, err = fixIsMasterCluster(doc)
		} else {
			// direct mode
			n, err = fixIsMasterDirect(doc, mri.mongoPort, mri.proxyPort)
		}
		if err != nil {
			return mm, err
		}
		mm.Docs[0] = n
		return mm, nil
	case *MessageMessage:
		var err error
		var n SimpleBSON
		var bodySection *BodySection = nil
		for _, section := range mm.Sections {
			if bs, ok := section.(*BodySection); ok {
				if bodySection != nil {
					return mm, NewStackErrorf("OP_MSG should not have more than one body section!  Second body section: %v", bs)
				}
				bodySection = bs
			} else {
				// MongoDB 3.6 does not support anything other than body sections in replies
				return mm, NewStackErrorf("OP_MSG replies with sections other than a body section are not supported!")
			}
		}

		if bodySection == nil {
			return mm, NewStackErrorf("OP_MSG should have a body section!")
		}
		doc, err := bodySection.Body.ToBSOND()
		if err != nil {
			return mm, err
		}
		if mri.mode == Cluster {
			n, err = fixIsMasterCluster(doc)
		} else {
			// direct mode
			n, err = fixIsMasterDirect(doc, mri.mongoPort, mri.proxyPort)
		}
		bodySection.Body = n
		return mm, nil
	default:
		return m, nil
	}
}

type MyInterceptor struct {
	ps        *ProxySession
	mode      MongoConnectionMode
	mongoPort int
	proxyPort int
}

func (myi *MyInterceptor) Close() {
}
func (myi *MyInterceptor) TrackRequest(MessageHeader) {
}
func (myi *MyInterceptor) TrackResponse(MessageHeader) {
}

func (myi *MyInterceptor) CheckConnection() error {
	return nil
}

func (myi *MyInterceptor) CheckConnectionInterval() time.Duration {
	return 0
}

func (myi *MyInterceptor) InterceptClientToMongo(m Message) (Message, ResponseInterceptor, error) {
	switch mm := m.(type) {
	case *QueryMessage:
		if !NamespaceIsCommand(mm.Namespace) {
			return m, nil, nil
		}

		query, err := mm.Query.ToBSOND()
		if err != nil || len(query) == 0 {
			// let mongod handle error message
			return m, nil, nil
		}

		cmdName := strings.ToLower(query[0].Key)
		if cmdName != "ismaster" {
			return m, nil, nil
		}
		// remove client
		if idx := BSONIndexOf(query, "client"); idx >= 0 {
			query = append(query[:idx], query[idx+1:]...)
		}
		/*
			uncomment to disable streaming isMaster
			if idx := BSONIndexOf(query, "topologyVersion"); idx >= 0 {
				query = append(query[:idx], query[idx+1:]...)
			}
			if idx := BSONIndexOf(query, "maxAwaitTimeMS"); idx >= 0 {
				query = append(query[:idx], query[idx+1:]...)
			}
		*/
		qb, err := SimpleBSONConvert(query)
		if err != nil {
			panic(err)
		}
		mm.Query = qb
		return mm, &MyResponseInterceptor{myi.mode, myi.mongoPort, myi.proxyPort}, nil
		/*
			uncomment to disable streaming isMaster
			case *MessageMessage:
				var err error
				var bodySection *BodySection = nil
				for _, section := range mm.Sections {
					if bs, ok := section.(*BodySection); ok {
						if bodySection != nil {
							return mm, nil, NewStackErrorf("OP_MSG should not have more than one body section!  Second body section: %v", bs)
						}
						bodySection = bs
					}
				}

				if bodySection == nil {
					return mm, nil, NewStackErrorf("OP_MSG should have a body section!")
				}
				doc, err := bodySection.Body.ToBSOND()
				if err != nil {
					return mm, nil, err
				}
				if strings.ToLower(doc[0].Key) != "ismaster" {
					return mm, nil, nil
				}
				if idx := BSONIndexOf(doc, "maxAwaitTimeMS"); idx >= 0 {
					doc = append(doc[:idx], doc[idx+1:]...)
				}
				if idx := BSONIndexOf(doc, "topologyVersion"); idx >= 0 {
					doc = append(doc[:idx], doc[idx+1:]...)
				}
				n, err := SimpleBSONConvert(doc)
				if err != nil {
					panic(err)
				}
				bodySection.Body = n

				return mm, &MyResponseInterceptor{myi.mode, myi.mongoPort, myi.proxyPort}, nil
		*/
	}

	return m, nil, nil
}

func getTestClient(host string, port int, mode MongoConnectionMode, secondaryReads bool, appName string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", host, port)).
		SetDirect(mode == Direct)
	if secondaryReads {
		opts.SetReadPreference(readpref.Secondary())
	}
	if appName != "" {
		opts.SetAppName(appName)
	}
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("cannot create a mongo client. err: %v", err)
	}
	return client, nil
}

func runInsertFindUpdate(host string, proxyPort, iteration int, shouldFail bool, mode MongoConnectionMode, secondaryReads bool) error {
	client, err := getTestClient(host, proxyPort, mode, secondaryReads, fmt.Sprintf("runInsertFindUpdate-iteration%v", iteration))
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 15*time.Second)
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

func enableFailPoint(host string, mongoPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, mongoPort, mode, false, "enableFailPoint")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	// cannot fail "find" because it'll prevent certain driver machinery when operating against replica sets to work properly
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

func disableFailPoint(host string, mongoPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, mongoPort, mode, false, "disableFailPoint")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "off"},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func runOps(host string, proxyPort, parallelism int, shouldFail bool, t *testing.T, mode MongoConnectionMode, secondaryReads bool) int32 {
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
			runtime.Gosched()
			err := runInsertFindUpdate(host, proxyPort, iteration, shouldFail, mode, secondaryReads)
			if err != nil {
				t.Error(err)
				atomic.AddInt32(&failing, 1)
			}
		}(i)
	}
	wg.Wait()
	return failing
}

func getProxyConfig(hostname string, mongoPort, proxyPort, maxPoolSize, maxPoolIdleTimeSec int, mode MongoConnectionMode, enableTracing bool) ProxyConfig {
	var uri string
	if mode == Cluster {
		uri = fmt.Sprintf("mongodb://%s:%v,%s:%v,%s:%v/?replSet=proxytest", hostname, mongoPort, hostname, mongoPort+1, hostname, mongoPort+2)
	}
	pc := NewProxyConfig("localhost", proxyPort, uri, hostname, mongoPort, "", "", "test proxy", enableTracing, mode, ServerSelectionTimeoutSecForTests, maxPoolSize, maxPoolIdleTimeSec)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{mode, mongoPort, proxyPort}
	return pc
}

func getHostAndPorts() (mongoPort, proxyPort int, hostname string) {
	var err error
	mongoPort = 30000
	proxyPort = 9900
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	hostname, err = os.Hostname()
	if err != nil {
		panic(err)
	}
	return
}

func privateSanityTestMongodMode(secondaryMode bool, t *testing.T) {
	mongoPort, proxyPort, _ := getHostAndPorts()
	if err := disableFailPoint("localhost", mongoPort, Direct); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := getProxyConfig("localhost", mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, Direct, true)
	privateSanityTester(t, pc, "localhost", proxyPort, mongoPort, ParallelClients, Direct, secondaryMode)
}

func TestProxySanityMongodModePrimary(t *testing.T) {
	privateSanityTestMongodMode(false, t)
}

// this is expected to go through the same code as primary mode since Mongod mode essentially ignores read preference from client
func TestProxySanityMongodModeSecondary(t *testing.T) {
	privateSanityTestMongodMode(true, t)
}

func privateSanityTestMongosMode(secondaryMode bool, t *testing.T) {
	mongoPort, proxyPort, hostname := getHostAndPorts()
	if err := disableFailPoint(hostname, mongoPort, Cluster); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, Cluster, true)
	privateSanityTester(t, pc, hostname, proxyPort, mongoPort, 5, Cluster, secondaryMode)
}

func TestProxySanityMongosModePrimary(t *testing.T) {
	privateSanityTestMongosMode(false, t)
}

func TestProxySanityMongosModeSecondary(t *testing.T) {
	privateSanityTestMongosMode(true, t)
}

// backing mongo must be started with --setParameter enableTestCommands=1
func privateSanityTester(t *testing.T, pc ProxyConfig, host string, proxyPort, mongoPort, parallelism int, mode MongoConnectionMode, secondaryReads bool) {
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

	failing := runOps(host, proxyPort, parallelism*2, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}

	conns := proxy.GetConnectionsCreated()
	if conns == 0 {
		t.Fatalf("expected connections created to increase but were still %v", conns)
	}
	currConns = conns

	t.Log("*** run ops again to confirm connections are reused")
	failing = runOps(host, proxyPort, parallelism, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}
	conns = proxy.GetConnectionsCreated()
	if conns != currConns {
		t.Fatalf("expected connections created to remain the same (%v), but got %v", currConns, conns)
	}
	currConns = conns

	t.Log("*** enable failpoint and run ops")
	enableFailPoint(host, mongoPort, mode)
	failing = runOps(host, proxyPort, parallelism, true, t, mode, secondaryReads)

	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}

	conns = proxy.GetConnectionsCreated()
	if conns == currConns {
		t.Fatalf("expected connections created to increase from (%v), but got %v", currConns, conns)
	}
	currConns = conns

	t.Log("*** disable failpoint and run ops")
	if err := disableFailPoint(host, mongoPort, mode); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}

	failing = runOps(host, proxyPort, parallelism, false, t, mode, secondaryReads)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("ops failures")
		return
	}

	conns = proxy.GetConnectionsCreated()
	if conns == currConns {
		t.Fatalf("expected connections created to increase from (%v), but got %v", currConns, conns)
	}
}

func insertDummyDocs(host string, proxyPort, numOfDocs int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "insertDummyDocs")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	dbName, collName := "test2", "foo"

	defer client.Disconnect(ctx)

	coll := client.Database(dbName).Collection(collName)
	// insert some docs
	docs := make([]interface{}, numOfDocs)
	for i := 0; i < numOfDocs; i++ {
		docs[i] = bson.D{{"x", i}}
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("initial insert failed. err: %v", err)
	}
	return nil
}

func cleanup(host string, proxyPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "cleanup")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	dbName, collName := "test2", "foo"

	defer client.Disconnect(ctx)

	coll := client.Database(dbName).Collection(collName)
	return coll.Drop(ctx)
}

func runFind(logger *slogger.Logger, host string, proxyPort, workerNum int, mode MongoConnectionMode) (time.Duration, bool, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"

	client, err := getTestClient(host, proxyPort, mode, false, fmt.Sprintf("worker-%v", workerNum))
	if err != nil {
		return 0, false, fmt.Errorf("failed to get a test client. err=%v", err)
	}
	goctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
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
	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func privateConnectionPerformanceTesterFindOne(mode MongoConnectionMode, maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T) {
	Iterations := 50
	mongoPort, proxyPort, hostname := getHostAndPorts()
	hostToUse := hostname
	if mode == Direct {
		hostToUse = "localhost"
	}
	serverPort := mongoPort
	preSetupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return disableFailPoint(hostname, mongoPort, mode)
	}
	setupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return insertDummyDocs(hostname, serverPort, 1000, mode)
	}

	testFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int, mode MongoConnectionMode) (elapsed time.Duration, success bool, err error) {
		return runFind(logger, hostname, serverPort, iteration, mode)
	}

	cleanupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return cleanup(hostname, serverPort, mode)
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort, maxPoolSize, DefaultMaxPoolIdleTimeSec, mode, true)
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

	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(proxy.NewLogger("tester"),
		hostToUse, mongoPort, proxyPort, mode,
		Iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	if failedCount > 0 {
		t.Errorf("failed workers %v", failedCount)
	}
	if avgLatencyMs > targetAvgLatencyMs {
		t.Errorf("average latency %v > %v", avgLatencyMs, targetAvgLatencyMs)
	}
	if maxLatencyMs > targetMaxLatencyMs {
		t.Errorf("max latency %v > %v", maxLatencyMs, targetMaxLatencyMs)
	}
	if len(results) == 0 {
		t.Errorf("no successful runs!")
	}
	t.Logf("ALL DONE workers=%v, successful runs=%v, avg=%vms, max=%vms, failures=%v, percentiles=%v\nresults=%v", workers, len(results), avgLatencyMs, maxLatencyMs, failedCount, percentiles, results)
}

func TestProxyConnectionPerformanceFindOneMongodMode(t *testing.T) {
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 5, 50, 100, t)
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 20, 250, 1500, t)
	// privateConnectionPerformanceTesterFindOne(Direct, 0, 50, 1500, 2000, t)
	privateConnectionPerformanceTesterFindOne(Direct, 0, 75, 1500, 3000, t)

}

func TestProxyConnectionPerformanceFindOneMongosMode(t *testing.T) {
	//privateConnectionPerformanceTesterFindOne(Cluster, 0, 5, 50, 100, t)
	privateConnectionPerformanceTesterFindOne(Cluster, 0, 75, 50, 100, t)
}
