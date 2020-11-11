package mongonet

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MyFactory struct {
	mongoPort int
	proxyPort int
}

func (myf *MyFactory) NewInterceptor(ps *ProxySession) (ProxyInterceptor, error) {
	return &MyInterceptor{ps, myf.mongoPort, myf.proxyPort}, nil
}

type MyResponseInterceptor struct {
	mongoPort int
	proxyPort int
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
		n, err = fixIsMasterDirect(doc, mri.mongoPort, mri.proxyPort)
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
		n, err = fixIsMasterDirect(doc, mri.mongoPort, mri.proxyPort)
		bodySection.Body = n
		return mm, nil
	default:
		return m, nil
	}
}

type MyInterceptor struct {
	ps        *ProxySession
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
		qb, err := SimpleBSONConvert(query)
		if err != nil {
			panic(err)
		}
		mm.Query = qb
		return mm, &MyResponseInterceptor{myi.mongoPort, myi.proxyPort}, nil
	}

	return m, nil, nil
}

func getTestClient(host string, port int, appName string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", host, port)).
		SetDirect(true)

	if appName != "" {
		opts.SetAppName(appName)
	}
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("cannot create a mongo client. err: %v", err)
	}
	return client, nil
}

func getProxyConfig(hostname string, mongoPort, proxyPort int) ProxyConfig {

	pc := NewProxyConfig("localhost", proxyPort, hostname, mongoPort)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{mongoPort, proxyPort}
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

func insertDummyDocs(host string, proxyPort, numOfDocs int) error {
	client, err := getTestClient(host, proxyPort, "insertDummyDocs")
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
	goctx := context.Background()

	defer client.Disconnect(goctx)

	coll := client.Database(dbName).Collection(collName)
	// insert some docs
	docs := make([]interface{}, numOfDocs)
	for i := 0; i < numOfDocs; i++ {
		docs[i] = bson.D{{"x", i}}
	}
	if _, err := coll.InsertMany(goctx, docs); err != nil {
		return fmt.Errorf("initial insert failed. err: %v", err)
	}
	return nil
}

func cleanup(host string, proxyPort int) error {
	client, err := getTestClient(host, proxyPort, "cleanup")
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
	goctx := context.Background()

	defer client.Disconnect(goctx)

	coll := client.Database(dbName).Collection(collName)
	return coll.Drop(goctx)
}

func runFind(host string, proxyPort, workerNum int, t *testing.T) (time.Duration, error) {
	start := time.Now()
	dbName, collName := "test2", "foo"

	client, err := getTestClient(host, proxyPort, fmt.Sprintf("worker-%v", workerNum))
	if err != nil {
		return 0, fmt.Errorf("failed to get a test client. err=%v", err)
	}
	goctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	if err := client.Connect(goctx); err != nil {
		return 0, fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(goctx)
	doc := bson.D{}
	coll := client.Database(dbName).Collection(collName)

	cur, err := coll.Find(goctx, bson.D{{"x", 1}})
	if err != nil {
		return 0, fmt.Errorf("failed to run find. err=%v", err)
	}
	cur.Next(goctx)
	if err := cur.Decode(&doc); err != nil {
		return 0, fmt.Errorf("failed to decode find. err=%v", err)
	}
	elapsed := time.Since(start)
	debugLog(t, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, nil
}

func debugLog(t *testing.T, pattern string, args ...interface{}) {
	prefix := time.Now().Format("2006-01-02 15:04:05.00000")
	t.Logf(fmt.Sprintf("%s %s", prefix, pattern), args...)
}

func privateConnectionPerformanceTester(workers, targetDuration int, t *testing.T) {
	resLock := sync.RWMutex{}
	var results []int64
	Iterations := 10
	mongoPort, proxyPort, _ := getHostAndPorts()

	hostToUse := "localhost"

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort)
	pc.LogLevel = slogger.DEBUG
	proxy := NewProxy(pc)
	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	// insert dummy docs
	if err := insertDummyDocs(hostToUse, proxyPort, 1000); err != nil {
		t.Fatal(err)
	}
	defer cleanup(hostToUse, proxyPort)

	var wg sync.WaitGroup
	totalAvg := int64(0)
	for j := 0; j < Iterations; j++ {
		var errCount int32
		var successCount int32
		var sum int64
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(it int, wg *sync.WaitGroup) {
				// runtime.Gosched()
				defer wg.Done()
				debugLog(t, "running worker-%v", it)
				elapsed, err := runFind("localhost", proxyPort, it, t)
				debugLog(t, "worker-%v elapsed %v err=%v", it, elapsed, err)
				if err != nil {
					atomic.AddInt32(&errCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
					atomic.AddInt64(&sum, elapsed.Milliseconds())
					resLock.Lock()
					results = append(results, elapsed.Milliseconds())
					resLock.Unlock()
				}
			}(i, &wg)
		}
		wg.Wait()
		errVal := atomic.LoadInt32(&errCount)
		successVal := atomic.LoadInt32(&successCount)
		totalSum := atomic.LoadInt64(&sum)
		if errVal > 0 {
			t.Errorf("workers failed count %v", errVal)
		}
		avg := float64(totalSum) / float64(successVal)
		totalAvg += int64(avg)
		t.Logf("DONE worker error count=%v, success count=%v, avg=%vms", errVal, successVal, avg)
		if int(avg) > targetDuration {
			t.Errorf("duration too high %v", avg)
		}
	}
	resLock.RLock()

	max := int64(0)
	for _, val := range results {
		if val > max {
			max = val
		}
	}
	resLock.RUnlock()
	t.Logf("ALL DONE workers=%v, avg=%vms, max=%vms", workers, float64(totalAvg)/float64(Iterations), max)
}

func TestProxyConnectionPerformanceMongodMode(t *testing.T) {
	privateConnectionPerformanceTester(100, 1000, t)
	/*
		successful:
		privateConnectionPerformanceTester(Direct, 200, 100, 1000, t)
		privateConnectionPerformanceTester(Direct, 10, 5, 100, t)
		privateConnectionPerformanceTester(Direct, 20, 10, 100, t)
		privateConnectionPerformanceTester(Direct, 0, 100, 1000, t) // unlimited pool size
	*/

	/*
		failing:
		privateConnectionPerformanceTester(Direct, 20, 20, 100, t)
		privateConnectionPerformanceTester(Direct, 5, 5, 100, t)
		privateConnectionPerformanceTester(Direct, 5, 4, 100, t) - yields high response times
	*/

}
