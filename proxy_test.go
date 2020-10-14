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

	"github.com/go-test/deep"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MyFactory struct {
}

func (myf *MyFactory) NewInterceptor(ps *ProxySession) (ProxyInterceptor, error) {
	return &MyInterceptor{ps}, nil
}

type MyInterceptor struct {
	ps *ProxySession
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

		return mm, nil, nil
	}

	return m, nil, nil
}

func getTestClient(host string, port int) (*mongo.Client, error) {
	fmt.Println("*** new client to ", host, port)
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", host, port))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("cannot create a mongo client. err: %v", err)
	}
	return client, nil
}

func doFind(host string, proxyPort, iteration int, shouldFail bool) error {
	client, err := getTestClient(host, proxyPort)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	coll := client.Database("test").Collection(fmt.Sprintf("bar_%v", iteration))

	fmt.Println("dropping collection")
	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("failed to drop collection: %v", err)
	}

	fmt.Println("inserting doc")
	docIn := bson.D{{"foo", int32(17)}}
	if _, err := coll.InsertOne(ctx, docIn); err != nil {
		return fmt.Errorf("can't insert: %v", err)
	}
	docOut := bson.D{}
	fopts := options.FindOne().SetProjection(bson.M{"_id": 0})
	err = coll.FindOne(ctx, bson.D{}, fopts).Decode(&docOut)
	if err != nil {
		if shouldFail {
			return nil
		}
		return fmt.Errorf("can't find: %v", err)
	}
	if shouldFail {
		return fmt.Errorf("expected find to fail but it didn't")
	}
	if len(docIn) != len(docOut) {
		return fmt.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
	}
	if diff := deep.Equal(docIn[0], docOut[0]); diff != nil {
		return fmt.Errorf("docs don't match: %v", diff)
	}
	return nil
}

func enableFailPoint(host string, mongoPort int) error {
	client, err := getTestClient(host, mongoPort)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", []string{"find"}},
			{"closeConnection", true},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func disableFailPoint(host string, mongoPort int) error {
	client, err := getTestClient(host, mongoPort)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
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

func runFinds(host string, proxyPort, parallelism int, shouldFail bool, t *testing.T) int32 {
	var wg sync.WaitGroup
	var failing int32
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			err := doFind(host, proxyPort, iteration, shouldFail)
			if err != nil {
				t.Log(err)
				atomic.AddInt32(&failing, 1)
			}
		}(i)
	}
	wg.Wait()
	return failing
}

func TestProxySanityMongodMode(t *testing.T) {
	mongoPort := 30000
	proxyPort := 9900
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	if err := disableFailPoint("localhost", mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := NewProxyConfig("localhost", proxyPort, "", "localhost", mongoPort, "", "", "test proxy", true, Direct, 5)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{}
	connExpected := []int64{5, 5, 10, 15}
	privateTester(t, pc, "localhost", proxyPort, mongoPort, 5, connExpected)
}

func TestProxySanityMongosMode(t *testing.T) {
	mongoPort := 30000
	proxyPort := 9900
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	if err := disableFailPoint(hostname, mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	uri := fmt.Sprintf("mongodb://%s:%v,%s:%v,%s:%v/?replSet=proxytest", hostname, mongoPort, hostname, mongoPort+1, hostname, mongoPort+2)
	pc := NewProxyConfig("localhost", proxyPort, uri, hostname, mongoPort, "", "", "test proxy", true, Cluster, 5)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{}
	//connExpected := []int64{5, 5, 10, 15}
	connExpected := []int64{1, 1, 2, 3}
	privateTester(t, pc, hostname, proxyPort, mongoPort, 1, connExpected)
}

// backing mongo must be started with --setParameter enableTestCommands=1
func privateTester(t *testing.T, pc ProxyConfig, host string, proxyPort, mongoPort, parallelism int, connExpected []int64) {
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()
	if conns := proxy.GetConnectionsCreated(); conns != 0 {
		t.Fatalf("expected connections created to equal 0 but was %v", conns)
	}
	failing := runFinds(host, proxyPort, parallelism, false, t)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("finds failures")
		return
	}

	t.Log("*** after 1")
	if conns := proxy.GetConnectionsCreated(); conns != connExpected[0] {
		t.Fatalf("expected connections created to equal 5 but was %v", conns)
	}

	// run finds again to confirm connections are reused
	failing = runFinds(host, proxyPort, parallelism, false, t)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("finds failures")
		return
	}
	if conns := proxy.GetConnectionsCreated(); conns != connExpected[1] {
		t.Fatalf("expected connections created to equal 5 but was %v", conns)
	}
	t.Log("*** after 2")

	// enable fail point - fail connections a bunch of times
	enableFailPoint(host, mongoPort)
	failing = runFinds(host, proxyPort, parallelism, true, t)

	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("finds failures")
		return
	}

	if conns := proxy.GetConnectionsCreated(); conns != connExpected[2] {
		t.Fatalf("expected connections created to equal 10 but was %v", conns)
	}
	t.Log("*** after 3")

	// disable fail point - verify connections work again
	if err := disableFailPoint(host, mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}

	failing = runFinds(host, proxyPort, parallelism, false, t)
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("finds failures")
		return
	}

	if conns := proxy.GetConnectionsCreated(); conns != connExpected[3] {
		t.Fatalf("expected connections created to equal 15 but was %v", conns)
	}
	t.Log("*** after 4")
}
