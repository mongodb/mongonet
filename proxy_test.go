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

func GetConnCreated(p *Proxy) int64 {
	return p.GetConnectionsCreated()
}

func doFind(proxyPort, iteration int, shouldFail bool) error {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://localhost:%d", proxyPort))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return fmt.Errorf("cannot create a mongo client. err: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
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

func enableFailPoint(mongoPort int) error {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://localhost:%d", mongoPort))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return fmt.Errorf("cannot create a mongo client. err: %v", err)
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
		// {"mode", bson.D{{"times", 5}}},
		{"data", bson.D{
			{"failCommands", []string{"find"}},
			{"closeConnection", true},
		}},
	}
	res := client.Database("admin").RunCommand(ctx, cmd)
	fmt.Println(res)
	return nil
}

func disableFailPoint(mongoPort int) error {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://localhost:%d", mongoPort))
	client, err := mongo.NewClient(opts)
	if err != nil {
		return fmt.Errorf("cannot create a mongo client. err: %v", err)
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
	res := client.Database("admin").RunCommand(ctx, cmd)
	fmt.Println(res)
	return nil
}

// backing mongo must be started with --setParameter enableTestCommands=1
func TestProxySanity(t *testing.T) {
	mongoPort := 30000
	proxyPort := 9900
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	pc := NewProxyConfig("localhost", proxyPort, "localhost", mongoPort, "", "", "test proxy", true)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{}

	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()
	fmt.Println("initial conns created", GetConnCreated(&proxy))
	var wg sync.WaitGroup
	var failing int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			err := doFind(proxyPort, i, false)
			wg.Done()
			if err != nil {
				atomic.AddInt32(&failing, 1)
			}
		}()
	}

	wg.Wait()
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("initial finds failures")
		return
	}

	fmt.Println("#1 conns created", GetConnCreated(&proxy))
	// enable test commands - fail connections a bunch of times
	enableFailPoint(mongoPort)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			err := doFind(proxyPort, i, true)
			wg.Done()
			if err != nil {
				atomic.AddInt32(&failing, 1)
			}
		}()
	}

	wg.Wait()
	if atomic.LoadInt32(&failing) > 0 {
		t.Fatalf("initial finds failures")
		return
	}

	fmt.Println("#2 conns created", GetConnCreated(&proxy))
	// disable test command - verify connections work again
	if err := disableFailPoint(mongoPort); err != nil {
		t.Fatalf("failed to disable failpoint. err=%v", err)
		return
	}
	fmt.Println("#3 conns created", GetConnCreated(&proxy))
}
