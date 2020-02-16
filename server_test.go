package mongonet_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erh/mongonet"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MyServerSession struct {
	session *mongonet.Session
	mydata  map[string][]bson.D
}

func (mss *MyServerSession) handleIsMaster(mm mongonet.Message) error {
	return mss.session.RespondToCommandMakeBSON(mm,
		"ismaster", true,
		"maxBsonObjectSize", 16777216,
		"maxMessageSizeBytes", 48000000,
		"maxWriteBatchSize", 100000,
		//"localTime", ISODate("2017-12-14T17:40:28.640Z"),
		"logicalSessionTimeoutMinutes", 30,
		"minWireVersion", 0,
		"maxWireVersion", 6,
		"readOnly", false,
	)
}

func (mss *MyServerSession) handleInsert(mm mongonet.Message, cmd bson.D, ns string) error {
	mss.mydata[ns] = []bson.D{}
	return mss.session.RespondToCommandMakeBSON(mm)
}

func (mss *MyServerSession) handleEndSessions(mm mongonet.Message) error {
	return mss.session.RespondToCommandMakeBSON(mm)
}

/*
* @return (error, <fatal>)
 */
func (mss *MyServerSession) handleMessage(m mongonet.Message) (error, bool) {

	switch mm := m.(type) {
	case *mongonet.QueryMessage:

		if !mongonet.NamespaceIsCommand(mm.Namespace) {
			return fmt.Errorf("can only use old query style to bootstrap, not a valid namesapce (%s)", mm.Namespace), false
		}

		cmd, err := mm.Query.ToBSOND()
		if err != nil {
			return fmt.Errorf("old thing not valid, barfing %s", err), true
		}

		if len(cmd) == 0 {
			return fmt.Errorf("old thing not valid, barfing"), true
		}

		db := mongonet.NamespaceToDB(mm.Namespace)
		cmdName := cmd[0].Key

		if cmdName == "getnonce" {
			return mss.session.RespondToCommandMakeBSON(mm, "nonce", "914653afbdbdb833"), false
		}

		if cmdName == "ismaster" || cmdName == "isMaster" {
			return mss.handleIsMaster(mm), false
		}

		if cmdName == "ping" {
			return mss.session.RespondToCommandMakeBSON(mm), false
		}

		if cmdName == "insert" {
			ns := fmt.Sprintf("%s.%s", db, cmd[0].Value.(string)) // TODO: check type cast?
			docs := mongonet.BSONIndexOf(cmd, "documents")
			if docs < 0 {
				return fmt.Errorf("no documents to insert :("), false
			}

			old, found := mss.mydata[ns]
			if !found {
				old = []bson.D{}
			}

			toInsert, _, err := mongonet.GetAsBSONDocs(cmd[docs])
			if err != nil {
				return fmt.Errorf("documents not a good array: %s", err), false
			}

			for _, d := range toInsert {
				old = append(old, d)
			}

			mss.mydata[ns] = old
			return mss.session.RespondToCommandMakeBSON(mm), false
		}

		if cmdName == "find" {
			ns := fmt.Sprintf("%s.%s", db, cmd[0].Value.(string)) // TODO: check type cast?

			data, found := mss.mydata[ns]
			if !found {
				data = []bson.D{}
			}
			return mss.session.RespondToCommandMakeBSON(mm,
				"cursor", bson.D{{"firstBatch", data}, {"id", 0}, {"ns", ns}},
			), false

		}

		fmt.Printf("hi1 %s %s %s\n", mm.Namespace, cmdName, cmd)
		return fmt.Errorf("command (%s) not done", cmdName), true

	case *mongonet.CommandMessage:
		fmt.Printf("hi2 %#v\n", mm)
	case *mongonet.MessageMessage:
		var bodySection *mongonet.BodySection = nil

		for _, section := range mm.Sections {
			if bodySec, ok := section.(*mongonet.BodySection); ok {
				if bodySection != nil {
					return fmt.Errorf("OP_MSG contains more than one body section"), true
				}

				bodySection = bodySec
			}
		}

		if bodySection == nil {
			return fmt.Errorf("OP_MSG does not contain a body section"), true
		}

		cmd, err := bodySection.Body.ToBSOND()
		if err != nil {
			return mongonet.NewStackErrorf("can't parse body section bson: %v", err), true
		}

		if len(cmd) == 0 {
			return mongonet.NewStackErrorf("can't parse body section bson. length=0"), true
		}
		idx := mongonet.BSONIndexOf(cmd, "$db")
		if idx < 0 {
			return fmt.Errorf("can't find the db"), true
		}
		db, _, err := mongonet.GetAsString(cmd[idx])
		if err != nil {
			return mongonet.NewStackErrorf("can't parse the db from bson: %v", err), true
		}
		ns := fmt.Sprintf("%s.%s", db, cmd[0].Value)
		cmdName := cmd[0].Key
		if cmdName == "ismaster" || cmdName == "isMaster" {
			return mss.handleIsMaster(mm), false
		}
		if cmdName == "insert" {
			return mss.handleInsert(mm, cmd, ns), false
		}
		if cmdName == "endSessions" {
			return mss.handleEndSessions(mm), false
		}
		return nil, false
	}

	return fmt.Errorf("what are you! %t", m), true
}

func (mss *MyServerSession) DoLoopTemp() {
	for {
		m, err := mss.session.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return
			}
			mss.session.Logf(slogger.WARN, "error reading message: %s", err)
			return
		}

		err, fatal := mss.handleMessage(m)
		if err == nil && fatal {
			panic(fmt.Errorf("should be impossible, no error but fatal"))
		}
		if err != nil {
			err = mss.session.RespondWithError(m, err)
			if err != nil {
				mss.session.Logf(slogger.WARN, "error writing error: %s", err)
				return
			}
			if fatal {
				return
			}

		}
	}
}

func (mss *MyServerSession) Close() {
}

type MyServerTestFactory struct {
}

func (sf *MyServerTestFactory) CreateWorker(session *mongonet.Session) (mongonet.ServerWorker, error) {
	return &MyServerSession{session, map[string][]bson.D{}}, nil
}

func (sf *MyServerTestFactory) GetConnection(conn net.Conn) io.ReadWriteCloser {
	return conn
}

func TestServer(t *testing.T) {
	port := 9919 // TODO: pick randomly or check?
	syncTlsConfig := mongonet.NewSyncTlsConfig()
	server := mongonet.NewServer(
		mongonet.ServerConfig{
			"127.0.0.1",
			port,
			false,
			nil,
			syncTlsConfig,
			0,
			0,
			nil,
			slogger.DEBUG,
			nil,
		},
		&MyServerTestFactory{},
	)

	go server.Run()

	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d", port))
	client, err := mongo.NewClient(opts)
	if err != nil {
		t.Errorf("cannot create a mongo client. err: %v", err)
	}

	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		t.Errorf("cannot connect to server. err: %v", err)
		return
	}
	defer client.Disconnect(ctx)
	coll := client.Database("test").Collection("bar")
	docIn := bson.D{{"foo", 17}}
	res, err := coll.InsertOne(ctx, docIn)
	if err != nil {
		t.Errorf("can't insert: %v", err)
		return
	}
	fmt.Println(res)
	return

	docOut := bson.D{}
	fres := coll.FindOne(ctx, bson.D{})
	fmt.Println(fres)
	if fres.Err() != nil {
		t.Errorf("can't find: %v", fres.Err())
		return
	}
	

	if len(docIn) != len(docOut) {
		t.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
		return
	}

	if docIn[0] != docOut[0] {
		t.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
		return
	}

}

// ---------------------------------------------------------------------------------------------------------------
// Testing for server with contextualWorkerFactory

type TestFactoryWithContext struct {
	counter *int32
}

func (sf *TestFactoryWithContext) CreateWorkerWithContext(session *mongonet.Session, ctx *context.Context) (mongonet.ServerWorker, error) {
	return &TestSessionWithContext{session, ctx, sf.counter}, nil
}

func (sf *TestFactoryWithContext) CreateWorker(session *mongonet.Session) (mongonet.ServerWorker, error) {
	return nil, fmt.Errorf("create worker not allowed with contextual worker factory")
}

func (sf *TestFactoryWithContext) GetConnection(conn net.Conn) io.ReadWriteCloser {
	return conn
}

type TestSessionWithContext struct {
	session *mongonet.Session
	ctx     *context.Context
	counter *int32
}

func (tsc *TestSessionWithContext) handleMessage(m mongonet.Message) (error, bool) {
	switch mm := m.(type) {
	case *mongonet.QueryMessage:
		cmd, err := mm.Query.ToBSOND()
		if err != nil {
			return fmt.Errorf("error converting query to bsond: %v", err), true
		}

		if len(cmd) == 0 {
			return fmt.Errorf("invalid command length"), true
		}

		cmdName := cmd[0].Key

		if cmdName == "ismaster" || cmdName == "isMaster" {
			return tsc.session.RespondToCommandMakeBSON(mm,
				"ismaster", true,
				"maxBsonObjectSize", 16777216,
				"maxMessageSizeBytes", 48000000,
				"maxWriteBatchSize", 100000,
				//"localTime", ISODate("2017-12-14T17:40:28.640Z"),
				"logicalSessionTimeoutMinutes", 30,
				"minWireVersion", 0,
				"maxWireVersion", 6,
				"readOnly", false,
			), false
		}

		if cmdName == "getnonce" {
			return tsc.session.RespondToCommandMakeBSON(mm, "nonce", "6d32d13b13436425"), false
		}

		if cmdName == "ping" {
			return tsc.session.RespondToCommandMakeBSON(mm), false
		}

		if cmdName == "endSessions" {
			return tsc.session.RespondToCommandMakeBSON(mm), false
		}

		return fmt.Errorf("command (%s) not done", cmdName), true
	}

	return fmt.Errorf("error handling message"), true

}

func (tsc *TestSessionWithContext) DoLoopTemp() {
	atomic.AddInt32(tsc.counter, 1)
	ctx := *tsc.ctx

	for {
		m, err := tsc.session.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return
			}
			tsc.session.Logf(slogger.WARN, "error reading message: %s", err)
			return
		}

		err, fatal := tsc.handleMessage(m)
		if err == nil && fatal {
			panic(fmt.Errorf("should be impossible, no error but fatal"))
		}
		if err != nil {
			err = tsc.session.RespondWithError(m, err)
			if err != nil {
				tsc.session.Logf(slogger.WARN, "error writing error: %s", err)
				return
			}
			if fatal {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}
func (tsc *TestSessionWithContext) Close() {
	time.Sleep(5000 * time.Millisecond)
	atomic.AddInt32(tsc.counter, -1)
}

func TestServerWorkerWithContext(t *testing.T) {
	port := 27027

	var sessCtr int32
	syncTlsConfig := mongonet.NewSyncTlsConfig()
	server := mongonet.NewServer(
		mongonet.ServerConfig{
			"127.0.0.1",
			port,
			false,
			nil,
			syncTlsConfig,
			0,
			0,
			nil,
			slogger.DEBUG,
			nil,
		},
		&TestFactoryWithContext{&sessCtr},
	)

	go server.Run()

	if err := <-server.InitChannel(); err != nil {
		t.Error(err)
	}

	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d", port))
	for i := 0; i < 10; i++ {
		client, err := mongo.NewClient(opts)
		if err != nil {
			t.Errorf("cannot create a mongo client. err: %v", err)
			return
		}

		ctx := context.Background()
		if err := client.Connect(ctx); err != nil {
			t.Errorf("cannot connect to server. err: %v", err)
			return
		}
		client.Disconnect(ctx)
	}

	sessCtrCurr := atomic.LoadInt32(&sessCtr)

	if sessCtrCurr != int32(10) {
		t.Errorf("expect session counter to be 10 but got %d", sessCtrCurr)
	}

	server.Close()

	sessCtrFinal := atomic.LoadInt32(&sessCtr)
	if sessCtrFinal != int32(0) {
		t.Errorf("expect session counter to be 0 but got %d", sessCtrFinal)
	}
}
