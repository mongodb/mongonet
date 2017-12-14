package mongonet_test

import "fmt"
import "io"
import "testing"

import "github.com/mongodb/slogger/v2/slogger"

import "gopkg.in/mgo.v2"
import "gopkg.in/mgo.v2/bson"

import "github.com/10gen/atlasproxy"

import "github.com/erh/mongonet"

type MyServerSession struct {
	session *mongonet.Session
	nonce string
	mydata map[string][]bson.D
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
		cmdName := cmd[0].Name
		
		if cmdName == "getnonce" {
			n, err := atlasproxy.NewNonce()
			if err != nil {
				return err, false
			}
			mss.nonce = n.String()
			return mss.session.RespondToCommandMakeBSON(mm, "nonce", mss.nonce), false
		}

		if cmdName == "ismaster" {
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
			), false
		}

		if cmdName == "ping" {
			return mss.session.RespondToCommandMakeBSON(mm), false
		}

		if cmdName == "insert" {
			ns := fmt.Sprintf( "%s.%s", db, cmd[0].Value.(string) ) // TODO: check type cast?
			docs := mongonet.BSONIndexOf(cmd, "documents")
			if docs < 0 {
				return fmt.Errorf("no documents to insert :("), false
			}

			old, found := mss.mydata[ns]
			if !found {
				old = []bson.D{}
			}

			toInsert, err := mongonet.GetAsBSONDocs(cmd[docs])
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
			ns := fmt.Sprintf( "%s.%s", db, cmd[0].Value.(string) ) // TODO: check type cast?

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
	return &MyServerSession{session, "", map[string][]bson.D{}}, nil
}

func TestServer(t *testing.T) {
	port := 9919 // TODO: pick randomly or check?
	
	server := mongonet.NewServer(
		mongonet.ServerConfig{
			"127.0.0.1",
			port,
			false,
			nil,
			0,
			slogger.DEBUG,
			nil,
		},
		&MyServerTestFactory{},
	)

	go server.Run()

	session, err := mgo.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Errorf("cannot dial: %s", err)
		return
	}
	defer session.Close()

	coll := session.DB("test").C("bar")
	docIn := bson.D{{"foo", 17}} 
	err = coll.Insert( docIn )
	if err != nil {
		t.Errorf("can't insert: %s", err)
		return
	}

	docOut := bson.D{}
	err = coll.Find(bson.D{}).One(&docOut)
	if err != nil {
		t.Errorf("can't find: %s", err)
		return
	}

	if len(docIn) != len(docOut) {
		t.Errorf("docs don't match\n %s\n %s\n", docIn, docOut)
		return
	}
	
	if docIn[0] != docOut[0] {
		t.Errorf("docs don't match\n %s\n %s\n", docIn, docOut)
		return
	}

}
