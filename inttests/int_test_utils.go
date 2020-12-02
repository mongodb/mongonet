package inttests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	ServerSelectionTimeoutSecForTests = 10
)

func insertDummyDocs(client *mongo.Client, numOfDocs int, ctx context.Context) error {
	dbName, collName := "test2", "foo"

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

func cleanup(client *mongo.Client, ctx context.Context) error {
	dbName, collName := "test2", "foo"

	coll := client.Database(dbName).Collection(collName)
	return coll.Drop(ctx)
}

type MyFactory struct {
	mode            util.MongoConnectionMode
	mongoPort       int
	proxyPort       int
	disableIsMaster bool
}

func (myf *MyFactory) NewInterceptor(ps *ProxySession) (ProxyInterceptor, error) {
	return &MyInterceptor{ps, myf.mode, myf.mongoPort, myf.proxyPort, myf.disableIsMaster}, nil
}

type SimulateRetryFixer struct {
	OriginalMessage Message
}

func (srf *SimulateRetryFixer) InterceptMongoToClient(m Message) (Message, error) {
	switch mm := m.(type) {
	case *MessageMessage:
		var err error
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
		val := BSONGetValueByNestedPathForTests(doc, "cursor.firstBatch.val", 0)
		if v, ok := val.(int32); ok {
			fmt.Println("SimulateRetryFixer::got ", v)
			if v == util.RetryOnRemoteVal {
				return mm, NewProxyRetryError(srf.OriginalMessage, util.RemoteRsName)
			}
		}
		return mm, nil
	default:
		return m, nil
	}
}

type IsMasterFixer struct {
	mode      util.MongoConnectionMode
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

func (mri *IsMasterFixer) InterceptMongoToClient(m Message) (Message, error) {
	switch mm := m.(type) {
	case *ReplyMessage:
		var err error
		var n SimpleBSON
		doc, err := mm.Docs[0].ToBSOND()
		if err != nil {
			return mm, err
		}
		if mri.mode == util.Cluster {
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
		if mri.mode == util.Cluster {
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
	ps                       *ProxySession
	mode                     util.MongoConnectionMode
	mongoPort                int
	proxyPort                int
	disableStreamingIsMaster bool
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

func (myi *MyInterceptor) InterceptClientToMongo(m Message) (Message, ResponseInterceptor, string, error) {
	switch mm := m.(type) {
	case *QueryMessage:
		if !NamespaceIsCommand(mm.Namespace) {
			return m, nil, "", nil
		}

		query, err := mm.Query.ToBSOND()
		if err != nil || len(query) == 0 {
			// let mongod handle error message
			return m, nil, "", nil
		}

		cmdName := strings.ToLower(query[0].Key)
		if cmdName != "ismaster" {
			return m, nil, "", nil
		}
		// remove client
		if idx := BSONIndexOf(query, "client"); idx >= 0 {
			query = append(query[:idx], query[idx+1:]...)
		}
		if myi.disableStreamingIsMaster {
			if idx := BSONIndexOf(query, "topologyVersion"); idx >= 0 {
				query = append(query[:idx], query[idx+1:]...)
			}
			if idx := BSONIndexOf(query, "maxAwaitTimeMS"); idx >= 0 {
				query = append(query[:idx], query[idx+1:]...)
			}
		}
		qb, err := SimpleBSONConvert(query)
		if err != nil {
			panic(err)
		}
		mm.Query = qb
		return mm, &IsMasterFixer{myi.mode, myi.mongoPort, myi.proxyPort}, "", nil
	case *MessageMessage:
		var err error
		var bodySection *BodySection = nil
		for _, section := range mm.Sections {
			if bs, ok := section.(*BodySection); ok {
				if bodySection != nil {
					return mm, nil, "", NewStackErrorf("OP_MSG should not have more than one body section!  Second body section: %v", bs)
				}
				bodySection = bs
			}
		}

		if bodySection == nil {
			return mm, nil, "", NewStackErrorf("OP_MSG should have a body section!")
		}
		doc, err := bodySection.Body.ToBSOND()
		if err != nil {
			return mm, nil, "", err
		}
		var rsName string
		var db string
		if idx := BSONIndexOf(doc, "$db"); idx >= 0 {
			db, _, err = GetAsString(doc[idx])
			if err != nil {
				panic(err)
			}
			if db == util.RemoteDbNameForTests {
				rsName = util.RemoteRsName
				myi.ps.Logf(slogger.DEBUG, "got a remote DB request")
			}
		}

		switch strings.ToLower(doc[0].Key) {
		case "ismaster":
			// streaming isMaster is enabled. no need to fix
			if !myi.disableStreamingIsMaster {
				return mm, nil, rsName, nil
			}
			// fixing isMaster request when streamingIsMaster is disabled
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
			return mm, &IsMasterFixer{myi.mode, myi.mongoPort, myi.proxyPort}, rsName, nil
		case "find":
			if db == util.RetryOnRemoteDbNameForTests {
				return mm, &SimulateRetryFixer{mm}, "", nil
			}
			return mm, nil, rsName, nil
		default:
			return mm, nil, rsName, nil
		}
	}

	return m, nil, "", nil
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandString(length int) string {
	return stringWithCharset(length, charset)
}

func RandDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := seededRand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

func getRandStringArray(maxArrLen, strLen int) []string {
	arrLen := rand.Intn(maxArrLen) + 1
	arr := make([]string, arrLen)
	for i := 0; i < arrLen; i++ {
		arr[i] = RandString(strLen)
	}
	return arr
}

func getProxyConfig(hostname string, mongoPort, proxyPort, maxPoolSize, maxPoolIdleTimeSec int, mode util.MongoConnectionMode, enableTracing bool) ProxyConfig {
	var uri string
	if mode == util.Cluster {
		uri = fmt.Sprintf("mongodb://%s:%v,%s:%v,%s:%v/?replSet=proxytest", hostname, mongoPort, hostname, mongoPort+1, hostname, mongoPort+2)
	}
	pc := NewProxyConfig("localhost", proxyPort, uri, hostname, mongoPort, "", "", "test proxy", enableTracing, mode, ServerSelectionTimeoutSecForTests, maxPoolSize, maxPoolIdleTimeSec, 500)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{mode, mongoPort, proxyPort, false}
	return pc
}
