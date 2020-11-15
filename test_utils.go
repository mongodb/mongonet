package mongonet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	ServerSelectionTimeoutSecForTests = 10
	ClientTimeoutSecForTests          = 20 * time.Second
)

func enableFailPointCloseConnection(host string, mongoPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, mongoPort, mode, false, "enableFailPointCloseConnection")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
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

func enableFailPointErrorCode(host string, mongoPort, errorCode int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, mongoPort, mode, false, "enableFailPointErrorCode")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
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
			{"errorCode", errorCode},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func disableFailPoint(host string, mongoPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, mongoPort, mode, false, "disableFailPoint")
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
		{"mode", "off"},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func EphemeralPort() (int, error) {
	fd, err := syscall.Socket(
		syscall.AF_INET,
		syscall.SOCK_STREAM,
		syscall.IPPROTO_TCP,
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to obtain socket. err=%v", err)
	}

	defer syscall.Close(fd)

	err = syscall.Bind(
		fd,
		&syscall.SockaddrInet4{
			Port: 0,                   // 0 requests that the kernel assign an ephemeral port
			Addr: [4]byte{0, 0, 0, 0}, // 0.0.0.0 means any IP address
		},
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to bind socket. err=%v", err)
	}

	var sockaddr syscall.Sockaddr
	sockaddr, err = syscall.Getsockname(fd)
	if err != nil {
		return -1, fmt.Errorf("Failed to Getsockname() for fd %v. err=%v", fd, err)
	}

	sockaddr4, ok := sockaddr.(*syscall.SockaddrInet4)
	if !ok {
		return -1, fmt.Errorf("Expected *syscall.SockaddrInet4 from Getsockname(), but got %#v", sockaddr)
	}

	return sockaddr4.Port, nil
}

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

func getProxyConfig(hostname string, mongoPort, proxyPort, maxPoolSize, maxPoolIdleTimeSec int, mode MongoConnectionMode, enableTracing bool) ProxyConfig {
	var uri string
	if mode == Cluster {
		uri = fmt.Sprintf("mongodb://%s:%v,%s:%v,%s:%v/?replSet=proxytest", hostname, mongoPort, hostname, mongoPort+1, hostname, mongoPort+2)
	}
	pc := NewProxyConfig("localhost", proxyPort, uri, hostname, mongoPort, "", "", "test proxy", enableTracing, mode, ServerSelectionTimeoutSecForTests, maxPoolSize, maxPoolIdleTimeSec, 500)
	pc.MongoSSLSkipVerify = true
	pc.InterceptorFactory = &MyFactory{mode, mongoPort, proxyPort}
	return pc
}

func getHostAndPorts() (mongoPort, proxyPort int, hostname string) {
	var err error
	mongoPort = 30000
	proxyPort, err = EphemeralPort()
	if err != nil {
		panic(err)
	}
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	hostname, err = os.Hostname()
	if err != nil {
		panic(err)
	}
	return
}

func insertDummyDocs(host string, proxyPort, numOfDocs int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "insertDummyDocs")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

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

func cleanup(host string, proxyPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "cleanup")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	dbName, collName := "test2", "foo"

	coll := client.Database(dbName).Collection(collName)
	return coll.Drop(ctx)
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
