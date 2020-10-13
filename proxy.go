package mongonet

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime/pprof"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type Proxy struct {
	config ProxyConfig
	server *Server

	logger             *slogger.Logger
	MongoClient        *mongo.Client
	topology           *topology.Topology
	descriptionServer  description.Server
	Context            context.Context
	connectionsCreated *int64
}

func logTrace(logger *slogger.Logger, trace bool, format string, args ...interface{}) {
	if trace {
		fmt.Printf(fmt.Sprintf("%s\n", format), args...)
		logger.Logf(slogger.DEBUG, format, args...)
	}
}

func logMessageTrace(logger *slogger.Logger, trace bool, m Message) {
	if trace {
		var doc bson.D
		switch mm := m.(type) {
		case *MessageMessage:
			for _, section := range mm.Sections {
				if bs, ok := section.(*BodySection); ok {
					doc, _ = bs.Body.ToBSOND()
				}
			}
		case *QueryMessage:
			doc, _ = mm.Query.ToBSOND()
		case *ReplyMessage:
			doc, _ = mm.Docs[0].ToBSOND()
		default:
			// not bothering about printing other message types
			return
		}

		msg := fmt.Sprintf("got message %v", doc)
		fmt.Println(msg)
		logger.Logf(slogger.DEBUG, msg)
	}
}

// MongoConnectionWrapper is used to wrap the driver connection so we can explicitly expire (close out) connections on certain occasions that aren't being picked up by the driver
type MongoConnectionWrapper struct {
	conn          driver.Connection
	expirableConn driver.Expirable
	bad           bool
	logger        *slogger.Logger
	trace         bool
}

func (m *MongoConnectionWrapper) Close() {
	if m.conn == nil {
		m.logger.Logf(slogger.WARN, "attempt to close a nil mongo connection. noop")
		return
	}
	id := m.conn.ID()
	if m.bad {
		if m.expirableConn.Alive() {
			logTrace(m.logger, m.trace, "closing underlying bad mongo connection %v", id)
			if err := m.expirableConn.Expire(); err != nil {
				logTrace(m.logger, m.trace, "failed to expire connection. %v", err)
			}
		} else {
			logTrace(m.logger, m.trace, "bad mongo connection is nil!")
		}
	}
	logTrace(m.logger, m.trace, "closing mongo connection %v", id)
	if m.expirableConn.Alive() {
		if err := m.conn.Close(); err != nil {
			logTrace(m.logger, m.trace, "failed to close mongo connection %v: %v", id, err)
		}
	}
}

type ProxySession struct {
	*Session

	proxy       *Proxy
	interceptor ProxyInterceptor
	mongoConn   *MongoConnectionWrapper
}

type ResponseInterceptor interface {
	InterceptMongoToClient(m Message) (Message, error)
}

type ProxyInterceptor interface {
	InterceptClientToMongo(m Message) (Message, ResponseInterceptor, error)
	Close()
	TrackRequest(MessageHeader)
	TrackResponse(MessageHeader)
	CheckConnection() error
	CheckConnectionInterval() time.Duration
}

type ProxyInterceptorFactory interface {
	// This has to be thread safe, will be called from many clients
	NewInterceptor(ps *ProxySession) (ProxyInterceptor, error)
}

// -----

func (ps *ProxySession) RemoteAddr() net.Addr {
	return ps.remoteAddr
}

func (ps *ProxySession) GetLogger() *slogger.Logger {
	return ps.logger
}

func (ps *ProxySession) ServerPort() int {
	return ps.proxy.config.BindPort
}

func (ps *ProxySession) Stats() bson.D {
	return bson.D{
		{"connectionPool", bson.D{
			{"totalCreated", ps.proxy.GetConnectionsCreated()},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp() {
	defer logPanic(ps.logger)
	var err error
	for {
		ps.mongoConn, err = ps.doLoop(ps.mongoConn)
		if err != nil {
			if ps.mongoConn != nil {
				ps.mongoConn.Close()
			}
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %v", err)
			}
			return
		}
	}
}

func (ps *ProxySession) respondWithError(clientMessage Message, err error) error {
	ps.logger.Logf(slogger.INFO, "respondWithError %v", err)

	var errBSON bson.D
	if err == nil {
		errBSON = bson.D{{"ok", 1}}
	} else if mongoErr, ok := err.(MongoError); ok {
		errBSON = mongoErr.ToBSON()
	} else {
		errBSON = bson.D{{"ok", 0}, {"errmsg", err.Error()}}
	}

	doc, myErr := SimpleBSONConvert(errBSON)
	if myErr != nil {
		return myErr
	}

	switch clientMessage.Header().OpCode {
	case OP_QUERY, OP_GET_MORE:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},

			// We should not set the error bit because we are
			// responding with errmsg instead of $err
			0, // flags - error bit

			0, // cursor id
			0, // StartingFrom
			1, // NumberReturned
			[]SimpleBSON{doc},
		}
		return SendMessage(rm, ps.conn)

	case OP_COMMAND:
		rm := &CommandReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_COMMAND_REPLY},
			doc,
			SimpleBSONEmpty(),
			[]SimpleBSON{},
		}
		return SendMessage(rm, ps.conn)

	case OP_MSG:
		rm := &MessageMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_MSG},
			0,
			[]MessageMessageSection{
				&BodySection{
					doc,
				},
			},
		}
		return SendMessage(rm, ps.conn)

	default:
		panic(fmt.Sprintf("unsupported opcode %v", clientMessage.Header().OpCode))
	}
}

func (ps *ProxySession) Close() {
	if ps.interceptor != nil {
		ps.interceptor.Close()
	}
}

func logPanic(logger *slogger.Logger) {
	if r := recover(); r != nil {
		var stacktraces bytes.Buffer
		pprof.Lookup("goroutine").WriteTo(&stacktraces, 2)
		logger.Logf(slogger.ERROR, "Recovering from mongonet panic. error is: %v \n stack traces: %v", r, stacktraces.String())
		logger.Flush()
		panic(r)
	}
}

// https://jira.mongodb.org/browse/GODRIVER-1760 will add the ability to create a topology.Topology from ClientOptions
func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	if d.IsZero() {
		panic("failed to extract deployment topology")
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func (ps *ProxySession) getMongoConnection() (*MongoConnectionWrapper, error) {
	srv, err := ps.proxy.topology.FindServer(ps.proxy.descriptionServer)
	if err != nil {
		return nil, err
	}
	conn, err := srv.Connection(ps.proxy.Context)
	if err != nil {
		return nil, err
	}
	ec, ok := conn.(driver.Expirable)
	if !ok {
		return nil, fmt.Errorf("bad connection type %T", conn)
	}
	return &MongoConnectionWrapper{conn, ec, false, ps.proxy.logger, ps.proxy.config.TraceConnPool}, nil
}

func (ps *ProxySession) doLoop(mongoConn *MongoConnectionWrapper) (*MongoConnectionWrapper, error) {
	// reading message from client
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return mongoConn, err
		}
		return mongoConn, NewStackErrorf("got error reading from client: %v", err)
	}
	logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got message from client")
	logMessageTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, m)
	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())
		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				return mongoConn, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return mongoConn, err
			}
			if respondErr := ps.RespondWithError(m, err); respondErr != nil {
				return mongoConn, NewStackErrorf("couldn't send error response to client; original error: %v, error sending response: %v", err, respondErr)
			}
			return mongoConn, nil
		}
		if m == nil {
			// already responded
			return mongoConn, nil
		}
	}
	if mongoConn == nil || !mongoConn.expirableConn.Alive() {
		mongoConn, err = ps.getMongoConnection()
		if err != nil {
			return nil, NewStackErrorf("cannot get connection to mongo %v", err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got new connection %v", mongoConn.conn.ID())
	}

	// Send message to mongo
	err = mongoConn.conn.WriteWireMessage(ps.proxy.Context, m.Serialize())
	if err != nil {
		return mongoConn, NewStackErrorf("error writing to mongo: %v", err)
	}

	if !m.HasResponse() {
		return mongoConn, nil
	}
	defer mongoConn.Close()

	inExhaustMode := m.IsExhaust()

	for {
		// Read message back from mongo
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "reading data from mongo conn %v", mongoConn.conn.ID())
		ret, err := mongoConn.conn.ReadWireMessage(ps.proxy.Context, nil)
		if err != nil {
			return nil, NewStackErrorf("error reading wire message from mongo conn %v: %v", mongoConn.conn.ID(), err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "read data from mongo conn %v", mongoConn.conn.ID())
		resp, err := ReadMessageFromBytes(ret)
		if err != nil {
			if err == io.EOF {
				return nil, err
			}
			return nil, NewStackErrorf("got error reading response from mongo %v", err)
		}
		logMessageTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, resp)
		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return nil, NewStackErrorf("error intercepting message %v", err)
			}
		}

		// Send message back to user
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sending back data to user from mongo conn %v", mongoConn.conn.ID())
		err = SendMessage(resp, ps.conn)
		if err != nil {
			mongoConn.bad = true
			return nil, NewStackErrorf("got error sending response to client from conn %v: %v", mongoConn.conn.ID(), err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sent back data to user from mongo conn %v", mongoConn.conn.ID())
		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil, nil
		}

		switch r := resp.(type) {
		case *ReplyMessage:
			if r.CursorId == 0 {
				return nil, nil
			}
		case *MessageMessage:
			if !r.HasMoreToCome() {
				// moreToCome wasn't set - stop the loop
				return nil, nil
			}
		default:
			return nil, NewStackErrorf("bad response type from server %T", r)
		}
	}
}

func NewProxy(pc ProxyConfig) (Proxy, error) {
	ctx := context.Background()
	var initCount int64 = 0
	p := Proxy{pc, nil, nil, nil, nil, description.Server{Addr: address.Address(pc.MongoAddress())}, ctx, &initCount}
	mongoClient, err := getMongoClient(&p, pc, ctx)
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p.MongoClient = mongoClient
	p.topology = extractTopology(mongoClient)
	p.logger = p.NewLogger("proxy")

	return p, nil
}

func getMongoClient(p *Proxy, pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", pc.MongoAddress())).
		SetDirect(true).
		SetAppName(pc.AppName).
		SetPoolMonitor(&event.PoolMonitor{
			Event: func(evt *event.PoolEvent) {
				switch evt.Type {
				case event.ConnectionCreated:
					p.AddConnection()
				}
			},
		})
	if pc.MongoUser != "" {
		auth := options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			Username:      pc.MongoUser,
			AuthSource:    "admin",
			Password:      pc.MongoPassword,
			PasswordSet:   true,
		}
		opts.SetAuth(auth)
	}
	if pc.MongoSSL {
		tlsConfig := &tls.Config{RootCAs: pc.MongoRootCAs}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(ctx, opts)
}

func (p *Proxy) InitializeServer() {
	server := Server{
		p.config.ServerConfig,
		p.logger,
		p,
		make(chan struct{}),
		make(chan error, 1),
		make(chan struct{}),
		nil,
		nil,
	}
	p.server = &server
}

func (p *Proxy) Run() error {
	return p.server.Run()
}

// called by a synched method
func (p *Proxy) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	return p.server.OnSSLConfig(sslPairs)
}

func (p *Proxy) NewLogger(prefix string) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(p.config.LogLevel)}

	appenders := p.config.Appenders
	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

func (p *Proxy) AddConnection() {
	atomic.AddInt64(p.connectionsCreated, 1)
}

func (p *Proxy) GetConnectionsCreated() int64 {
	return atomic.LoadInt64(p.connectionsCreated)
}

func (p *Proxy) CreateWorker(session *Session) (ServerWorker, error) {
	var err error

	ps := &ProxySession{session, p, nil, nil}
	if p.config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			return nil, err
		}

		session.conn = CheckedConn{session.conn.(net.Conn), ps.interceptor}
	}

	return ps, nil
}

func (p *Proxy) GetConnection(conn net.Conn) io.ReadWriteCloser {
	return conn
}
