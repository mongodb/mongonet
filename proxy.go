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
	"go.mongodb.org/mongo-driver/mongo/readpref"
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
	Context            context.Context
	connectionsCreated int64
}

func logTrace(logger *slogger.Logger, trace bool, format string, args ...interface{}) {
	if trace {
		logger.Logf(slogger.DEBUG, format, args...)
	}
}

// MongoConnectionWrapper is used to wrap the driver connection so we can explicitly expire (close out) connections on certain occasions that aren't being picked up by the driver
type MongoConnectionWrapper struct {
	conn   driver.Connection
	bad    bool
	logger *slogger.Logger
	trace  bool
}

func (m *MongoConnectionWrapper) Close() {
	if m.conn == nil {
		logTrace(m.logger, m.trace, "mongo connection is nil!")
		return
	}
	id := m.conn.ID()
	if m.bad {
		if m.conn != nil && m.conn.ID() != "<closed>" {
			logTrace(m.logger, m.trace, "closing underlying bad mongo connection %v", id)
			ec, ok := m.conn.(*topology.Connection)
			if !ok {
				logTrace(m.logger, m.trace, "bad connection type %T", m.conn)
			}
			if err := ec.Expire(); err != nil {
				logTrace(m.logger, m.trace, "failed to expire connection. %v", err)
			}
		} else {
			logTrace(m.logger, m.trace, "bad mongo connection is nil!")
		}
	}
	logTrace(m.logger, m.trace, "closing mongo connection %v", id)
	if m.conn.ID() != "<closed>" {
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
			{"totalCreated", atomic.LoadInt64(&ps.proxy.connectionsCreated)},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp() {
	defer printPanic(ps.logger)
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
	ps.interceptor.Close()
}

func printPanic(logger *slogger.Logger) {
	if r := recover(); r != nil {
		var stacktraces bytes.Buffer
		pprof.Lookup("goroutine").WriteTo(&stacktraces, 2)
		logger.Logf(slogger.ERROR, "Recovering from mongonet panic. error is: %v \n stack traces: %v", r, stacktraces.String())
		panic(r)
	}
}

func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func (ps *ProxySession) getMongoConnection() (driver.Connection, error) {
	topo := extractTopology(ps.proxy.MongoClient)
	if ps.proxy.config.ConnectionMode == Direct {
		s := description.Server{Addr: address.Address(ps.proxy.config.MongoAddress())}
		srv, err := topo.FindServer(s)
		if err != nil {
			return nil, err
		}
		return srv.Connection(ps.proxy.Context)
	}
	// TODO - construct another read pref based on message
	srv, err := topo.SelectServer(ps.proxy.Context, description.ReadPrefSelector(readpref.Primary()))
	if err != nil {
		return nil, err
	}
	return srv.Connection(ps.proxy.Context)
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

	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())
		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				if mongoConn != nil {
					mongoConn.Close()
				}
				return nil, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return mongoConn, err
			}
			err = ps.RespondWithError(m, err)
			if err != nil {
				return mongoConn, NewStackErrorf("couldn't send error response to client %v", err)
			}
			return mongoConn, nil
		}
		if m == nil {
			// already responded
			return mongoConn, nil
		}
	}
	if mongoConn == nil || mongoConn.conn.ID() == "<closed>" {
		conn, err := ps.getMongoConnection()
		if err != nil {
			return nil, NewStackErrorf("cannot get connection to mongo %v", err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got new connection %v using connection mode %v", conn.ID(), ps.proxy.config.ConnectionMode)
		mongoConn = &MongoConnectionWrapper{conn, false, ps.proxy.logger, ps.proxy.config.TraceConnPool}
	}

	// Send message to mongo
	err = mongoConn.conn.WriteWireMessage(ps.proxy.Context, m.Serialize())
	if err != nil {
		mongoConn.bad = true
		return mongoConn, NewStackErrorf("error writing to mongo: %v", err)
	}

	if !m.HasResponse() {
		return mongoConn, nil
	}
	defer mongoConn.Close()
	// TODO - should probably return nil for mongoConn so we don't attempt to close twice

	inExhaustMode := m.IsExhaust()

	for {
		// Read message back from mongo
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "reading data from mongo conn %v", mongoConn.conn.ID())
		ret, err := mongoConn.conn.ReadWireMessage(ps.proxy.Context, nil)
		if err != nil {
			mongoConn.bad = true
			return mongoConn, NewStackErrorf("error reading wire message from mongo conn %v: %v", mongoConn.conn.ID(), err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "read data from mongo conn %v", mongoConn.conn.ID())
		resp, err := ReadMessageFromBytes(ret)
		if err != nil {
			if err == io.EOF {
				return mongoConn, err
			}
			return mongoConn, NewStackErrorf("got error reading response from mongo %v", err)
		}
		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return mongoConn, NewStackErrorf("error intercepting message %v", err)
			}
		}

		// Send message back to user
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sending back data to user from mongo conn %v", mongoConn.conn.ID())
		err = SendMessage(resp, ps.conn)
		if err != nil {
			mongoConn.bad = true
			return mongoConn, NewStackErrorf("got error sending response to client from conn %v: %v", mongoConn.conn.ID(), err)
		}
		logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sent back data to user from mongo conn %v", mongoConn.conn.ID())
		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return mongoConn, nil
		}

		switch r := resp.(type) {
		case *ReplyMessage:
			if r.CursorId == 0 {
				return mongoConn, nil
			}
		case *MessageMessage:
			if !r.HasMoreToCome() {
				// moreToCome wasn't set - stop the loop
				return mongoConn, nil
			}
		default:
			return mongoConn, NewStackErrorf("bad response type from server %T", r)
		}
	}
}

func NewProxy(pc ProxyConfig) (Proxy, error) {
	ctx := context.Background()
	p := Proxy{pc, nil, nil, nil, ctx, 0}
	mongoClient, err := getMongoClient(&p, pc, ctx)
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p.MongoClient = mongoClient
	p.logger = p.NewLogger("proxy")

	return p, nil
}

func getMongoClient(p *Proxy, pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", pc.MongoAddress())).
		SetDirect(pc.ConnectionMode == Direct).
		SetAppName(pc.AppName).
		SetPoolMonitor(&event.PoolMonitor{
			Event: func(evt *event.PoolEvent) {
				switch evt.Type {
				case event.ConnectionCreated:
					atomic.AddInt64(&p.connectionsCreated, 1)
				}
			},
		})
	if pc.ConnectionMode == Cluster {
		opts.SetServerSelectionTimeout(5 * time.Second)
	}
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
