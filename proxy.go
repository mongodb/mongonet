package mongonet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"
	"unsafe"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type Proxy struct {
	config ProxyConfig
	server *Server

	logger      *slogger.Logger
	mongoClient *mongo.Client
}

type ProxySession struct {
	*Session

	proxy       *Proxy
	interceptor ProxyInterceptor
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
			// TODO - implement if needed!
			{"totalCreated", 0},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp() {
	var err error
	for {
		err = ps.doLoop()
		if err != nil {
			// TODO - may need to close the mongo connection here
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %v", err)
			}
			return
		}
	}
	// TODO - may need to close the mongo connection here

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
	ps.interceptor.Close() // TODO - see why Louisa decided to comment this out
}

func extractTopology(c *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(c).Elem()
	d := e.FieldByName("deployment")
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

func getMongoConnection(mc *mongo.Client, ctx context.Context) (driver.Connection, error) {
	// TODO - conn string, server options, context
	topo := extractTopology(mc)
	srv, err := topo.FindServer(description.NewDefaultServer("127.0.0.1:27017"))
	if err != nil {
		return nil, err
	}
	return srv.Connection(ctx)
}

func (ps *ProxySession) doLoop() error {
	// reading message from client
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return NewStackErrorf("got error reading from client: %v", err)
	}

	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())

		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				return err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return err
			}
			err = ps.RespondWithError(m, err)
			if err != nil {
				return NewStackErrorf("couldn't send error response to client %v", err)
			}
			return nil
		}
		if m == nil {
			// already responded
			return nil
		}
	}

	// implement getting a mongo connection
	ctx := context.Background()
	mongoConn, err := getMongoConnection(ps.proxy.mongoClient, ctx)
	if err != nil {
		return fmt.Errorf("Error getting connection: %v", err)
	}
	defer mongoConn.Close()
	// Send message to mongo
	err = mongoConn.WriteWireMessage(ctx, m.Serialize())
	if err != nil {
		return NewStackErrorf("error writing to mongo: %v", err)
	}

	if !m.HasResponse() {
		return nil
	}

	inExhaustMode := m.IsExhaust()

	for {
		// Read message back from mongo
		ret, err := mongoConn.ReadWireMessage(ctx, nil)
		if err != nil {
			return NewStackErrorf("go error reading wire message: %v", err)
		}
		// TODO: will def need performance enhancements here
		// i.e. now the ReadMessage doesn't have to Read bytes,
		// since we know that the result of ReadWireMessage is
		// perfectly formed, so can just slice off bytes
		respBytesReader := bytes.NewReader(ret)
		resp, err := ReadMessage(respBytesReader)
		if err != nil {
			if err == io.EOF {
				return err
			}
			return NewStackErrorf("got error reading response from mongo %v", err)
		}
		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return NewStackErrorf("error intercepting message %v", err)
			}
		}

		// Send message back to user
		err = SendMessage(resp, ps.conn)
		if err != nil {
			return NewStackErrorf("got error sending response to client %v", err)
		}

		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil
		}

		switch r := resp.(type) {
		case *ReplyMessage:
			if r.CursorId == 0 {
				return nil
			}
		case *MessageMessage:
			if !r.HasMoreToCome() {
				// moreToCome wasn't set - stop the loop
				return nil
			}
		default:
			return NewStackErrorf("bad response type from server %T", r)
		}
	}
}

func NewProxy(pc ProxyConfig) (Proxy, error) {
	// TODO - push the context to Proxy
	mongoClient, err := getMongoClient(pc, context.Background())
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p := Proxy{pc, nil, nil, mongoClient}

	p.logger = p.NewLogger("proxy")

	return p, nil
}

func getMongoClient(pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	return mongo.Connect(ctx, options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", pc.MongoAddress())))
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

	ps := &ProxySession{session, p, nil}
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
