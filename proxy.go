package mongonet

import (
	"bytes"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"net"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type Proxy struct {
	config   ProxyConfig
	connPool *ConnectionPool
	server   *Server

	logger *slogger.Logger

	mongoClient *mongo.Client
}

type ProxySession struct {
	*Session

	proxy       *Proxy
	interceptor ProxyInterceptor
	pooledConn  *PooledConnection
}

type MongoError struct {
	err      error
	code     int
	codeName string
}

func NewMongoError(err error, code int, codeName string) MongoError {
	return MongoError{err, code, codeName}
}

func (me MongoError) ToBSON() bson.D {
	doc := bson.D{{"ok", 0}}

	if me.err != nil {
		doc = append(doc, bson.E{"errmsg", me.err.Error()})
	}

	doc = append(doc,
		bson.E{"code", me.code},
		bson.E{"codeName", me.codeName})

	return doc
}

func (me MongoError) Error() string {
	return fmt.Sprintf(
		"code=%v codeName=%v errmsg = %v",
		me.code,
		me.codeName,
		me.err.Error(),
	)
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
			{"totalCreated", ps.proxy.connPool.totalCreated},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp() {
	var err error
	for {
		err = ps.doLoop()
		// ps.pooledConn, err = ps.doLoop(ps.pooledConn)
		if err != nil {
			// if ps.pooledConn != nil {
			// 	ps.pooledConn.Close()
			// }
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %v", err)
			}
			return
		}
	}

	// if ps.pooledConn != nil {
	// 	ps.pooledConn.Close()
	// }
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
		panic("impossible")
	}
}

func (ps *ProxySession) Close() {
	// ps.interceptor.Close()
}

func (ps *ProxySession) doLoop() error {
	fmt.Printf("Reading message for Proxy (initial loop)\n")
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return NewStackErrorf("got error reading from client: %v", err)
	}
	fmt.Printf("Got message for Proxy: %v\n", string(m.Serialize()))

	// // TODO MONGOS: pull out $readPreference from message
	// rp, err := GetReadPreference(m)
	// if err ...

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

	// COULD DO:
	// r, err := mongoClient.RunCommand({ insert: 1 }, rp)
	//
	// Don't want this because there would be performance costs to unmarshalling/marshalling BSON!
	// There are special rules for the `RunCommand` helper, it may not support passing in all the
	// options you want to pass

	// INSTEAD:
	// We want to select a server, grab a connection, and write raw bytes to it

	fmt.Printf("Getting topology\n")

	ctx := context.Background()
	var t *topology.Topology = ps.proxy.mongoClient.GetTopology() // DRIVER TEAM ASK
	fmt.Printf("Running selectServer\n")

	// For now, always do Primary (since single server)
	// Future -- pass in whatever we grabbed from message.
	rp := description.ReadPrefSelector(readpref.Primary())
	s, err := t.SelectServer(ctx, rp)
	if err != nil {
		return fmt.Errorf("Error selecting server : %v", err)
	}
	mongoConn, err := s.Connection(ctx)
	if err != nil {
		return fmt.Errorf("Error getting connection: %v", err)
	}
	fmt.Printf("Writing wire message to %v\n", mongoConn.Address())

	err = mongoConn.WriteWireMessage(ctx, m.Serialize())
	if err != nil {
		return fmt.Errorf("Error writing wire message: %v\n", err)
	}
	fmt.Printf("Wrote wire message: %v\n", string(m.Serialize()))

	if !m.HasResponse() {
		return nil
	}

	inExhaustMode := m.IsExhaust()

	for {
		ret, err := mongoConn.ReadWireMessage(ctx, nil)
		if err != nil {
			return NewStackErrorf("go error reading wire message: %v", err)
		}
		fmt.Printf("Read Wire Message\n")

		// TODO: consider performance enhancements here
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

		fmt.Printf("Read Message. Got: %v\n", string(resp.Serialize()))

		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return NewStackErrorf("error intercepting message %v", err)
			}
		}

		err = SendMessage(resp, ps.conn)
		if err != nil {
			return NewStackErrorf("got error sending response to client %v", err)
		}

		fmt.Printf("Sent Message to User: %v\n", string(resp.Serialize()))

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
	mongoClient, err := mongo.NewClient(options.Client().ApplyURI(fmt.Sprintf("mongodb://%v", pc.MongoAddress())))
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v", pc.MongoAddress())
	}
	if err := mongoClient.Connect(context.Background()); err != nil {
		return Proxy{}, NewStackErrorf("error connecting to driver client: %v\n", err)
	}

	p := Proxy{pc, NewConnectionPool(pc.MongoAddress(), pc.MongoSSL, pc.MongoRootCAs, pc.MongoSSLSkipVerify, pc.ConnectionPoolHook), nil, nil, mongoClient}

	p.logger = p.NewLogger("proxy")

	return p, nil
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

// called by a syched method
func (p *Proxy) OnSSLConfig(sslPairs []*SSLPair) {
	p.server.OnSSLConfig(sslPairs)
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
