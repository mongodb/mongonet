package mongonet

import "fmt"
import "io"
import "net"
import "time"

import "gopkg.in/mgo.v2/bson"

import "github.com/mongodb/slogger/v2/slogger"

type Proxy struct {
	config   ProxyConfig
	connPool *ConnectionPool

	logger *slogger.Logger
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
		doc = append(doc, bson.DocElem{"errmsg", me.err.Error()})
	}

	doc = append(doc,
		bson.DocElem{"code", me.code},
		bson.DocElem{"codeName", me.codeName})

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
		ps.pooledConn, err = ps.doLoop(ps.pooledConn)
		if err != nil {
			if ps.pooledConn != nil {
				ps.pooledConn.Close()
			}
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %s", err)
			}
			return
		}
	}

	if ps.pooledConn != nil {
		ps.pooledConn.Close()
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
		panic("impossible")
	}
}

func (ps *ProxySession) Close() {
	ps.interceptor.Close()
}

func (ps *ProxySession) doLoop(pooledConn *PooledConnection) (*PooledConnection, error) {
	m, err := ReadMessage(ps.conn)
	if err != nil {
		if err == io.EOF {
			return pooledConn, err
		}
		return pooledConn, NewStackErrorf("got error reading from client: %s", err)
	}

	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())

		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				if pooledConn != nil {
					pooledConn.Close()
				}
				return nil, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				return pooledConn, err
			}
			err = ps.RespondWithError(m, err)
			if err != nil {
				return pooledConn, NewStackErrorf("couldn't send error response to client %s", err)
			}
			return pooledConn, nil
		}
		if m == nil {
			// already responded
			return pooledConn, nil
		}
	}

	if pooledConn == nil {
		pooledConn, err = ps.proxy.connPool.Get()
		if err != nil {
			return nil, NewStackErrorf("cannot get connection to mongo %s", err)
		}
	}

	if pooledConn.closed {
		panic("oh no!")
	}
	mongoConn := pooledConn.conn

	err = SendMessage(m, mongoConn)
	if err != nil {
		return pooledConn, NewStackErrorf("error writing to mongo: %s", err)
	}

	if !m.HasResponse() {
		return pooledConn, nil
	}

	defer pooledConn.Close()

	inExhaustMode :=
		m.Header().OpCode == OP_QUERY &&
			m.(*QueryMessage).Flags&(1<<6) != 0

	for {
		resp, err := ReadMessage(mongoConn)
		if err != nil {
			pooledConn.bad = true
			return nil, NewStackErrorf("got error reading response from mongo %s", err)
		}

		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				return nil, NewStackErrorf("error intercepting message %s", err)
			}
		}

		err = SendMessage(resp, ps.conn)
		if err != nil {
			return nil, NewStackErrorf("got error sending response to client %s", err)
		}

		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil, nil
		}

		if resp.(*ReplyMessage).CursorId == 0 {
			return nil, nil
		}
	}
}

func NewProxy(pc ProxyConfig) Proxy {
	p := Proxy{pc, NewConnectionPool(pc.MongoAddress(), pc.MongoSSL, pc.MongoRootCAs, pc.MongoSSLSkipVerify, pc.ConnectionPoolHook), nil}

	p.logger = p.NewLogger("proxy")

	return p
}

func (p *Proxy) Run() error {
	server := Server{
		p.config.ServerConfig,
		p.logger,
		p,
		make(chan struct{}),
		make(chan error, 1),
		make(chan struct{}),
		nil,
	}
	return server.Run()
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
