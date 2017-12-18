package mongonet

import "crypto/tls"
import "fmt"
import "io"
import "net"
import "time"
import "strings"

import "gopkg.in/mgo.v2/bson"

import "github.com/mongodb/slogger/v2/slogger"

type Proxy struct {
	config   ProxyConfig
	connPool *ConnectionPool

	logger *slogger.Logger
}

type ProxySession struct {
	proxy      *Proxy
	conn       io.ReadWriteCloser
	remoteAddr net.Addr

	interceptor ProxyInterceptor

	logger *slogger.Logger

	SSLServerName string
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

func (ps *ProxySession) RespondToCommand(clientMessage Message, doc SimpleBSON) error {
	switch clientMessage.Header().OpCode {

	case OP_QUERY:
		rm := &ReplyMessage{
			MessageHeader{
				0,
				17, // TODO
				clientMessage.Header().RequestID,
				OP_REPLY},
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
			err = ps.respondWithError(m, err)
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
		return nil, NewStackErrorf("error writing to mongo: %s", err)
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

func (ps *ProxySession) Run(conn net.Conn) {
	var err error
	defer conn.Close()

	switch c := conn.(type) {
	case *tls.Conn:
		// we do this here so that we can get the SNI server name
		err = c.Handshake()
		if err != nil {
			ps.logger.Logf(slogger.WARN, "error doing tls handshake %s", err)
			return
		}
		ps.SSLServerName = strings.TrimSuffix(c.ConnectionState().ServerName, ".")
	}

	ps.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", ps.SSLServerName)

	if ps.proxy.config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			ps.logger.Logf(slogger.INFO, "error creating new interceptor because: %s", err)
			return
		}
		defer ps.interceptor.Close()

		ps.conn = CheckedConn{conn, ps.interceptor}
	}

	defer ps.logger.Logf(slogger.INFO, "socket closed")

	var pooledConn *PooledConnection = nil

	for {
		pooledConn, err = ps.doLoop(pooledConn)
		if err != nil {
			if pooledConn != nil {
				pooledConn.Close()
			}
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %s", err)
			}
			return
		}
	}

	if pooledConn != nil {
		pooledConn.Close()
	}

}

// -------

func NewProxy(pc ProxyConfig) Proxy {
	p := Proxy{pc, NewConnectionPool(pc.MongoAddress(), pc.MongoSSL, pc.MongoRootCAs, pc.MongoSSLSkipVerify, pc.ConnectionPoolHook), nil}

	p.logger = p.NewLogger("proxy")

	return p
}

func (p *Proxy) NewLogger(prefix string) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(p.config.LogLevel)}

	appenders := p.config.Appenders
	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

func (p *Proxy) Run() error {

	bindTo := fmt.Sprintf("%s:%d", p.config.BindHost, p.config.BindPort)
	p.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	var tlsConfig *tls.Config

	if p.config.UseSSL {
		if len(p.config.SSLKeys) == 0 {
			return fmt.Errorf("no ssl keys configured")
		}

		certs := []tls.Certificate{}
		for _, pair := range p.config.SSLKeys {
			cer, err := tls.LoadX509KeyPair(pair.CertFile, pair.KeyFile)
			if err != nil {
				return fmt.Errorf("cannot LoadX509KeyPair from %s %s %s", pair.CertFile, pair.KeyFile, err)
			}
			certs = append(certs, cer)
		}

		tlsConfig = &tls.Config{Certificates: certs}

		tlsConfig.BuildNameToCertificate()
	}

	ln, err := net.Listen("tcp", bindTo)
	if err != nil {
		return NewStackErrorf("cannot start listening in proxy: %s", err)
	}

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return NewStackErrorf("could not accept in proxy: %s", err)
		}

		if p.config.TCPKeepAlivePeriod > 0 {
			switch conn := conn.(type) {
			case *net.TCPConn:
				conn.SetKeepAlive(true)
				conn.SetKeepAlivePeriod(p.config.TCPKeepAlivePeriod)
			default:
				p.logger.Logf(slogger.WARN, "Want to set TCP keep alive on accepted connection but connection is not *net.TCPConn.  It is %T", conn)
			}
		}

		if p.config.UseSSL {
			conn = tls.Server(conn, tlsConfig)
		}

		remoteAddr := conn.RemoteAddr()
		c := &ProxySession{p, nil, remoteAddr, nil, p.NewLogger(fmt.Sprintf("ProxySession %s", remoteAddr)), ""}
		go c.Run(conn)
	}

}
