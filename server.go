package mongonet

import "crypto/tls"
import "fmt"
import "io"
import "net"
import "strings"
import "time"

import "gopkg.in/mgo.v2/bson"

import "github.com/mongodb/slogger/v2/slogger"

type ServerConfig struct {
	BindHost string
	BindPort int

	UseSSL        bool
	SSLKeys       []SSLPair
	MinTlsVersion uint16 // see tls.Version* constants

	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives

	LogLevel  slogger.Level
	Appenders []slogger.Appender
}

// --------

type Session struct {
	server     *Server
	conn       io.ReadWriteCloser
	remoteAddr net.Addr

	logger *slogger.Logger

	SSLServerName string
}

// --------
type ServerWorker interface {
	DoLoopTemp()
	Close()
}

type ServerWorkerFactory interface {
	CreateWorker(session *Session) (ServerWorker, error)
}

// --------

type Server struct {
	config        ServerConfig
	logger        *slogger.Logger
	workerFactory ServerWorkerFactory
	killChan      chan struct{}
	initChan      chan error
	doneChan      chan struct{}
	net.Addr
}

// ------------------

func (s *Session) Logf(level slogger.Level, messageFmt string, args ...interface{}) (*slogger.Log, []error) {
	return s.logger.Logf(level, messageFmt, args...)
}

func (s *Session) ReadMessage() (Message, error) {
	return ReadMessage(s.conn)
}

func (s *Session) Run(conn net.Conn) {
	var err error
	defer conn.Close()

	s.conn = conn

	switch c := conn.(type) {
	case *tls.Conn:
		// we do this here so that we can get the SNI server name
		err = c.Handshake()
		if err != nil {
			s.logger.Logf(slogger.WARN, "error doing tls handshake %s", err)
			return
		}
		s.SSLServerName = strings.TrimSuffix(c.ConnectionState().ServerName, ".")
	}

	s.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", s.SSLServerName)

	defer s.logger.Logf(slogger.INFO, "socket closed")

	worker, err := s.server.workerFactory.CreateWorker(s)
	if err != nil {
		s.logger.Logf(slogger.WARN, "error creating worker %s", err)
		return
	}
	defer worker.Close()

	worker.DoLoopTemp()
}

func (s *Session) RespondToCommandMakeBSON(clientMessage Message, args ...interface{}) error {
	if len(args)%2 == 1 {
		return fmt.Errorf("magic bson has to be even # of args, got %d", len(args))
	}

	gotOk := false

	doc := bson.D{}
	for idx := 0; idx < len(args); idx += 2 {
		name, ok := args[idx].(string)
		if !ok {
			return fmt.Errorf("got a non string for bson name: %t", args[idx])
		}
		doc = append(doc, bson.DocElem{name, args[idx+1]})
		if name == "ok" {
			gotOk = true
		}
	}

	if !gotOk {
		doc = append(doc, bson.DocElem{"ok", 1})
	}

	doc2, err := SimpleBSONConvert(doc)
	if err != nil {
		return err
	}
	return s.RespondToCommand(clientMessage, doc2)
}

func (s *Session) RespondToCommand(clientMessage Message, doc SimpleBSON) error {
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
		return SendMessage(rm, s.conn)

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
		return SendMessage(rm, s.conn)

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
		return SendMessage(rm, s.conn)

	default:
		panic("impossible")
	}

}

func (s *Session) RespondWithError(clientMessage Message, err error) error {
	s.logger.Logf(slogger.INFO, "RespondWithError %v", err)

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
		return SendMessage(rm, s.conn)

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
		return SendMessage(rm, s.conn)

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
		return SendMessage(rm, s.conn)

	default:
		panic("impossible")
	}

}

// -------------------

func (s *Server) Run() error {
	bindTo := fmt.Sprintf("%s:%d", s.config.BindHost, s.config.BindPort)
	s.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	var tlsConfig *tls.Config

	defer close(s.initChan)

	if s.config.UseSSL {
		if len(s.config.SSLKeys) == 0 {
			returnErr := fmt.Errorf("no ssl keys configured")
			s.initChan <- returnErr
			return returnErr
		}

		certs := []tls.Certificate{}
		for _, pair := range s.config.SSLKeys {
			cer, err := tls.LoadX509KeyPair(pair.CertFile, pair.KeyFile)
			if err != nil {
				returnErr := fmt.Errorf("cannot LoadX509KeyPair from %s %s %s", pair.CertFile, pair.KeyFile, err)
				s.initChan <- returnErr
				return returnErr
			}
			certs = append(certs, cer)
		}

		tlsConfig = &tls.Config{Certificates: certs}

		if s.config.MinTlsVersion != 0 {
			tlsConfig.MinVersion = s.config.MinTlsVersion
		}

		tlsConfig.BuildNameToCertificate()
	}

	ln, err := net.Listen("tcp", bindTo)
	if err != nil {
		returnErr := NewStackErrorf("cannot start listening in proxy: %s", err)
		s.initChan <- returnErr
		return returnErr
	}
	s.Addr = ln.Addr()
	s.initChan <- nil

	defer close(s.doneChan)
	defer ln.Close()

	type accepted struct {
		conn net.Conn
		err  error
	}

	incomingConnections := make(chan accepted, 1)

	for {
		go func() {
			conn, err := ln.Accept()
			incomingConnections <- accepted{conn, err}
		}()

		select {
		case <-s.killChan:
			// TODO close down all active connections before returning
			return nil
		case connectionEvent := <-incomingConnections:
			if connectionEvent.err != nil {
				return NewStackErrorf("could not accept in proxy: %s", err)
			}
			conn := connectionEvent.conn
			if s.config.TCPKeepAlivePeriod > 0 {
				switch conn := conn.(type) {
				case *net.TCPConn:
					conn.SetKeepAlive(true)
					conn.SetKeepAlivePeriod(s.config.TCPKeepAlivePeriod)
				default:
					s.logger.Logf(slogger.WARN, "Want to set TCP keep alive on accepted connection but connection is not *net.TCPConn.  It is %T", conn)
				}
			}

			if s.config.UseSSL {
				conn = tls.Server(conn, tlsConfig)
			}

			remoteAddr := conn.RemoteAddr()
			c := &Session{s, nil, remoteAddr, s.NewLogger(fmt.Sprintf("Session %s", remoteAddr)), ""}
			go c.Run(conn)
		}

	}
}

// InitChannel returns a channel that will send nil once the server has started 
// listening, or an error indicating why the server failed to start
func (s *Server) InitChannel() <-chan error {
	return s.initChan
}

func (s *Server) Close() {
	close(s.killChan)
	<-s.doneChan
}

func (s *Server) NewLogger(prefix string) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(s.config.LogLevel)}

	appenders := s.config.Appenders
	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

func NewServer(config ServerConfig, factory ServerWorkerFactory) Server {
	return Server{
		config,
		&slogger.Logger{"Server", config.Appenders, 0, nil},
		factory,
		make(chan struct{}),
		make(chan error, 1),
		make(chan struct{}),
		nil,
	}
}
