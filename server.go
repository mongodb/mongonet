package mongonet

import "context"
import "crypto/tls"
import "fmt"
import "io"
import "net"
import "sync"
import "time"

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

type ServerWorker interface {
	DoLoopTemp()
	Close()
}

type ServerWorkerFactory interface {
	CreateWorker(session *Session) (ServerWorker, error)
	GetConnection(conn net.Conn) io.ReadWriteCloser
}

type ServerWorkerWithContextFactory interface {
	ServerWorkerFactory
	CreateWorkerWithContext(session *Session, ctx *context.Context) (ServerWorker, error)
}

type sessionManager struct {
	sessionWG    *sync.WaitGroup
	ctx          *context.Context
	stopSessions context.CancelFunc
}

type Server struct {
	config         ServerConfig
	logger         *slogger.Logger
	workerFactory  ServerWorkerFactory
	killChan       chan struct{}
	initChan       chan error
	doneChan       chan struct{}
	sessionManager *sessionManager
	net.Addr
}

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

	defer func() {
		// wait for all sessions to end
		s.sessionManager.sessionWG.Wait()
		ln.Close()
		close(s.doneChan)
	}()

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
			// close the Done channel on the sessions ctx
			s.sessionManager.stopSessions()
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
			if s.hasContextualWorkerFactory() {
				s.sessionManager.sessionWG.Add(1)
			}
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
	sessionCtx, stopSessions := context.WithCancel(context.Background())
	return Server{
		config,
		&slogger.Logger{"Server", config.Appenders, 0, nil},
		factory,
		make(chan struct{}),
		make(chan error, 1),
		make(chan struct{}),
		&sessionManager{
			&sync.WaitGroup{},
			&sessionCtx,
			stopSessions,
		},
		nil,
	}
}

func (s *Server) hasContextualWorkerFactory() bool {
	_, ok := s.workerFactory.(ServerWorkerWithContextFactory)
	return ok
}
