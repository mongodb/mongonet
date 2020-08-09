package mongonet

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
)

type SyncTlsConfig struct {
	lock      sync.RWMutex
	tlsConfig *tls.Config
}

func NewSyncTlsConfig() *SyncTlsConfig {
	return &SyncTlsConfig{
		sync.RWMutex{},
		&tls.Config{},
	}
}

func (s *SyncTlsConfig) getTlsConfig() *tls.Config {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.tlsConfig
}

func (s *SyncTlsConfig) setTlsConfig(sslKeys []*SSLPair, cipherSuites []uint16, minTlsVersion uint16, fallbackKeys []SSLPair) (ok bool, names []string, errs []error) {
	ok = true
	certs := []tls.Certificate{}
	for _, pair := range fallbackKeys {
		cer, err := tls.LoadX509KeyPair(pair.Cert, pair.Key)
		if err != nil {
			return false, append(errs, fmt.Errorf("cannot load fallback certificate from files %s, %s. Error: %v", pair.Cert, pair.Key, err))
		}
		certs = append(certs, cer)
	}

	for _, pair := range sslKeys {
		cer, err := tls.X509KeyPair([]byte(pair.Cert), []byte(pair.Key))
		if err != nil {
			ok = false
			errs = append(errs, fmt.Errorf("cannot parse certificate %v. err=%v", pair.Id, err))
			continue
		}
		certs = append(certs, cer)
	}

	tlsConfig := &tls.Config{Certificates: certs}

	if minTlsVersion != 0 {
		tlsConfig.MinVersion = minTlsVersion
	}

	if cipherSuites != nil {
		tlsConfig.CipherSuites = cipherSuites
	}

	tlsConfig.ClientAuth = tls.RequestClientCert

	s.lock.Lock()
	defer s.lock.Unlock()
	s.tlsConfig = tlsConfig
	s.tlsConfig.BuildNameToCertificate()
	var names []string
	for key := range s.tlsConfig.NameToCertificate {
		names = append(names, key)
	}
	return ok, names, errs
}

type ServerConfig struct {
	BindHost string
	BindPort int

	UseSSL        bool
	SSLKeys       []SSLPair
	SyncTlsConfig *SyncTlsConfig

	MinTlsVersion      uint16        // see tls.Version* constants
	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives

	CipherSuites []uint16

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

// ServerWorkerWithContextFactory should be used when workers need to listen to the Done channel of the session context.
// The server will call stopSessions() when the server killChan is closed; stopSessions() will close the Done channel.
// Implementing this interface will cause the server to incrememnt a session wait group when each new session starts.
// A mongonet session will decrement the wait group after calling .Close() on the session.
// When using this you should make sure that your `DoLoopTemp` returns when it receives from the context Done Channel.
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

// called by a synched method
func (s *Server) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	return s.config.SyncTlsConfig.setTlsConfig(sslPairs, s.config.CipherSuites, s.config.MinTlsVersion, s.config.SSLKeys)
}

func (s *Server) Run() error {
	bindTo := fmt.Sprintf("%s:%d", s.config.BindHost, s.config.BindPort)

	s.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	defer close(s.initChan)

	ln, err := net.Listen("tcp", bindTo)
	if err != nil {
		returnErr := NewStackErrorf("cannot start listening in proxy: %s", err)
		s.initChan <- returnErr
		return returnErr
	}
	s.Addr = ln.Addr()
	s.initChan <- nil

	defer func() {
		ln.Close()
		// wait for all sessions to end
		s.logger.Logf(slogger.WARN, "waiting for sessions to close...")
		s.sessionManager.sessionWG.Wait()
		s.logger.Logf(slogger.WARN, "done")

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
				tlsConfig := s.config.SyncTlsConfig.getTlsConfig()
				conn = tls.Server(conn, tlsConfig)
			}

			remoteAddr := conn.RemoteAddr()
			c := &Session{s, nil, remoteAddr, s.NewLogger(fmt.Sprintf("Session %s", remoteAddr)), "", nil}
			if _, ok := s.contextualWorkerFactory(); ok {
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

func (s *Server) contextualWorkerFactory() (ServerWorkerWithContextFactory, bool) {
	swf, ok := s.workerFactory.(ServerWorkerWithContextFactory)
	return swf, ok
}
