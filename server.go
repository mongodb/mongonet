package mongonet

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
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

func (s *SyncTlsConfig) SetTlsConfig(sslKeys []*SSLPair, cipherSuites []uint16, minTlsVersion uint16, fallbackKeys []SSLPair) (ok bool, names []string, errs []error) {
	ok = true
	certs := []tls.Certificate{}
	for _, pair := range fallbackKeys {
		cer, err := tls.LoadX509KeyPair(pair.Cert, pair.Key)
		if err != nil {
			return false, []string{}, append(errs, fmt.Errorf("cannot load fallback certificate from files %s, %s. Error: %v", pair.Cert, pair.Key, err))
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
// The server will cancel the ctx passed to CreateWorkerWithContext() when it exits; causing the ctx's Done channel to be closed
// Implementing this interface will cause the server to incrememnt a session wait group when each new session starts.
// A mongonet session will decrement the wait group after calling .Close() on the session.
// When using this you should make sure that your `DoLoopTemp` returns when it receives from the context Done Channel.
type ServerWorkerWithContextFactory interface {
	ServerWorkerFactory
	CreateWorkerWithContext(session *Session, ctx context.Context) (ServerWorker, error)
}

type sessionManager struct {
	sessionWG *sync.WaitGroup
	ctx       context.Context
}

type Server struct {
	config         ServerConfig
	logger         *slogger.Logger
	workerFactory  ServerWorkerFactory
	ctx            context.Context
	cancelCtx      context.CancelFunc
	initChan       chan error
	doneChan       chan struct{}
	sessionManager *sessionManager
	net.Addr
}

// called by a synched method
func (s *Server) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	return s.config.SyncTlsConfig.SetTlsConfig(sslPairs, s.config.CipherSuites, s.config.MinTlsVersion, s.config.SSLKeys)
}

func (s *Server) Run() error {
	bindTo := fmt.Sprintf("%s:%d", s.config.BindHost, s.config.BindPort)

	s.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	defer s.cancelCtx()
	defer close(s.initChan)

	keepAlive := time.Duration(-1)       // negative Duration means keep-alives disabled in ListenConfig
	if s.config.TCPKeepAlivePeriod > 0 { // but in our config we use 0 to mean keep-alives are disabled
		keepAlive = s.config.TCPKeepAlivePeriod
	}
	lc := &net.ListenConfig{KeepAlive: keepAlive}
	ln, err := lc.Listen(s.ctx, "tcp", bindTo)
	if err != nil {
		returnErr := NewStackErrorf("cannot start listening in proxy: %s", err)
		s.initChan <- returnErr
		return returnErr
	}
	s.Addr = ln.Addr()
	s.initChan <- nil

	defer func() {
		ln.Close()

		// add another context cancellation so that it happens now.
		// Otherwise the prior deferred cancellation won't happen until
		// after this defer call (because defers are called LIFO)
		s.cancelCtx()

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

	incomingConnections := make(chan accepted, 128)

	go func() {
		for {
			conn, err := ln.Accept()
			incomingConnections <- accepted{conn, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return nil

		case connectionEvent := <-incomingConnections:
			if connectionEvent.err != nil {
				if s.ctx.Err() != nil {
					// context was cancelled.  Exit cleanly
					return nil
				}
				return NewStackErrorf("could not accept in proxy: %s", err)
			}
			go s.handleConnection(connectionEvent.conn)
		}
	}

}

func (s *Server) handshake(conn net.Conn) net.Conn {
	if !s.config.UseSSL {
		return conn
	}

	tlsConn := tls.Server(conn, s.config.SyncTlsConfig.getTlsConfig())
	s.logger.Logf(slogger.ERROR, "got a TLS connection! attempting handshake")
	if err := tlsConn.Handshake(); err != nil {
		s.logger.Logf(slogger.ERROR, "TLS Handshake failed. err=%v", err)
		return conn
	}
	s.logger.Logf(slogger.ERROR, "handshake successfull!")
	return tlsConn
}

func (s *Server) handleConnection(origConn net.Conn) {
	proxyProtoConn, err := NewConn(origConn)
	if err != nil {
		s.logger.Logf(slogger.ERROR, "Error setting up a proxy protocol compatible connection: %v", err)
		origConn.Close()
		return
	}

	if proxyProtoConn.IsProxied() {
		s.logger.Logf(
			slogger.ERROR,
			"accepted a proxied connection (local=%v, remote=%v, proxy=%v, target=%v, version=%v)",
			proxyProtoConn.LocalAddr(),
			proxyProtoConn.RemoteAddr(),
			proxyProtoConn.ProxyAddr(),
			proxyProtoConn.TargetAddr(),
			proxyProtoConn.Version(),
		)
	} else {
		s.logger.Logf(
			slogger.ERROR,
			"accepted a regular connection (local=%v, remote=%v)",
			origConn.LocalAddr(),
			origConn.RemoteAddr(),
		)
	}
	newconn := s.handshake(proxyProtoConn)
	remoteAddr := proxyProtoConn.RemoteAddr()
	c := &Session{
		s,
		nil,
		remoteAddr,
		s.NewLogger(fmt.Sprintf("Session %s", remoteAddr)),
		"",
		nil,
		proxyProtoConn.IsProxied(),
	}

	if _, ok := s.contextualWorkerFactory(); ok {
		s.sessionManager.sessionWG.Add(1)
	}

	c.Run(newconn)
}

// InitChannel returns a channel that will send nil once the server has started
// listening, or an error indicating why the server failed to start
func (s *Server) InitChannel() <-chan error {
	return s.initChan
}

func (s *Server) Close() {
	s.cancelCtx()
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
	ctx, cancelCtx := context.WithCancel(context.Background())
	return Server{
		config,
		&slogger.Logger{"Server", config.Appenders, 0, nil},
		factory,
		ctx,
		cancelCtx,
		make(chan error, 1),
		make(chan struct{}),
		&sessionManager{
			&sync.WaitGroup{},
			ctx,
		},
		nil,
	}
}

func (s *Server) contextualWorkerFactory() (ServerWorkerWithContextFactory, bool) {
	swf, ok := s.workerFactory.(ServerWorkerWithContextFactory)
	return swf, ok
}

func MergeErrors(errors ...error) error {
	n, laste := 0, error(nil)

	for _, e := range errors {
		if e != nil {
			n++
			laste = e
		}
	}
	switch n {
	case 0:
		return nil
	case 1:
		return laste
	default:
		s := make([]string, 0, n)
		for _, e := range errors {
			if e != error(nil) {
				s = append(s, e.Error())
			}
		}
		return fmt.Errorf("Multiple errors: %v", strings.Join(s, "; "))
	}
}
