package mongonet

import "crypto/tls"
import "fmt"
import "io"
import "net"
import "time"

import "github.com/mongodb/slogger/v2/slogger"

type ServerConfig struct {
	BindHost string
	BindPort int

	UseSSL  bool
	SSLKeys []SSLPair

	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives

	LogLevel  slogger.Level
	Appenders []slogger.Appender
}

// --------

type Session struct {
	server      *Server
	conn       io.ReadWriteCloser
	remoteAddr net.Addr

	logger *slogger.Logger
	
	SSLServerName string
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
		s.SSLServerName = c.ConnectionState().ServerName
	}

	s.logger.Logf(slogger.INFO, "new connection SSLServerName [%s]", s.SSLServerName)

	defer s.logger.Logf(slogger.INFO, "socket closed")

	worker, err := s.server.workerFactory.CreateWorker(s)
	if err != nil {
		s.logger.Logf(slogger.WARN, "error creating worker %s", err)
		return
	}
	defer worker.Close()
	
	worker.doLoopTemp()
}

// --------
type ServerWorker interface {
	doLoopTemp()
	Close()
}

type ServerWorkerFactory interface {
	CreateWorker(session *Session) (ServerWorker, error)
}

// --------

type Server struct {
	config ServerConfig
	logger *slogger.Logger
	workerFactory ServerWorkerFactory
}

func (s *Server) Run() error {
	bindTo := fmt.Sprintf("%s:%d", s.config.BindHost, s.config.BindPort)
	s.logger.Logf(slogger.WARN, "listening on %s", bindTo)

	var tlsConfig *tls.Config

	if s.config.UseSSL {
		if len(s.config.SSLKeys) == 0 {
			return fmt.Errorf("no ssl keys configured")
		}

		certs := []tls.Certificate{}
		for _, pair := range s.config.SSLKeys {
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
		c := &Session{s, nil, remoteAddr, s.NewLogger(fmt.Sprintf("ProxySession %s", remoteAddr)), ""}
		go c.Run(conn)
	}
}

func (s *Server) NewLogger(prefix string) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(s.config.LogLevel)}

	appenders := s.config.Appenders
	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

