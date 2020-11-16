package mongonet

import (
	"crypto/x509"
	"fmt"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
)

type SSLPair struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
	Id   string
}

// driver defaults
const (
	DefaultMaxPoolSize                       = 100
	DefaultMaxPoolIdleTimeSec                = 0
	DefaultConnectionPoolHeartbeatIntervalMs = 0
)

type ProxyConfig struct {
	ServerConfig

	MongoURI           string
	MongoHost          string
	MongoPort          int
	MongoSSL           bool
	MongoRootCAs       *x509.CertPool
	MongoSSLSkipVerify bool
	MongoUser          string
	MongoPassword      string

	InterceptorFactory ProxyInterceptorFactory

	AppName string

	TraceConnPool                     bool
	ConnectionMode                    util.MongoConnectionMode
	ServerSelectionTimeoutSec         int
	MaxPoolSize                       int
	MaxPoolIdleTimeSec                int
	ConnectionPoolHeartbeatIntervalMs int
}

func NewProxyConfig(bindHost string, bindPort int, mongoUri, mongoHost string, mongoPort int, mongoUser, mongoPassword, appName string, traceConnPool bool, connectionMode util.MongoConnectionMode, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int) ProxyConfig {

	syncTlsConfig := NewSyncTlsConfig()
	return ProxyConfig{
		ServerConfig{
			bindHost,
			bindPort,
			false, // UseSSL
			nil,   // SSLKeys
			syncTlsConfig,
			0,           // MinTlsVersion
			0,           // TCPKeepAlivePeriod
			nil,         // CipherSuites
			slogger.OFF, // LogLevel
			nil,         // Appenders
		},
		mongoUri,
		mongoHost,
		mongoPort,
		false, // MongoSSL
		nil,   // MongoRootCAs
		false, // MongoSSLSkipVerify
		mongoUser,
		mongoPassword,
		nil, // InterceptorFactory
		appName,
		traceConnPool,
		connectionMode,
		serverSelectionTimeoutSec,
		maxPoolSize,
		maxPoolIdleTimeSec,
		connectionPoolHeartbeatIntervalMs,
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}
