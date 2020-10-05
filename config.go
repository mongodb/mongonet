package mongonet

import (
	"crypto/x509"
	"fmt"

	"github.com/mongodb/slogger/v2/slogger"
)

type SSLPair struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
	Id   string
}

type MongoConnectionMode int

const (
	Direct  MongoConnectionMode = iota // directly connect to a specific node
	Cluster                            // use server selection and read preference to pick a node
)

func (m MongoConnectionMode) String() string {
	if m == Direct {
		return "direct"
	}
	return "cluster"
}

type ProxyConfig struct {
	ServerConfig

	MongoHost          string
	MongoPort          int
	MongoSSL           bool
	MongoRootCAs       *x509.CertPool
	MongoSSLSkipVerify bool
	MongoUser          string
	MongoPassword      string

	InterceptorFactory ProxyInterceptorFactory

	AppName string

	TraceConnPool  bool
	ConnectionMode MongoConnectionMode
}

func NewProxyConfig(bindHost string, bindPort int, mongoHost string, mongoPort int, mongoUser, mongoPassword, appName string, traceConnPool bool, connectionMode MongoConnectionMode) ProxyConfig {

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
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}
