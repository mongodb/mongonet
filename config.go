package mongonet

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"

	"github.com/mongodb/slogger/v2/slogger"
)

type SSLPair struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

type ProxyConfig struct {
	ServerConfig

	MongoHost          string
	MongoPort          int
	MongoSSL           bool
	MongoRootCAs       *x509.CertPool
	MongoSSLSkipVerify bool

	InterceptorFactory ProxyInterceptorFactory

	ConnectionPoolHook ConnectionHook
}

func NewProxyConfig(bindHost string, bindPort int, mongoHost string, mongoPort int) ProxyConfig {
	return ProxyConfig{
		ServerConfig{
			bindHost,
			bindPort,
			false, // UseSSL
			nil,   // SSLKeys
			&SyncTlsConfig{
				sync.RWMutex{},
				&tls.Config{},
			},
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
		nil,   // InterceptorFactory
		nil,   // ConnectionPoolHook
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}
