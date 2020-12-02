package mongonet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type Proxy struct {
	config ProxyConfig
	server *Server

	logger          *slogger.Logger
	MongoClient     *mongo.Client
	topology        *topology.Topology
	defaultReadPref *readpref.ReadPref

	Context            context.Context
	connectionsCreated *int64
	poolCleared        *int64

	// using a sync.Map instead of a map paired with mutex because sync.Map is optimized for cases in which the access pattern is predominant by reads
	remoteConnections *sync.Map
}

type RemoteConnection struct {
	client   *mongo.Client
	topology *topology.Topology
}

// https://jira.mongodb.org/browse/GODRIVER-1760 will add the ability to create a topology.Topology from ClientOptions
func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	if d.IsZero() {
		panic("failed to extract deployment topology")
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

/*
Clients estimate secondaries’ staleness by periodically checking the latest write date of each replica set member.
Since these checks are infrequent, the staleness estimate is coarse.
Thus, clients cannot enforce a maxStalenessSeconds value of less than 90 seconds.
https://docs.mongodb.com/manual/core/read-preference-staleness/
*/
const MinMaxStalenessVal int32 = 90

func NewProxy(pc ProxyConfig) (Proxy, error) {
	ctx := context.Background()
	var initCount, initPoolCleared int64 = 0, 0
	defaultReadPref := readpref.Primary()
	p := Proxy{pc, nil, nil, nil, nil, defaultReadPref, ctx, &initCount, &initPoolCleared, &sync.Map{}}
	mongoClient, err := getMongoClientFromProxyConfig(&p, pc, ctx)
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p.MongoClient = mongoClient
	p.topology = extractTopology(mongoClient)

	p.logger = p.NewLogger("proxy")

	return p, nil
}

func getBaseClientOptions(p *Proxy, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int) *options.ClientOptions {
	opts := options.Client()
	opts.ApplyURI(uri).
		SetAppName(appName).
		SetPoolMonitor(&event.PoolMonitor{
			Event: func(evt *event.PoolEvent) {
				switch evt.Type {
				case event.ConnectionCreated:
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection created %v", evt)
					}
					p.AddConnection()
				case "ConnectionCheckOutStarted":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection check out started %v", evt)
					}
				case "ConnectionCheckedIn":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection checked in %v", evt)
					}
				case "ConnectionCheckedOut":
					if trace {
						p.logger.Logf(slogger.DEBUG, "**** Connection checked out %v", evt)
					}
				case event.PoolCleared:
					p.IncrementPoolCleared()
				}
			},
		}).
		SetServerSelectionTimeout(time.Duration(serverSelectionTimeoutSec) * time.Second).
		SetMaxPoolSize(uint64(maxPoolSize))
	if maxPoolIdleTimeSec > 0 {
		opts.SetMaxConnIdleTime(time.Duration(maxPoolIdleTimeSec) * time.Second)
	}
	if connectionPoolHeartbeatIntervalMs > 0 {
		opts.SetHeartbeatInterval(time.Duration(connectionPoolHeartbeatIntervalMs) * time.Millisecond)
	}
	return opts
}

func getMongoClientFromUri(p *Proxy, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int, rootCAs *x509.CertPool, ctx context.Context) (*mongo.Client, error) {
	opts := getBaseClientOptions(p, uri, appName, trace, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs)
	if rootCAs != nil {
		tlsConfig := &tls.Config{RootCAs: rootCAs}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(ctx, opts)
}

func getMongoClientFromProxyConfig(p *Proxy, pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	var uri string
	if pc.ConnectionMode == util.Direct {
		uri = fmt.Sprintf("mongodb://%s", pc.MongoAddress())
	} else {
		uri = pc.MongoURI
	}
	opts := getBaseClientOptions(p, uri, pc.AppName, p.config.TraceConnPool, pc.ServerSelectionTimeoutSec, pc.MaxPoolSize, pc.MaxPoolIdleTimeSec, pc.ConnectionPoolHeartbeatIntervalMs)
	opts.
		SetDirect(pc.ConnectionMode == util.Direct)

	if pc.MongoUser != "" {
		auth := options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			Username:      pc.MongoUser,
			AuthSource:    "admin",
			Password:      pc.MongoPassword,
			PasswordSet:   true,
		}
		opts.SetAuth(auth)
	}
	if pc.MongoSSL {
		tlsConfig := &tls.Config{RootCAs: pc.MongoRootCAs}
		opts.SetTLSConfig(tlsConfig)
	}
	return mongo.Connect(ctx, opts)
}

func (p *Proxy) AddRemoteConnection(rsName, uri, appName string, trace bool, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs int, rootCAs *x509.CertPool) error {
	p.logger.Logf(slogger.DEBUG, "adding remote connection for %s", rsName)
	if _, ok := p.remoteConnections.Load(rsName); ok {
		p.logger.Logf(slogger.DEBUG, "remote connection for %s already exists", rsName)
		return nil
	}
	client, err := getMongoClientFromUri(p, uri, appName, trace, serverSelectionTimeoutSec, maxPoolSize, maxPoolIdleTimeSec, connectionPoolHeartbeatIntervalMs, rootCAs, p.Context)
	if err != nil {
		return err
	}
	p.remoteConnections.Store(rsName, &RemoteConnection{client, extractTopology(client)})
	return nil
}

func (p *Proxy) ClearRemoteConnection(rsName string, additionalGracePeriodSec int) error {
	rc, ok := p.remoteConnections.Load(rsName)
	if !ok {
		p.logger.Logf(slogger.WARN, "remote connection for %s doesn't exist", rsName)
		return nil
	}
	p.logger.Logf(slogger.DEBUG, "clearing remote connection for %s", rsName)
	ctx2, cancelFn := context.WithTimeout(p.Context, time.Duration(additionalGracePeriodSec)*time.Second)
	defer cancelFn()
	// remote connections only has *mongo.Client, so no need for type check here. being extra safe about null clients just in case.
	if rc.(*RemoteConnection).client != nil {
		return rc.(*RemoteConnection).client.Disconnect(ctx2)
	}
	p.logger.Logf(slogger.DEBUG, "remote connection client is nil")
	return nil
}

func (p *Proxy) InitializeServer() {
	server := Server{
		p.config.ServerConfig,
		p.logger,
		p,
		make(chan struct{}),
		make(chan error, 1),
		make(chan struct{}),
		nil,
		nil,
	}
	p.server = &server
}

func (p *Proxy) Run() error {
	return p.server.Run()
}

// called by a synched method
func (p *Proxy) OnSSLConfig(sslPairs []*SSLPair) (ok bool, names []string, errs []error) {
	return p.server.OnSSLConfig(sslPairs)
}

func (p *Proxy) NewLogger(prefix string) *slogger.Logger {
	filters := []slogger.TurboFilter{slogger.TurboLevelFilter(p.config.LogLevel)}

	appenders := p.config.Appenders
	if appenders == nil {
		appenders = []slogger.Appender{slogger.StdOutAppender()}
	}

	return &slogger.Logger{prefix, appenders, 0, filters}
}

func (p *Proxy) AddConnection() {
	atomic.AddInt64(p.connectionsCreated, 1)
}

func (p *Proxy) IncrementPoolCleared() {
	atomic.AddInt64(p.poolCleared, 1)
}

func (p *Proxy) GetConnectionsCreated() int64 {
	return atomic.LoadInt64(p.connectionsCreated)
}

func (p *Proxy) GetPoolCleared() int64 {
	return atomic.LoadInt64(p.poolCleared)
}

func (p *Proxy) CreateWorker(session *Session) (ServerWorker, error) {
	var err error

	ps := &ProxySession{session, p, nil, nil, nil, false}
	if p.config.InterceptorFactory != nil {
		ps.interceptor, err = ps.proxy.config.InterceptorFactory.NewInterceptor(ps)
		if err != nil {
			return nil, err
		}

		if ps.proxy.config.CollectorHookFactory != nil {
			requestDurationHook, err := ps.proxy.config.CollectorHookFactory.NewHook("processingDuration", "type", "request_total")
			if err != nil {
				return nil, err
			}

			responseDurationHook, err := ps.proxy.config.CollectorHookFactory.NewHook("processingDuration", "type", "response_total")
			if err != nil {
				return nil, err
			}

			requestErrorsHook, err := ps.proxy.config.CollectorHookFactory.NewHook("processingErrors", "type", "request")
			if err != nil {
				return nil, err
			}

			responseErrorsHook, err := ps.proxy.config.CollectorHookFactory.NewHook("processingErrors", "type", "response")
			if err != nil {
				return nil, err
			}

			ps.hooks = make(map[string]MetricsHook)
			ps.hooks["requestDurationHook"] = requestDurationHook
			ps.hooks["responseDurationHook"] = responseDurationHook
			ps.hooks["requestErrorsHook"] = requestErrorsHook
			ps.hooks["responseErrorsHook"] = responseErrorsHook

			ps.isMetricsEnabled = true
		}

		session.conn = CheckedConn{session.conn.(net.Conn), ps.interceptor}
	}

	return ps, nil
}

func (p *Proxy) GetConnection(conn net.Conn) io.ReadWriteCloser {
	return conn
}
