package mongonet

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/address"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
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
}

func (ps *ProxySession) logTrace(logger *slogger.Logger, trace bool, format string, args ...interface{}) {
	if trace {
		msg := fmt.Sprintf(fmt.Sprintf("client: %v - %s", ps.RemoteAddr(), format), args...)
		logger.Logf(slogger.DEBUG, msg)
	}
}

func (ps *ProxySession) logMessageTrace(logger *slogger.Logger, trace bool, m Message) {
	if trace {
		var doc bson.D
		var msg string
		var err error
		switch mm := m.(type) {
		case *MessageMessage:
			for _, section := range mm.Sections {
				if bs, ok := section.(*BodySection); ok {
					doc, err = bs.Body.ToBSOND()
					if err != nil {
						logger.Logf(slogger.WARN, "failed to convert body to Bson.D. err=%v", err)
						return
					}
					break
				}
			}
			msg = fmt.Sprintf("got OP_MSG %v", doc)
		case *QueryMessage:
			doc, err = mm.Query.ToBSOND()
			if err != nil {
				logger.Logf(slogger.WARN, "failed to convert query to Bson.D. err=%v", err)
				return
			}
			msg = fmt.Sprintf("got OP_QUERY %v", doc)
		case *ReplyMessage:
			doc, err = mm.Docs[0].ToBSOND()
			if err != nil {
				logger.Logf(slogger.WARN, "failed to convert reply doc to Bson.D. err=%v", err)
				return
			}
			msg = fmt.Sprintf("got OP_REPLY %v", doc)
		default:
			// not bothering about printing other message types
			msg = fmt.Sprintf("got another type %T", mm)
		}
		msg = fmt.Sprintf("client: %v - %s", ps.RemoteAddr(), msg)
		logger.Logf(slogger.DEBUG, msg)
	}
}

// MongoConnectionWrapper is used to wrap the driver connection so we can explicitly expire (close out) connections on certain occasions that aren't being picked up by the driver
type MongoConnectionWrapper struct {
	conn          driver.Connection
	ep            driver.ErrorProcessor
	expirableConn driver.Expirable
	bad           bool
	logger        *slogger.Logger
	trace         bool
}

func (m *MongoConnectionWrapper) Close(ps *ProxySession) {
	if m.conn == nil {
		m.logger.Logf(slogger.WARN, "attempt to close a nil mongo connection. noop")
		return
	}
	id := m.conn.ID()
	if m.bad {
		if m.expirableConn.Alive() {
			ps.logTrace(m.logger, m.trace, "closing underlying bad mongo connection %v", id)
			if err := m.expirableConn.Expire(); err != nil {
				ps.logTrace(m.logger, m.trace, "failed to expire connection. %v", err)
			}
		} else {
			ps.logTrace(m.logger, m.trace, "bad mongo connection is nil!")
		}
	}
	ps.logTrace(m.logger, m.trace, "closing mongo connection %v", id)
	if m.expirableConn.Alive() {
		if err := m.conn.Close(); err != nil {
			ps.logTrace(m.logger, m.trace, "failed to close mongo connection %v: %v", id, err)
		}
	}
}

type ProxySession struct {
	*Session

	proxy            *Proxy
	interceptor      ProxyInterceptor
	hooks            map[string]MetricsHook
	mongoConn        *MongoConnectionWrapper
	isMetricsEnabled bool
}

type MetricsHook interface {
	StartTimer() error
	StopTimer()
	SetGauge(val float64) error
	AddCounterGauge(val float64) error
	SubGauge(val float64) error
	IncCounterGauge() error
	DecGauge() error
}

type MetricsHookFactory interface {
	NewHook(metricName, labelName, labelValue string) (MetricsHook, error)
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
			{"totalCreated", ps.proxy.GetConnectionsCreated()},
		},
		},
	}
}

func (ps *ProxySession) DoLoopTemp() {
	defer logPanic(ps.logger)
	var err error
	for {
		ps.mongoConn, err = ps.doLoop(ps.mongoConn)
		if err != nil {
			if ps.mongoConn != nil {
				ps.mongoConn.Close(ps)
			}
			if err != io.EOF {
				ps.logger.Logf(slogger.WARN, "error doing loop: %v", err)
			}
			return
		}
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
			bsoncore.Document(doc.BSON),
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
			bsoncore.Document(doc.BSON),
		}
		return SendMessage(rm, ps.conn)

	default:
		panic(fmt.Sprintf("unsupported opcode %v", clientMessage.Header().OpCode))
	}
}

func (ps *ProxySession) Close() {
	if ps.interceptor != nil {
		ps.interceptor.Close()
	}
}

func logPanic(logger *slogger.Logger) {
	if r := recover(); r != nil {
		var stacktraces bytes.Buffer
		pprof.Lookup("goroutine").WriteTo(&stacktraces, 2)
		logger.Logf(slogger.ERROR, "Recovering from mongonet panic. error is: %v \n stack traces: %v", r, stacktraces.String())
		logger.Flush()
		panic(r)
	}
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

func getReadPrefFromOpMsg(mm *MessageMessage, logger *slogger.Logger, defaultRp *readpref.ReadPref) (rp *readpref.ReadPref, err error) {
	rpVal, err2 := mm.BodyDoc.LookupErr("$readPreference")
	if err2 != nil {
		if err2 == bsoncore.ErrElementNotFound {
			return defaultRp, nil
		}
		return nil, fmt.Errorf("got an error looking up $readPreference: %v", err)
	}
	rpDoc, ok := rpVal.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("$readPreference isn't a document")
	}
	opts := make([]readpref.Option, 0, 1)
	if maxStalenessVal, err := rpDoc.LookupErr("maxStalenessSeconds"); err == nil {
		if maxStalenessSec, ok := maxStalenessVal.AsInt32OK(); ok && maxStalenessSec >= MinMaxStalenessVal {
			opts = append(opts, readpref.WithMaxStaleness(time.Duration(maxStalenessSec)*time.Second))
		} else {
			return nil, fmt.Errorf("maxStalenessSeconds %v is invalid", maxStalenessVal)
		}
	}
	if modeVal, err2 := rpDoc.LookupErr("mode"); err2 == nil {
		modeStr, ok := modeVal.StringValueOK()
		if !ok {
			return nil, fmt.Errorf("mode %v isn't a string", modeVal)
		}
		switch strings.ToLower(modeStr) {
		case "primarypreferred":
			return readpref.PrimaryPreferred(opts...), nil
		case "secondary":
			return readpref.Secondary(opts...), nil
		case "secondarypreferred":
			return readpref.SecondaryPreferred(opts...), nil
		case "nearest":
			return readpref.Nearest(opts...), nil
		case "primary":
			return defaultRp, nil
		default:
			return nil, fmt.Errorf("got unsupported read preference %v", modeStr)
		}
	} else {
		return nil, errors.New("read preference is missing the required \"mode\" field")
	}
}

func PinnedServerSelector(addr address.Address) description.ServerSelector {
	return description.ServerSelectorFunc(func(t description.Topology, candidates []description.Server) ([]description.Server, error) {
		for _, s := range candidates {
			if s.Addr == addr {
				return []description.Server{s}, nil
			}
		}
		return nil, nil
	})
}

func (ps *ProxySession) getMongoConnection(rp *readpref.ReadPref) (*MongoConnectionWrapper, error) {
	ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "finding a server")
	var srvSelector description.ServerSelector
	if ps.proxy.config.ConnectionMode == util.Cluster {
		srvSelector = description.ReadPrefSelector(rp)
	} else {
		// Direct
		srvSelector = PinnedServerSelector(address.Address(ps.proxy.config.MongoAddress()))
	}
	srv, err := ps.proxy.topology.SelectServer(ps.proxy.Context, srvSelector)
	if err != nil {
		return nil, err
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "found a server. connecting")
	ep, ok := srv.(driver.ErrorProcessor)
	if !ok {
		return nil, fmt.Errorf("server ErrorProcessor type assertion failed")
	}
	conn, err := srv.Connection(ps.proxy.Context)
	if err != nil {
		return nil, err
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "connected")
	ec, ok := conn.(driver.Expirable)
	if !ok {
		return nil, fmt.Errorf("bad connection type %T", conn)
	}
	return &MongoConnectionWrapper{conn, ep, ec, false, ps.proxy.logger, ps.proxy.config.TraceConnPool}, nil
}

func wrapNetworkError(err error) error {
	labels := []string{driver.NetworkError}
	return driver.Error{Message: err.Error(), Labels: labels, Wrapped: err}
}

func (ps *ProxySession) doLoop(mongoConn *MongoConnectionWrapper) (*MongoConnectionWrapper, error) {
	var requestDurationHook, responseDurationHook, requestErrorsHook, responseErrorsHook MetricsHook

	if ps.isMetricsEnabled {
		var ok bool
		requestDurationHook, ok = ps.hooks["requestDurationHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the request processing duration metric hook")
		}
		responseDurationHook, ok = ps.hooks["responseDurationHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the response processing duration metric hook")
		}
		requestErrorsHook, ok = ps.hooks["requestErrorsHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the request processing errors metric hook")
		}
		responseErrorsHook, ok = ps.hooks["responseErrorsHook"]
		if !ok {
			return nil, fmt.Errorf("could not access the response processing errors metric hook")
		}
	}

	// reading message from client
	ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "reading message from client")
	m, err := ReadMessage(ps.conn)
	if err != nil {
		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "reading message from client fail %v", err)
		if ps.isMetricsEnabled {
			hookErr := requestErrorsHook.IncCounterGauge()
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
			}
		}
		if err == io.EOF {
			return mongoConn, err
		}
		return mongoConn, NewStackErrorf("got error reading from client: %v", err)
	}

	if ps.isMetricsEnabled {
		hookErr := requestDurationHook.StartTimer()
		if hookErr != nil {
			ps.proxy.logger.Logf(slogger.WARN, "failed to start request duration metric hook timer %v", hookErr)
		}
	}

	var rp *readpref.ReadPref = ps.proxy.defaultReadPref
	if ps.proxy.config.ConnectionMode == util.Cluster {
		// only concerned about OP_MSG at this point
		mm, ok := m.(*MessageMessage)
		if ok {
			rp2, err := getReadPrefFromOpMsg(mm, ps.proxy.logger, rp)
			if err != nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if rp2 != nil {
				rp = rp2
			}
		}
	}
	ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got message from client")
	ps.logMessageTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, m)
	var respInter ResponseInterceptor
	if ps.interceptor != nil {
		ps.interceptor.TrackRequest(m.Header())
		m, respInter, err = ps.interceptor.InterceptClientToMongo(m)
		if err != nil {
			if m == nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if !m.HasResponse() {
				// we can't respond, so we just fail
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, err
			}
			if respondErr := ps.RespondWithError(m, err); respondErr != nil {
				if ps.isMetricsEnabled {
					hookErr := requestErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
					}
				}
				return mongoConn, NewStackErrorf("couldn't send error response to client; original error: %v, error sending response: %v", err, respondErr)
			}
			return mongoConn, nil
		}
		if m == nil {
			// already responded
			return mongoConn, nil
		}
	}
	if mongoConn == nil || !mongoConn.expirableConn.Alive() {
		mongoConn, err = ps.getMongoConnection(rp)
		if err != nil {
			if ps.isMetricsEnabled {
				hookErr := requestErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
				}
			}
			ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "failed to get a new connection. err=%v", err)
			return nil, NewStackErrorf("cannot get connection to mongo %v using connection mode=%v readpref=%v", err, ps.proxy.config.ConnectionMode, rp)
		}
		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got new connection %v using connection mode=%v readpref=%v", mongoConn.conn.ID(), ps.proxy.config.ConnectionMode, rp)
	}

	if ps.isMetricsEnabled {
		requestDurationHook.StopTimer()
	}

	// Send message to mongo
	err = mongoConn.conn.WriteWireMessage(ps.proxy.Context, m.Serialize())
	if err != nil {
		if ps.isMetricsEnabled {
			hookErr := requestErrorsHook.IncCounterGauge()
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to increment requestErrorsHook %v", hookErr)
			}
		}
		mongoConn.ep.ProcessError(wrapNetworkError(err), mongoConn.conn)
		return mongoConn, NewStackErrorf("error writing to mongo: %v", err)
	}

	if !m.HasResponse() {
		return mongoConn, nil
	}
	defer mongoConn.Close(ps)

	inExhaustMode := m.IsExhaust()

	for {
		// Read message back from mongo
		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "reading data from mongo conn %v", mongoConn.conn.ID())
		ret, err := mongoConn.conn.ReadWireMessage(ps.proxy.Context, nil)
		if err != nil {
			ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "error reading wire message mongo conn %v %v", mongoConn.conn.ID(), err)
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			mongoConn.ep.ProcessError(wrapNetworkError(err), mongoConn.conn)
			return nil, NewStackErrorf("error reading wire message from mongo conn %v: %v", mongoConn.conn.ID(), err)
		}

		if ps.isMetricsEnabled {
			hookErr := responseDurationHook.StartTimer()
			if hookErr != nil {
				ps.proxy.logger.Logf(slogger.WARN, "failed to start reponse duration metric hook timer %v", hookErr)
			}
		}

		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "read data from mongo conn %v", mongoConn.conn.ID())
		resp, err := ReadMessageFromBytes(ret)
		if err != nil {
			ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "error reading message from bytes on mongo conn %v %v", mongoConn.conn.ID(), err)
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			if err == io.EOF {
				return nil, err
			}
			return nil, NewStackErrorf("got error reading response from mongo %v", err)
		}
		switch mm := resp.(type) {
		case *MessageMessage:
			if err := extractError(mm.BodyDoc); err != nil {
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "processing error %v on mongo conn %v", err, mongoConn.conn.ID())
				mongoConn.ep.ProcessError(err, mongoConn.conn)
			}
		case *ReplyMessage:
			if err := extractError(mm.CommandDoc); err != nil {
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "processing error %v on mongo conn %v", err, mongoConn.conn.ID())
				mongoConn.ep.ProcessError(err, mongoConn.conn)
			}
		}
		if respInter != nil {
			resp, err = respInter.InterceptMongoToClient(resp)
			if err != nil {
				if ps.isMetricsEnabled {
					hookErr := responseErrorsHook.IncCounterGauge()
					if hookErr != nil {
						ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
					}
				}
				return nil, NewStackErrorf("error intercepting message %v", err)
			}
		}

		ps.logMessageTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, resp)
		if ps.isMetricsEnabled {
			responseDurationHook.StopTimer()
		}

		// Send message back to user
		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sending back data to user from mongo conn %v", mongoConn.conn.ID())
		err = SendMessage(resp, ps.conn)
		if err != nil {
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			mongoConn.bad = true
			ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "got error sending response to client from conn %v. err=%v", mongoConn.conn.ID(), err)
			return nil, NewStackErrorf("got error sending response to client from conn %v: %v", mongoConn.conn.ID(), err)
		}
		ps.logTrace(ps.proxy.logger, ps.proxy.config.TraceConnPool, "sent back data to user from mongo conn %v", mongoConn.conn.ID())
		if ps.interceptor != nil {
			ps.interceptor.TrackResponse(resp.Header())
		}

		if !inExhaustMode {
			return nil, nil
		}

		switch r := resp.(type) {
		case *ReplyMessage:
			if r.CursorId == 0 {
				return nil, nil
			}
		case *MessageMessage:
			if !r.HasMoreToCome() {
				// moreToCome wasn't set - stop the loop
				return nil, nil
			}
		default:
			if ps.isMetricsEnabled {
				hookErr := responseErrorsHook.IncCounterGauge()
				if hookErr != nil {
					ps.proxy.logger.Logf(slogger.WARN, "failed to increment responseErrorsHook %v", hookErr)
				}
			}
			return nil, NewStackErrorf("bad response type from server %T", r)
		}
	}
}

func NewProxy(pc ProxyConfig) (Proxy, error) {
	ctx := context.Background()
	var initCount, initPoolCleared int64 = 0, 0
	defaultReadPref := readpref.Primary()
	p := Proxy{pc, nil, nil, nil, nil, defaultReadPref, ctx, &initCount, &initPoolCleared}
	mongoClient, err := getMongoClient(&p, pc, ctx)
	if err != nil {
		return Proxy{}, NewStackErrorf("error getting driver client for %v: %v", pc.MongoAddress(), err)
	}
	p.MongoClient = mongoClient
	p.topology = extractTopology(mongoClient)

	p.logger = p.NewLogger("proxy")

	return p, nil
}

func getMongoClient(p *Proxy, pc ProxyConfig, ctx context.Context) (*mongo.Client, error) {
	opts := options.Client()
	if pc.ConnectionMode == util.Direct {
		opts.ApplyURI(fmt.Sprintf("mongodb://%s", pc.MongoAddress()))
	} else {
		opts.ApplyURI(pc.MongoURI)
	}
	trace := p.config.TraceConnPool
	opts.
		SetDirect(pc.ConnectionMode == util.Direct).
		SetAppName(pc.AppName).
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
		SetServerSelectionTimeout(time.Duration(pc.ServerSelectionTimeoutSec) * time.Second).
		SetMaxPoolSize(uint64(pc.MaxPoolSize))
	if pc.MaxPoolIdleTimeSec > 0 {
		opts.SetMaxConnIdleTime(time.Duration(pc.MaxPoolIdleTimeSec) * time.Second)
	}
	if pc.ConnectionPoolHeartbeatIntervalMs > 0 {
		opts.SetHeartbeatInterval(time.Duration(pc.ConnectionPoolHeartbeatIntervalMs) * time.Millisecond)
	}

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
