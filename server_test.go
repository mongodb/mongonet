package mongonet_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/mongodb/mongonet"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BaseServerSession struct {
	session *mongonet.Session
	mydata  map[string][]bson.D
}

func (mss *BaseServerSession) handleIsMaster(mm mongonet.Message) error {
	return mss.session.RespondToCommandMakeBSON(mm,
		"ismaster", true,
		"maxBsonObjectSize", 16777216,
		"maxMessageSizeBytes", 48000000,
		"maxWriteBatchSize", 100000,
		//"localTime", ISODate("2017-12-14T17:40:28.640Z"),
		"logicalSessionTimeoutMinutes", 30,
		"minWireVersion", 0,
		"maxWireVersion", 6,
		"readOnly", false,
	)
}

func (mss *BaseServerSession) handleInsert(mm mongonet.Message, cmd bson.D, ns string) error {
	mss.mydata[ns] = []bson.D{{{"foo", int32(17)}}}
	return mss.session.RespondToCommandMakeBSON(mm)
}

func (mss *BaseServerSession) handleEndSessions(mm mongonet.Message) error {
	return mss.session.RespondToCommandMakeBSON(mm)
}

func (mss *BaseServerSession) handleMessage(m mongonet.Message) (error, bool) {

	switch mm := m.(type) {
	case *mongonet.QueryMessage:

		if !mongonet.NamespaceIsCommand(mm.Namespace) {
			return fmt.Errorf("can only use old query style to bootstrap, not a valid namesapce (%s)", mm.Namespace), false
		}

		cmd, err := mm.Query.ToBSOND()
		if err != nil {
			return fmt.Errorf("old thing not valid, barfing %s", err), true
		}

		if len(cmd) == 0 {
			return fmt.Errorf("old thing not valid, barfing"), true
		}

		db := mongonet.NamespaceToDB(mm.Namespace)
		cmdName := cmd[0].Key
		switch strings.ToLower(cmdName) {
		case "getnonce":
			return mss.session.RespondToCommandMakeBSON(mm, "nonce", "914653afbdbdb833"), false
		case "ismaster":
			return mss.handleIsMaster(mm), false
		case "ping":
			return mss.session.RespondToCommandMakeBSON(mm), false
		case "insert":
			valStr, ok := cmd[0].Value.(string)
			if !ok {
				return fmt.Errorf("command value should be string but is %T", cmd[0].Value), false
			}
			ns := fmt.Sprintf("%s.%s", db, valStr)
			docs := mongonet.BSONIndexOf(cmd, "documents")
			if docs < 0 {
				return fmt.Errorf("no documents to insert :("), false
			}

			old, found := mss.mydata[ns]
			if !found {
				old = []bson.D{}
			}

			toInsert, _, err := mongonet.GetAsBSONDocs(cmd[docs])
			if err != nil {
				return fmt.Errorf("documents not a good array: %s", err), false
			}

			for _, d := range toInsert {
				old = append(old, d)
			}

			mss.mydata[ns] = old
			return mss.session.RespondToCommandMakeBSON(mm), false
		case "find":
			collVal, ok := cmd[0].Value.(string)
			if !ok {
				return fmt.Errorf("expected to get a string but got %T", cmd[0].Value), false
			}
			ns := fmt.Sprintf("%s.%s", db, collVal)

			data, found := mss.mydata[ns]
			if !found {
				data = []bson.D{}
			}
			return mss.session.RespondToCommandMakeBSON(mm,
				"cursor", bson.D{{"firstBatch", data}, {"id", 0}, {"ns", ns}},
			), false
		}

		return fmt.Errorf("command (%s) not done", cmdName), true

	case *mongonet.CommandMessage:
		fmt.Printf("hi2 %#v\n", mm)
	case *mongonet.MessageMessage:
		var bodySection *mongonet.BodySection = nil

		for _, section := range mm.Sections {
			if bodySec, ok := section.(*mongonet.BodySection); ok {
				if bodySection != nil {
					return fmt.Errorf("OP_MSG contains more than one body section"), true
				}

				bodySection = bodySec
			}
		}

		if bodySection == nil {
			return fmt.Errorf("OP_MSG does not contain a body section"), true
		}

		cmd, err := bodySection.Body.ToBSOND()
		if err != nil {
			return mongonet.NewStackErrorf("can't parse body section bson: %v", err), true
		}

		if len(cmd) == 0 {
			return mongonet.NewStackErrorf("can't parse body section bson. length=0"), true
		}
		idx := mongonet.BSONIndexOf(cmd, "$db")
		if idx < 0 {
			return fmt.Errorf("can't find the db"), true
		}
		db, _, err := mongonet.GetAsString(cmd[idx])
		if err != nil {
			return mongonet.NewStackErrorf("can't parse the db from bson: %v", err), true
		}
		ns := fmt.Sprintf("%s.%s", db, cmd[0].Value)
		cmdName := cmd[0].Key
		switch strings.ToLower(cmdName) {
		case "ismaster":
			return mss.handleIsMaster(mm), false
		case "insert":
			return mss.handleInsert(mm, cmd, ns), false
		case "find":
			collVal, ok := cmd[0].Value.(string)
			if !ok {
				return fmt.Errorf("expected to get a string but got %T", cmd[0].Value), false
			}
			ns := fmt.Sprintf("%s.%s", db, collVal)

			data, found := mss.mydata[ns]
			if !found {
				data = []bson.D{}
			}
			return mss.session.RespondToCommandMakeBSON(mm,
				"cursor", bson.D{{"firstBatch", data}, {"id", int64(0)}, {"ns", ns}},
			), false
		case "endsessions":
			return mss.handleEndSessions(mm), false
		}
		return nil, false
	}

	return fmt.Errorf("what are you! %t", m), true
}

type MyServerSession struct {
	*BaseServerSession
}

/*
* @return (error, <fatal>)
 */

func (mss *MyServerSession) DoLoopTemp() {
	for {
		m, err := mss.session.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return
			}
			mss.session.Logf(slogger.WARN, "error reading message: %s", err)
			return
		}
		err, fatal := mss.handleMessage(m)
		if err == nil && fatal {
			panic(fmt.Errorf("should be impossible, no error but fatal"))
		}
		if err != nil {
			err = mss.session.RespondWithError(m, err)
			if err != nil {
				mss.session.Logf(slogger.WARN, "error writing error: %s", err)
				return
			}
			if fatal {
				return
			}
		}
	}
}

func (mss *MyServerSession) Close() {
}

type MyServerTestFactory struct {
}

func (sf *MyServerTestFactory) CreateWorker(session *mongonet.Session) (mongonet.ServerWorker, error) {
	return &MyServerSession{&BaseServerSession{session, map[string][]bson.D{}}}, nil
}

func (sf *MyServerTestFactory) GetConnection(conn *mongonet.Conn) io.ReadWriteCloser {
	return conn
}

func TestServer(t *testing.T) {
	port := 9919 // TODO: pick randomly or check?
	syncTlsConfig := mongonet.NewSyncTlsConfig()
	server := mongonet.NewServer(
		mongonet.ServerConfig{
			"127.0.0.1",
			port,
			false,
			nil,
			syncTlsConfig,
			0,
			0,
			nil,
			nil,
			slogger.DEBUG,
			[]slogger.Appender{slogger.StdOutAppender()},
		},
		&MyServerTestFactory{},
	)

	go server.Run()

	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d", port))
	client, err := mongo.NewClient(opts)
	if err != nil {
		t.Errorf("cannot create a mongo client. err: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		t.Errorf("cannot connect to server. err: %v", err)
		return
	}
	defer client.Disconnect(ctx)
	coll := client.Database("test").Collection("bar")
	docIn := bson.D{{"foo", int32(17)}}
	if _, err = coll.InsertOne(ctx, docIn); err != nil {
		t.Errorf("can't insert: %v", err)
		return
	}

	docOut := bson.D{}
	err = coll.FindOne(ctx, bson.D{}).Decode(&docOut)
	if err != nil {
		t.Errorf("can't find: %v", err)
		return
	}

	if len(docIn) != len(docOut) {
		t.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
		return
	}

	if diff := deep.Equal(docIn[0], docOut[0]); diff != nil {
		t.Errorf("docs don't match: %v", diff)
		return
	}

}

func TestServerWorkerWithContext(t *testing.T) {
	port := 9921

	var sessCtr int32
	syncTlsConfig := mongonet.NewSyncTlsConfig()
	server := mongonet.NewServer(
		mongonet.ServerConfig{
			"127.0.0.1",
			port,
			false,
			nil,
			syncTlsConfig,
			0,
			0,
			nil,
			slogger.DEBUG,
			[]slogger.Appender{slogger.StdOutAppender()},
		},
		&TestFactoryWithContext{&sessCtr},
	)

	go server.Run()

	if err := <-server.InitChannel(); err != nil {
		t.Error(err)
	}

	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d", port))
	for i := 0; i < 10; i++ {
		if err := checkClient(opts); err != nil {
			t.Error(err)
		}
	}
	sessCtrCurr := atomic.LoadInt32(&sessCtr)

	if sessCtrCurr != int32(30) {
		t.Errorf("expect session counter to be 30 but got %d", sessCtrCurr)
	}

	server.Close()

	sessCtrFinal := atomic.LoadInt32(&sessCtr)
	if sessCtrFinal != int32(0) {
		t.Errorf("expect session counter to be 0 but got %d", sessCtrFinal)
	}
}

// ---------------------------------------------------------------------------------------------------------------
// Testing for server with contextualWorkerFactory

type TestFactoryWithContext struct {
	counter *int32
}

func (sf *TestFactoryWithContext) CreateWorkerWithContext(session *mongonet.Session, ctx *context.Context) (mongonet.ServerWorker, error) {
	return &TestSessionWithContext{&BaseServerSession{session, map[string][]bson.D{}}, ctx, sf.counter}, nil
}

func (sf *TestFactoryWithContext) CreateWorker(session *mongonet.Session) (mongonet.ServerWorker, error) {
	return nil, fmt.Errorf("create worker not allowed with contextual worker factory")
}

func (sf *TestFactoryWithContext) GetConnection(conn *mongonet.Conn) io.ReadWriteCloser {
	return conn
}

type TestSessionWithContext struct {
	*BaseServerSession
	ctx     *context.Context
	counter *int32
}

func (tsc *TestSessionWithContext) DoLoopTemp() {
	atomic.AddInt32(tsc.counter, 1)
	ctx := *tsc.ctx

	for {
		m, err := tsc.session.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return
			}
			tsc.session.Logf(slogger.WARN, "error reading message: %s", err)
			return
		}

		err, fatal := tsc.handleMessage(m)
		if err == nil && fatal {
			panic(fmt.Errorf("should be impossible, no error but fatal"))
		}
		if err != nil {
			err = tsc.session.RespondWithError(m, err)
			if err != nil {
				tsc.session.Logf(slogger.WARN, "error writing error: %s", err)
				return
			}
			if fatal {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		default:
			continue
		}
	}
}
func (tsc *TestSessionWithContext) Close() {
	time.Sleep(500 * time.Millisecond)
	atomic.AddInt32(tsc.counter, -1)
}

func checkClient(opts *options.ClientOptions) error {
	client, err := mongo.NewClient(opts)
	if err != nil {
		return fmt.Errorf("cannot create a mongo client. err: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	coll := client.Database("test").Collection("test")
	docIn := bson.D{{"foo", 17}}
	if _, err = coll.InsertOne(ctx, docIn); err != nil {
		return err
	}
	return client.Disconnect(ctx)
}

func generateCertificateKeyPair(subject string, ca *x509.Certificate, authorityKey interface{}, hostname string, notAfter time.Time, serial int64) (*x509.Certificate, interface{}, error) {
	myKey, errRSA := rsa.GenerateKey(rand.Reader, 2048)
	if errRSA != nil {
		return nil, nil, errRSA
	}

	if authorityKey == nil {
		key, errRSA := rsa.GenerateKey(rand.Reader, 2048)
		if errRSA != nil {
			return nil, nil, errRSA
		}
		authorityKey = key
		myKey = key
	}

	pubKeySlice := append(myKey.PublicKey.N.Bytes(), big.NewInt(int64(myKey.PublicKey.E)).Bytes()...)
	pubKeyHash := sha512.Sum512(pubKeySlice)

	var ou []string

	name := pkix.Name{
		Country: []string{"US"}, Organization: []string{"MongoDB"},
		OrganizationalUnit: ou, Locality: []string{"NewYorkCity"},
		Province: []string{"NewYork"}, CommonName: subject}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      name,
		//Let's hope the tests can run in this window
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		IsCA:                  ca == nil,
		//MaxPathLen: -1,
		SubjectKeyId:                pubKeyHash[0:],
		DNSNames:                    []string{hostname},
		PermittedDNSDomainsCritical: false,
		SignatureAlgorithm:          x509.SHA512WithRSA,
	}

	// Update SAN fields using IPAddresses if hostname is really an IP Address
	ip := net.ParseIP(hostname)
	if ip != nil {
		template.IPAddresses = []net.IP{ip}
	}

	serial += 1

	if ca == nil {
		template.KeyUsage |= x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign
		ca = template
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

	certDER, errCert := x509.CreateCertificate(rand.Reader, template, ca, &myKey.PublicKey, authorityKey)
	if errCert != nil {
		return nil, nil, errCert
	}
	cert, errParse := x509.ParseCertificate(certDER)
	if errParse != nil {
		return nil, nil, errParse
	}
	return cert, myKey, nil
}

func generateCertificateAuthorityPair(hostname string, notAfter time.Time) (*x509.Certificate, interface{}, error) {
	return generateCertificateKeyPair("test CA", nil, nil, hostname, notAfter, 1)
}

func getTestCerts() (*x509.Certificate, string, error) {
	expiration := time.Now().Add(time.Hour * 24)
	hostname, err := os.Hostname()
	if err != nil {
		return nil, "", err
	}
	ca, cakey, err := generateCertificateAuthorityPair(hostname, expiration)
	if err != nil {
		return nil, "", err
	}
	cert, key, err := generateCertificateKeyPair("test cert", ca, cakey, hostname, expiration, 2)
	if err != nil {
		return nil, "", err
	}

	return ca, hostname, writeCertificatePairToDisk(cert, key, "cert.pem")
}

func encodeCertificatePairAsPEM(certificate *x509.Certificate, privateKey interface{}) (certPEM, keyPEM []byte) {
	if certificate != nil {
		block := &pem.Block{Type: "CERTIFICATE",
			Bytes: certificate.Raw}
		certPEM = pem.EncodeToMemory(block)
	}
	if privateKey != nil {
		block := &pem.Block{Type: "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey.(*rsa.PrivateKey))}
		keyPEM = pem.EncodeToMemory(block)
	}
	return
}

func writeCertificatePairToDisk(certificate *x509.Certificate, privateKey interface{}, filePath string) error {
	certPEM, keyPEM := encodeCertificatePairAsPEM(certificate, privateKey)
	data := make([]byte, 0, len(certPEM)+len(keyPEM))
	data = append(data, certPEM...)
	data = append(data, keyPEM...)
	return ioutil.WriteFile(filePath, data, 0666)
}

func TestServerWithTLS(t *testing.T) {
	port := 9922 // TODO: pick randomly or check?
	syncTlsConfig := mongonet.NewSyncTlsConfig()

	// generate keys
	ca, hostname, err := getTestCerts()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("cert.pem")

	sslPair := []mongonet.SSLPair{{
		Cert: "cert.pem",
		Key:  "cert.pem",
		Id:   "default",
	}}

	server := mongonet.NewServer(
		mongonet.ServerConfig{
			hostname,
			port,
			true,
			nil,
			syncTlsConfig,
			0,
			0,
			nil,
			nil,
			slogger.DEBUG,
			[]slogger.Appender{slogger.StdOutAppender()},
		},
		&MyServerTestFactory{},
	)

	ok, _, errs := syncTlsConfig.SetTlsConfig(nil, nil, tls.VersionTLS12, sslPair)
	if !ok {
		t.Fatal(errs)
	}

	go server.Run()

	certPool := x509.NewCertPool()
	certPool.AddCert(ca)
	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: true,
	}
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", hostname, port)).SetTLSConfig(tlsConfig).SetDirect(true)
	client, err := mongo.NewClient(opts)
	if err != nil {
		t.Errorf("cannot create a mongo client. err: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		t.Errorf("cannot connect to server. err: %v", err)
		return
	}
	defer client.Disconnect(ctx)
	coll := client.Database("test").Collection("bar")
	docIn := bson.D{{"foo", int32(17)}}
	if _, err = coll.InsertOne(ctx, docIn); err != nil {
		t.Errorf("can't insert: %v", err)
		return
	}

	docOut := bson.D{}
	err = coll.FindOne(ctx, bson.D{}).Decode(&docOut)
	if err != nil {
		t.Errorf("can't find: %v", err)
		return
	}

	if len(docIn) != len(docOut) {
		t.Errorf("docs don't match\n %v\n %v\n", docIn, docOut)
		return
	}

	if diff := deep.Equal(docIn[0], docOut[0]); diff != nil {
		t.Errorf("docs don't match: %v", diff)
		return
	}

}
