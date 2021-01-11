package util

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	ClientTimeoutSecForTests    = 20 * time.Second
	RemoteDbNameForTests        = "testRemote"
	RetryOnRemoteDbNameForTests = "testRetryOnRemote"
	RetryOnRemoteVal            = 10
	RemoteRsName                = "proxytest2"
)

type ClientFactoryFunc func(host string, port int, mode MongoConnectionMode, secondaryReads bool, appName string, ctx context.Context) (*mongo.Client, error)

func EphemeralPort() (int, error) {
	fd, err := syscall.Socket(
		syscall.AF_INET,
		syscall.SOCK_STREAM,
		syscall.IPPROTO_TCP,
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to obtain socket. err=%v", err)
	}

	defer syscall.Close(fd)

	err = syscall.Bind(
		fd,
		&syscall.SockaddrInet4{
			Port: 0,                   // 0 requests that the kernel assign an ephemeral port
			Addr: [4]byte{0, 0, 0, 0}, // 0.0.0.0 means any IP address
		},
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to bind socket. err=%v", err)
	}

	var sockaddr syscall.Sockaddr
	sockaddr, err = syscall.Getsockname(fd)
	if err != nil {
		return -1, fmt.Errorf("Failed to Getsockname() for fd %v. err=%v", fd, err)
	}

	sockaddr4, ok := sockaddr.(*syscall.SockaddrInet4)
	if !ok {
		return -1, fmt.Errorf("Expected *syscall.SockaddrInet4 from Getsockname(), but got %#v", sockaddr)
	}

	return sockaddr4.Port, nil
}

func GetTestHostAndPorts() (mongoPort, proxyPort int, hostname string) {
	var err error
	mongoPort = 30000
	proxyPort, err = EphemeralPort()
	if err != nil {
		panic(err)
	}
	if os.Getenv("MONGO_PORT") != "" {
		mongoPort, _ = strconv.Atoi(os.Getenv("MONGO_PORT"))
	}
	hostname = "localhost"
	return
}

func GetTestClient(host string, port int, mode MongoConnectionMode, secondaryReads bool, appName string, ctx context.Context) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", host, port)).
		SetDirect(mode == Direct)
	if secondaryReads {
		opts.SetReadPreference(readpref.Secondary())
	}
	if appName != "" {
		opts.SetAppName(appName)
	}
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("cannot create a mongo client. err: %v", err)
	}
	err = client.Connect(ctx)
	return client, err
}

func DisableFailPoint(client *mongo.Client, ctx context.Context) error {
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "off"},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func EnableFailPointCloseConnection(mongoPort int) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	client, err := GetTestClient("localhost", mongoPort, Direct, false, "enableFailPointCloseConnection", ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", []string{"update"}},
			{"closeConnection", true},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func EnableFailPointErrorCode(mongoPort, errorCode int) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	client, err := GetTestClient("localhost", mongoPort, Direct, false, "enableFailPointErrorCode", ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", []string{"update"}},
			{"errorCode", errorCode},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}

func EnableFailPointForCommand(mongoPort int, failCommands interface{}, errorCode int, blockTimeMs int) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSecForTests)
	defer cancelFunc()
	client, err := GetTestClient("localhost", mongoPort, Direct, false, "enableFailPointErrorCode", ctx)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	cmd := bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "alwaysOn"},
		{"data", bson.D{
			{"failCommands", failCommands},
			{"errorCode", errorCode},
			{"blockTimeMS", blockTimeMs},
		}},
	}
	return client.Database("admin").RunCommand(ctx, cmd).Err()
}
