package inttests

import (
	"context"
	"os/exec"
	"testing"

	. "github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
)

/*
	This test requires a HAProxy running and forwarding TCP traffic to the proxy (using haproxy.conf)
	connection reset by peer errors might appear due to HAProxy health checks
*/

func runShellCommand(cmd string, t *testing.T, goctx context.Context, args ...string) error {
	cmdObj := exec.CommandContext(goctx, cmd, args...)
	out, err := cmdObj.CombinedOutput()
	t.Logf("cmd %s %v out: %v. err=%v", cmd, args, string(out), err)
	return err
}

func TestCommonProxyProtocolInt(t *testing.T) {
	goctx := context.Background()
	if err := runShellCommand("haproxy", t, goctx, "-f", "mongonethaproxy.conf"); err != nil {
		panic(err)
	}
	defer runShellCommand("pkill", t, goctx, "-9", "-f", "mongonethaproxy.conf")

	type mongonetIsConnectionProxiedResponse struct {
		Proxied bool `bson:"proxied"`
	}
	mongoPort, _, hostname := util.GetTestHostAndPorts()
	proxyPort := 9917 // fixed for HAProxy config
	haProxyPortV1 := 9915
	haProxyPortV2 := 9916
	pc := getProxyConfig(hostname, mongoPort, proxyPort, DefaultMaxPoolSize, DefaultMaxPoolIdleTimeSec, util.Cluster, false, nil)
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}
	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}
	go proxy.Run()

	check := func(name string, port int, expected bool) {
		proxyClient, err := util.GetTestClient(hostname, port, util.Cluster, false, "testProxy1", goctx)
		if err != nil {
			t.Fatalf("%s:%v", name, err)
		}
		defer proxyClient.Disconnect(goctx)
		var response mongonetIsConnectionProxiedResponse
		res := proxyClient.Database("admin").RunCommand(goctx, bson.D{{"mongonetIsConnectionProxied", ""}})
		if res.Err() != nil {
			t.Fatalf("%s:%v", name, res.Err())
		}
		if err := res.Decode(&response); err != nil {
			t.Fatalf("%s:%v", name, err)
		}
		if expected != response.Proxied {
			t.Fatalf("%s:expected proxied=%v but got %v", name, expected, response.Proxied)
		}
		t.Logf("got proxied=%v for %s", response.Proxied, name)
	}
	check("direct", proxyPort, false)
	check("haProxyPortV1", haProxyPortV1, true)
	check("haProxyPortV2", haProxyPortV2, true)
}
