package inttests

import (
	"context"
	"testing"

	"github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCommonRunCommandUsingRawBSON(t *testing.T) {
	mongoPort, _, hostname := util.GetTestHostAndPorts()
	goctx := context.Background()
	client, err := util.GetTestClient(hostname, mongoPort, util.Direct, false, "mongoapi_tester", goctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(goctx)

	cmd := bson.D{{"ping", 1}, {"$db", "admin"}}
	response, err := mongonet.RunCommandUsingRawBSON(cmd, client, goctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := response.Map()["ok"]
	if !ok {
		t.Fatalf("expected to find 'ok' in response %+v", raw)
	}
	v, ok := raw.(float64)
	if !ok {
		t.Fatalf("expected to that 'ok' is int but got %T", raw)
	}
	if v != 1 {
		t.Fatalf("expected to get ok:1 but got %v", v)
	}

}
