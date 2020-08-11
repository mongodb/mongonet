package main

import (
	"flag"
	"fmt"

	"github.com/mongodb/mongonet"
)

func main() {

	bindHost := flag.String("host", "host.local.10gen.cc", "what to bind to")
	bindPort := flag.Int("port", 9999, "what to bind to")
	mongoHost := flag.String("mongoHost", "host1.local.10gen.cc", "host mongo is on")
	mongoPort := flag.Int("mongoPort", 27017, "port mongo is on")

	flag.Parse()

	pc := mongonet.NewProxyConfig(*bindHost, *bindPort, *mongoHost, *mongoPort)
	pc.MongoSSLSkipVerify = true

	proxy, err := mongonet.NewProxy(pc)
	if err != nil {
		fmt.Printf("Error making new proxy: %v\n", err)
		return
	}

	proxy.InitializeServer()
	proxy.OnSSLConfig(nil)

	err = proxy.Run()
	if err != nil {
		fmt.Printf("Error running proxy: %v\n", err)
		return
	}
}
