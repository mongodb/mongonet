package mongonet

import "net"

type EarlyAccessChecker interface {
	PreClientHelloCheck(remoteAddr net.Addr) error

	// data can be used for data gathered during PostClientHelloCheck for use later
	// It is opaque to mongonet but mongonet will hand it back to the using code later on
	PostClientHelloCheck(serverName string, remoteAddr net.Addr) (data interface{}, err error)
}
