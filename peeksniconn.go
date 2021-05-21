package mongonet

import (
	"bufio"
	"crypto/tls"
	"net"
)

// PeekSniConn emulates a tls.Conn but provides a way to get at the SNI hostname
// without doing a full (and computionally expensive) handshake

type PeekSniConn struct {
	innerConn   net.Conn
	buffer      *bufio.Reader
	sniHostname string
	*tls.Conn
}

// Buffer size:
//  We want a buffer large enough to hold a client hello
// Here is the Client Hello from TLS 1.3:
//
//uint16 ProtocolVersion;
//opaque Random[32];
//
//uint8 CipherSuite[2];    /* Cryptographic suite selector */
//
//struct {
//ProtocolVersion legacy_version = 0x0303;    /* TLS v1.2 */
//Random random;
//opaque legacy_session_id<0..32>;
//CipherSuite cipher_suites<2..2^16-2>;
//opaque legacy_compression_methods<1..2^8-1>;
//Extension extensions<8..2^16-1>;
//} ClientHello;
//
// in bytes that's approx 2 + 32 + 32 + 64k + 256 + 64k
// so a little more than 128k
// so we can do a half megabyte to be safe

const peekSniConnBufferSize = 512 * 1024 * 1024

func NewPeekSniConn(conn net.Conn, tlsConfig *tls.Config) *PeekSniConn {

}
