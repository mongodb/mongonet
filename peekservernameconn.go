package mongonet

import (
	"golang.org/x/crypto/cryptobyte"

	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
)

type PeekableConn struct {
	bufReader *bufio.Reader
	net.Conn
}

func NewPeekableConn(conn net.Conn, bufSize int) *PeekableConn {
	return &PeekableConn{
		bufio.NewReaderSize(conn, bufSize),
		conn,
	}
}

func (pc *PeekableConn) Read(p []byte) (n int, err error) {
	return pc.bufReader.Read(p)
}

func (pc *PeekableConn) Peek(n int) ([]byte, error) {
	return pc.bufReader.Peek(n)
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

// TIM TODO: grow a buffer on demand instead
const peekServerNameConnBufferSize = 512 * 1024 * 1024

const (
	// from RFC 8446
	typeHandshakeMsgClientHello = 1
	typeExtensionServerName     = 0

	// from RFC 6066
	typeServerNameHostName = 0
)

// PeekServerNameConn emulates a tls.ProxyProtoConn but provides a way to get at the SNI hostname
// without doing a full (and computionally expensive) handshake

type PeekServerNameConn struct {
	pc                   *PeekableConn
	sniHostname          string
	sniHostnameAttempted bool
	*tls.Conn
}

func NewPeekServerNameConn(conn net.Conn, tlsConfig *tls.Config) *PeekServerNameConn {
	pc := NewPeekableConn(conn, peekServerNameConnBufferSize)
	return &PeekServerNameConn{
		pc,
		"",
		false,
		tls.Server(pc, tlsConfig),
	}
}

func (psc *PeekServerNameConn) Handshake() error {
	psc.sniHostnameAttempted = true
	err := psc.Conn.Handshake()
	if err == nil && psc.sniHostname == "" {
		psc.sniHostname = psc.Conn.ConnectionState().ServerName
	}
	return err
}

func (psc *PeekServerNameConn) Read(p []byte) (n int, err error) {
	psc.sniHostnameAttempted = true
	return psc.Conn.Read(p)
}

func (psc *PeekServerNameConn) ServerName() (string, error) {
	if !psc.sniHostnameAttempted {
		psc.sniHostnameAttempted = true
		serverName, err := readServerNameFromClientHello(psc.pc)
		if err != nil {
			return "", err
		}
		psc.sniHostname = serverName
	}

	return psc.sniHostname, nil
}

type Peeker interface {
	Peek(n int) ([]byte, error)
}

func readServerNameFromClientHello(peeker Peeker) (string, error) {
	// peek the handshake message type and length of message
	bites, err := peeker.Peek(4)
	if err != nil {
		return "", err
	}
	if bites[0] != typeHandshakeMsgClientHello {
		return "", fmt.Errorf("Expected CLIENT HELLO but handshake message type is %v", bites[0])
	}

	// message length is in big endian
	msgLen := int(bites[1])*256*256 + int(bites[2])*256 + int(bites[3])
	if msgLen > (peekServerNameConnBufferSize - 4) {
		return "", fmt.Errorf("CLIENT HELLO is too large: %v byte", msgLen)
	}

	// repeek to get whole message
	bites, err = peeker.Peek(msgLen + 4)
	if err != nil {
		return "", err
	}

	cbs := cryptobyte.String(bites[4:])

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

	// ProtocolVersion legacy_version = 0x0303
	if !cbs.Skip(4) {
		return "", fmt.Errorf("Failed to skip ProtocolVersion")
	}

	//Random random
	if !cbs.Skip(32) {
		return "", fmt.Errorf("Failed to skip Random")
	}

	//opaque legacy_session_id<0..32>
	var opaqueStr cryptobyte.String
	if !cbs.ReadUint8LengthPrefixed(&opaqueStr) {
		return "", fmt.Errorf("Failed to skip legacy_session_id")
	}

	//CipherSuite cipher_suites<2..2^16-2>;
	if !cbs.ReadUint16LengthPrefixed(&opaqueStr) {
		return "", fmt.Errorf("Failed to skip cipher_suites")
	}

	// Extension extensions<8..2^16-1>
	var extensionsStr cryptobyte.String
	if !cbs.ReadUint16LengthPrefixed(&extensionsStr) {
		return "", fmt.Errorf("Failed to read in extensions")
	}
	if !cbs.Empty() {
		return "", fmt.Errorf("Expected to finish reading CLIENT HELLO")
	}

	for !extensionsStr.Empty() {
		var extensionType uint16
		if !extensionsStr.ReadUint16(&extensionType) {
			return "", fmt.Errorf("Failed to read extension type")
		}
		var extDataStr cryptobyte.String
		if !extensionsStr.ReadUint16LengthPrefixed(&extDataStr) {
			return "", fmt.Errorf("Failed to read extension data for extension type %v", extensionType)
		}
		if extensionType != typeExtensionServerName {
			continue
		}

		var nameListStr cryptobyte.String
		if !extDataStr.ReadUint16LengthPrefixed(&nameListStr) {
			return "", fmt.Errorf("Failed to read Server Name list")
		}

		if nameListStr.Empty() {
			return "", fmt.Errorf("Server name list is empty")
		}

		for !nameListStr.Empty() {
			var nameType uint8
			var serverNameStr cryptobyte.String
			if !nameListStr.ReadUint8(&nameType) {
				return "", fmt.Errorf("Failed to read name type")
			}
			if !nameListStr.ReadUint16LengthPrefixed(&serverNameStr) {
				return "", fmt.Errorf("Failed to read Server Name")
			}
			if serverNameStr.Empty() {
				return "", fmt.Errorf("Server Name is empty")
			}
			if nameType != typeServerNameHostName {
				return "", fmt.Errorf("Server Name is not a hostname")
			}

			serverName := string(serverNameStr)

			// RFC 6066 disallows trailing dots
			if strings.HasSuffix(serverName, ".") {
				return "", fmt.Errorf("Server Name should not have a trailing dot")
			}

			return serverName, nil
		}
	}

	// empty string means no server name found
	return "", nil
}
