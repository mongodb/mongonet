package mongonet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestProxyProtocol(t *testing.T) {

	var (
		v2SignatureBytes = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

		// version/command.
		localBytes = []byte{0x20}
		proxyBytes = []byte{0x21}

		// address family and protocol
		tcpV4Bytes = []byte{0x11}
		udpV4Bytes = []byte{0x12}
		tcpV6Bytes = []byte{0x21}
		udpV6Bytes = []byte{0x22}

		// addresses
		localIPv4                 = net.ParseIP("127.0.0.1").To4()
		localIPv6                 = net.ParseIP("::1").To16()
		remoteIPv4                = net.ParseIP("127.0.0.2").To4()
		remoteIPv6                = net.ParseIP("::2").To16()
		portBytes                 = []byte{0xFD, 0xE8, 0xFD, 0xE9}
		v4AddressesBytes          = append(localIPv4, remoteIPv4...)
		v6AddressesBytes          = append(localIPv6, remoteIPv6...)
		v4AddressesWithPortsBytes = append(v4AddressesBytes, portBytes...)
		v6AddressesWithPortsBytes = append(v6AddressesBytes, portBytes...)

		// lengths
		lengthV4          = uint16(12)
		lengthV6          = uint16(36)
		lengthPadded      = uint16(84)
		lengthV4Bytes     = []byte{0x00, 0xC}
		lengthV6Bytes     = []byte{0x00, 0x24}
		lengthEmptyBytes  = []byte{0x00, 0x00}
		lengthPaddedBytes = []byte{0x00, 0x54}

		// message
		messageBytes = []byte("MESSAGE")

		// util
		concat = func(parts ...[]byte) []byte {
			var result []byte
			for _, p := range parts {
				result = append(result, p...)
			}
			return result
		}
	)

	testCases := []struct {
		name               string
		bytes              []byte
		expectedLocalAddr  string
		expectedProxyAddr  string
		expectedRemoteAddr string
		expectedTargetAddr string
		expectedErr        error
	}{
		{
			"not proxy protocol",
			messageBytes,
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v1-invalid header 1",
			[]byte("PROXY"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
		},
		{
			"v1-invalid header 2",
			[]byte("PROXY \r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
		},
		{
			"v1-invalid header 2",
			[]byte("PROXY UDP4 127.0.0.1 127.0.0.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid protocol and family"),
		},
		{
			"v1-invalid src ip address",
			[]byte("PROXY TCP4 127.0 127.0.0.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid ip address"),
		},
		{
			"v1-invalid dst ip address",
			[]byte("PROXY TCP4 127.0.0.1 127.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid ip address"),
		},
		{
			"v1-invalid src port",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 70000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid port number"),
		},
		{
			"v1-invalid dst port",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 65000 abd\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid port number"),
		},
		{
			"v1-unknown",
			[]byte("PROXY UNKNOWN 127.0.0.1 127.0.0.2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v1-unknown",
			[]byte("PROXY UNKNOWN\r\nMESSAGE"),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v1-tcp4",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
		},
		{
			"v1-tcp6",
			[]byte("PROXY TCP6 ::1 ::2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
		},
		{
			"v2-invalid version",
			concat(v2SignatureBytes, []byte{0x10}, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid version"),
		},
		{
			"v2-invalid command",
			concat(v2SignatureBytes, []byte{0x22}, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid command"),
		},
		{
			"v2-invalid address family",
			concat(v2SignatureBytes, proxyBytes, []byte{0x41}, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid address family"),
		},
		{
			"v2-invalid protocol",
			concat(v2SignatureBytes, proxyBytes, []byte{0x23}, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid protocol"),
		},
		{
			"v2-invalid ipv4 length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, []byte{0x00, 0xB}, v4AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid IPv4 payload"),
		},
		{
			"v2-invalid ipv6 length",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, []byte{0x00, 0xC}, v4AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid IPv6 payload"),
		},
		{
			"v2-local",
			concat(v2SignatureBytes, localBytes, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v2-local-tcp-ipv4 with extra bytes",
			concat(v2SignatureBytes, localBytes, tcpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v2-local-tcp-ipv6 with extra bytes",
			concat(v2SignatureBytes, localBytes, tcpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
		},
		{
			"v2-proxy-tcp-ipv4",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4Bytes, v4AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
		},
		{
			"v2-proxy-tcp-ipv4 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
		},
		{
			"v2-proxy-tcp-ipv6",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
		},
		{
			"v2-proxy-tcp-ipv6 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
		},
		{
			"v2-proxy-udp-ipv4",
			concat(v2SignatureBytes, proxyBytes, udpV4Bytes, lengthV4Bytes, v4AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
		},
		{
			"v2-proxy-udp-ipv4 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, udpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
		},
		{
			"v2-proxy-udp-ipv6",
			concat(v2SignatureBytes, proxyBytes, udpV6Bytes, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
		},
		{
			"v2-proxy-udp-ipv6 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, udpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
		},
		{
			"unix sockets",
			[]byte("\r\n\r\n\x00\r\nQUIT\n!0\x00\x00"),
			"",
			"",
			"",
			"",
			errors.New("unix sockets are not supported"),
		},
		{
			"not enough data",
			[]byte("PROXY\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newMockConn(tc.bytes)
			subject, err := NewConn(c)
			if err != nil && tc.expectedErr == nil {
				t.Fatalf("expected no error, but got %v", err)
			} else if err == nil && tc.expectedErr != nil {
				t.Fatalf("expected error %v, but got none", tc.expectedErr)
			} else if err != nil && tc.expectedErr != nil {
				if err.Error() != tc.expectedErr.Error() {
					t.Fatalf("expected error %v, but got %v", tc.expectedErr, err)
				}
				return // we are successful
			}

			if tc.expectedLocalAddr != subject.LocalAddr().String() {
				t.Fatalf("expected local address %v, but got %v", tc.expectedLocalAddr, subject.LocalAddr())
			}

			if subject.IsProxied() && tc.expectedProxyAddr != subject.ProxyAddr().String() {
				t.Fatalf("expected proxy address %v, but got %v", tc.expectedProxyAddr, subject.ProxyAddr())
			}

			if tc.expectedRemoteAddr != subject.RemoteAddr().String() {
				t.Fatalf("expected remote address %v, but got %v", tc.expectedRemoteAddr, subject.RemoteAddr())
			}

			if subject.IsProxied() && tc.expectedTargetAddr != subject.TargetAddr().String() {
				t.Fatalf("expected target address %v, but got %v", tc.expectedTargetAddr, subject.TargetAddr())
			}

			actualMessageBytes := make([]byte, len(messageBytes))
			_, err = io.ReadFull(subject, actualMessageBytes)
			if err != nil {
				t.Fatalf("expected no error, but got %v", err)
			}
			if !bytes.Equal(messageBytes, actualMessageBytes) {
				t.Fatalf("expected message %v, but got %v", messageBytes, actualMessageBytes)
			}
		})
	}
}

func newMockConn(b []byte) *mockConn {
	return &mockConn{
		r: bytes.NewReader(b),
	}
}

type mockConn struct {
	r *bytes.Reader
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Read(b []byte) (n int, err error) {
	return c.r.Read(b)
}

func (c *mockConn) Write(b []byte) (n int, err error) {
	panic("don't call me")
}

func (c *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP("192.168.0.2"),
		Port: 5001,
	}
}

func (c *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP("192.168.0.1"),
		Port: 5000,
	}
}

func (c *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
