package mongonet

import "net"
import "time"
import "crypto/tls"

type ConnChecker interface {
	CheckConnection() error
	CheckConnectionInterval() time.Duration // set to 0 to not check
}

type CheckedConn struct {
	conn    net.Conn
	checker ConnChecker
}

func (c CheckedConn) Read(b []byte) (n int, err error) {
	for {
		interval := c.checker.CheckConnectionInterval()
		if interval > 0 {
			if err := c.checker.CheckConnection(); err != nil {
				return n, err
			}

			deadline := time.Now().Add(interval)
			if err = c.conn.SetReadDeadline(deadline); err != nil {
				return n, err
			}
		}

		nDelta, err := c.conn.Read(b[n:])
		n += nDelta

		// If a timeout occurs, the TLS connection will be corrupted, and all future writes
		// will return the same error. (https://golang.org/pkg/crypto/tls/#Conn.SetDeadline)
		// Therefore, always return.
		if isTlsConn(c.conn) {
			return n, err
		}
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			return n, err
		}
	}
}

func (c CheckedConn) Write(b []byte) (n int, err error) {
	for {
		interval := c.checker.CheckConnectionInterval()
		if interval > 0 {
			if err := c.checker.CheckConnection(); err != nil {
				return n, err
			}

			deadline := time.Now().Add(interval)
			if err = c.conn.SetWriteDeadline(deadline); err != nil {
				return n, err
			}
		}

		nDelta, err := c.conn.Write(b[n:])
		n += nDelta

		// If a timeout occurs, the TLS connection will be corrupted, and all future writes
		// will return the same error. (https://golang.org/pkg/crypto/tls/#Conn.SetDeadline)
		// Therefore, always return.
		if isTlsConn(c.conn) {
			return n, err
		}
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			return n, err
		}
	}
}

func (c CheckedConn) Close() error {
	return c.conn.Close()
}

func isTlsConn(conn net.Conn) bool {
	switch c := conn.(type) {

	case *tls.Conn:
		return true

	case *Conn:
		return isTlsConn(c.wrapped)

	default:
		return false
	}
}
