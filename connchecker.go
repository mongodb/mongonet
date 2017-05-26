package mongonet

import (
	"crypto/tls"
	"net"
	"time"
)

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

		// TLS connections are corrupted if there is a timeout.
		// Therefore, always return.
		if _, ok := c.conn.(*tls.Conn); ok {
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

		// TLS connections are corrupted if there is a timeout.
		// Therefore, always return.
		if _, ok := c.conn.(*tls.Conn); ok {
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
