package mongonet

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	v1Signature = []byte{0x50, 0x52, 0x4F, 0x58, 0x59}
	v2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
)

type Conn struct {
	wrapped net.Conn
	rbuf    *bufio.Reader

	version byte

	proxy  net.Addr
	remote net.Addr
	target net.Addr
}

func NewConn(wrapped net.Conn) (*Conn, error) {
	c := &Conn{
		wrapped: wrapped,
		rbuf:    bufio.NewReader(wrapped),
	}

	if err := c.init(); err != nil {
		return nil, err
	}

	return c, nil
}

// Close implements the io.Closer interface.
func (c *Conn) Close() error {
	return c.wrapped.Close()
}

// IsProxied returns true if the PROXY PROTOCOL was used.
func (c *Conn) IsProxied() bool {
	return c.proxy != nil
}

// LocalAddr implements the net.Conn interface.
func (c *Conn) LocalAddr() net.Addr {
	return c.wrapped.LocalAddr()
}

// ProxyAddr returns the proxy server's addr if IsProxied() is true; otherwise nil.
func (c *Conn) ProxyAddr() net.Addr {
	return c.proxy
}

// Read implements the io.Reader interface.
func (c *Conn) Read(p []byte) (int, error) {
	return c.rbuf.Read(p)
}

// RemoteAddr implements the net.Conn interface.
func (c *Conn) RemoteAddr() net.Addr {
	return c.remote
}

// TargetAddr returns the client's intended target addr if IsProxied() is true; otherwise nil.
func (c *Conn) TargetAddr() net.Addr {
	return c.target
}

// SetDeadline implements the net.Conn interface.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.wrapped.SetDeadline(t)
}

// SetReadDeadline implements the net.Conn interface.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.wrapped.SetReadDeadline(t)
}

// SetWriteDeadline implements the net.Conn interface.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.wrapped.SetWriteDeadline(t)
}

// Version returns the version of the PROXY PROTOCOL.
func (c *Conn) Version() byte {
	return c.version
}

// Write implements the io.Writer interface.
func (c *Conn) Write(p []byte) (int, error) {
	return c.wrapped.Write(p)
}

func (c *Conn) init() error {
	c.remote = c.wrapped.RemoteAddr()
	// check if the first byte is one of our recognized signatures
	if b1, err := c.rbuf.Peek(1); err == nil && b1[0] == v1Signature[0] || b1[0] == v2Signature[0] {
		if sig, err := c.rbuf.Peek(5); err == nil && bytes.Equal(sig, v1Signature) {
			return c.initv1()
		}
		if sig, err := c.rbuf.Peek(12); err == nil && bytes.Equal(sig, v2Signature) {
			return c.initv2()
		}
	}

	// not a proxy connection
	return nil
}

func (c *Conn) initv1() error {
	c.version = 1
	line, _ := c.rbuf.ReadString('\n')
	if !strings.HasSuffix(line, "\r\n") {
		return errors.New("invalid header")
	}

	parts := strings.Split(line[:len(line)-2], " ")
	if len(parts) < 6 {
		if len(parts) > 1 && parts[1] == "UNKNOWN" {
			return nil
		}

		return errors.New("invalid header")
	}

	var srcIP net.IP
	var dstIP net.IP
	switch parts[1] {
	case "TCP4":

		if srcIP = net.ParseIP(parts[2]).To4(); srcIP == nil {
			return errors.New("invalid ip address")
		}
		if dstIP = net.ParseIP(parts[3]).To4(); dstIP == nil {
			return errors.New("invalid ip address")
		}

	case "TCP6":

		if srcIP = net.ParseIP(parts[2]).To16(); srcIP == nil {
			return errors.New("invalid ip address")
		}
		if dstIP = net.ParseIP(parts[3]).To16(); dstIP == nil {
			return errors.New("invalid ip address")
		}
	case "UNKNOWN":
		return nil
	default:
		return errors.New("invalid protocol and family")
	}

	srcPort, err := parsePort(parts[4])
	if err != nil {
		return err
	}

	dstPort, err := parsePort(parts[5])
	if err != nil {
		return err
	}

	c.remote = &net.TCPAddr{
		IP:   srcIP,
		Port: srcPort,
	}

	c.target = &net.TCPAddr{
		IP:   dstIP,
		Port: dstPort,
	}

	c.proxy = c.wrapped.RemoteAddr()
	return nil
}

func (c *Conn) initv2() error {
	header := make([]byte, 16)
	_, err := io.ReadFull(c.rbuf, header)
	if err != nil {
		return fmt.Errorf("failed reading header: %w", err)
	}

	if !bytes.Equal(header[:12], v2Signature) {
		return errors.New("v2 header does not match signature")
	}

	vac := versionAndCommand(header[12])
	if vac.Version() != 2 {
		return errors.New("invalid version")
	} else if vac.Command() != local && vac.Command() != proxy {
		return errors.New("invalid command")
	}

	c.version = vac.Version()

	addrProto := addressFamilyAndProtocol(header[13])
	length := int64(binary.BigEndian.Uint16(header[14:16]))
	payload := make([]byte, length)
	_, err = io.ReadFull(c.rbuf, payload)
	if err != nil {
		return fmt.Errorf("failed reading payload: %w", err)
	}

	if vac.Command() == local {
		// we can ignore everything else
		return nil
	}

	var srcIP net.IP
	var dstIP net.IP
	var srcPort uint16
	var dstPort uint16

	switch addrProto.AddressFamily() {
	case inet:
		if len(payload) < 12 {
			return errors.New("invalid IPv4 payload")
		}

		srcIP = net.IPv4(payload[0], payload[1], payload[2], payload[3])
		dstIP = net.IPv4(payload[4], payload[5], payload[6], payload[7])
		srcPort = binary.BigEndian.Uint16(payload[8:10])
		dstPort = binary.BigEndian.Uint16(payload[10:12])
	case inet6:
		if len(payload) < 36 {
			return errors.New("invalid IPv6 payload")
		}

		srcIP = net.IP(payload[:16])
		dstIP = net.IP(payload[16:32])
		srcPort = binary.BigEndian.Uint16(payload[32:34])
		dstPort = binary.BigEndian.Uint16(payload[34:36])
	case unix:
		return errors.New("unix sockets are not supported")
	case unspec:
		// ignore
	default:
		return errors.New("invalid address family")
	}

	switch addrProto.Protocol() {
	case stream:
		c.remote = &net.TCPAddr{
			IP:   srcIP,
			Port: int(srcPort),
		}

		c.target = &net.TCPAddr{
			IP:   dstIP,
			Port: int(dstPort),
		}
	case datagram:
		c.remote = &net.UDPAddr{
			IP:   srcIP,
			Port: int(srcPort),
		}

		c.target = &net.UDPAddr{
			IP:   dstIP,
			Port: int(dstPort),
		}
	default:
		if addrProto.AddressFamily() != unspec {
			return errors.New("invalid protocol")
		}
	}

	c.proxy = c.wrapped.RemoteAddr()
	return nil
}

type addressFamilyAndProtocol byte

type addressFamily byte
type protocol byte

const (
	unspec addressFamily = iota
	inet
	inet6
	unix
)

const (
	stream protocol = iota + 1
	datagram
)

func (ap addressFamilyAndProtocol) AddressFamily() addressFamily {
	return addressFamily(ap >> 4)
}

func (ap addressFamilyAndProtocol) Protocol() protocol {
	return protocol(ap & 0x0F)
}

type versionAndCommand byte

type command byte

// Command constants.
const (
	local command = iota
	proxy
)

func (vac versionAndCommand) Version() byte {
	return byte(vac >> 4)
}

func (vac versionAndCommand) Command() command {
	return command(vac & 0x0F)
}

func parsePort(s string) (int, error) {
	port, err := strconv.Atoi(s)
	if err != nil || port < 0 || port > 65535 {
		return 0, errors.New("invalid port number")
	}

	return port, nil
}
