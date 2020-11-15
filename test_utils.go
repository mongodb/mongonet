package mongonet

import (
	"fmt"
	"syscall"
)

func EphemeralPort() (int, error) {
	fd, err := syscall.Socket(
		syscall.AF_INET,
		syscall.SOCK_STREAM,
		syscall.IPPROTO_TCP,
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to obtain socket. err=%v", err)
	}

	defer syscall.Close(fd)

	err = syscall.Bind(
		fd,
		&syscall.SockaddrInet4{
			Port: 0,                   // 0 requests that the kernel assign an ephemeral port
			Addr: [4]byte{0, 0, 0, 0}, // 0.0.0.0 means any IP address
		},
	)
	if err != nil {
		return -1, fmt.Errorf("Failed to bind socket. err=%v", err)
	}

	var sockaddr syscall.Sockaddr
	sockaddr, err = syscall.Getsockname(fd)
	if err != nil {
		return -1, fmt.Errorf("Failed to Getsockname() for fd %v. err=%v", fd, err)
	}

	sockaddr4, ok := sockaddr.(*syscall.SockaddrInet4)
	if !ok {
		return -1, fmt.Errorf("Expected *syscall.SockaddrInet4 from Getsockname(), but got %#v", sockaddr)
	}

	return sockaddr4.Port, nil
}
