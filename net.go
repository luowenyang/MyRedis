package main

import (
	"golang.org/x/sys/unix"
	"log"
)

const BACKLOG int = 64

func Accept(fd int) (int, error) {
	nfd, _, err := unix.Accept(fd)
	return nfd, err
}

func TcpServer(port int) (int, error) {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Printf("init socket error: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	err = unix.SetsockoptInt(s, unix.SOL_SOCKET, unix.SO_REUSEPORT, port)
	if err != nil {
		log.Printf("set SO_REUSEADDR error: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	var addr unix.SockaddrInet4
	addr.Port = port
	err = unix.Bind(s, &addr)
	if err != nil {
		log.Printf("bind socket error: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	err = unix.Listen(s, BACKLOG)
	if err != nil {
		log.Printf("listen unix error: %v\n", err)
		unix.Close(s)
		return -1, nil
	}
	return s, nil
}
func Read(fd int, buf []byte) (int, error) {
	return unix.Read(fd, buf)
}
func Close(fd int) {
	unix.Close(fd)
}
func Write(fd int, buf []byte) (int, error) {
	return unix.Write(fd, buf)
}
