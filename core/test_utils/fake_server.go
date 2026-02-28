package test_utils

import (
	"net"
	"testing"
)

// Mocks a simple TCP server that accepts one connection and runs the provided handler function.
func NewMockServer(t *testing.T, handler func(net.Conn)) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0") // :0 picks a random free port
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handler(conn)
	}()

	t.Cleanup(func() { ln.Close() })

	return ln.Addr().String()
}
