package testutils

import (
	"net"
	"sync/atomic"
	"testing"

	"baby-kafka/core/proto"

	"github.com/stretchr/testify/assert"
)

// NewMockServer starts a mock TCP server that accepts one connection and calls handler
// once per request frame. handler receives the parsed message type and raw gob payload,
// and returns the proto.Response to send back.
func NewMockServer(t *testing.T, handler func(msgType int, payload []byte) proto.Response) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			msgType, payload, err := proto.ReadFrame(conn)
			if err != nil {
				return
			}

			resp := handler(msgType, payload)
			data, err := proto.GobEncode(resp)
			if err != nil {
				return
			}
			if err := proto.WriteFrame(conn, data); err != nil {
				return
			}
		}
	}()

	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// NewTestConn starts a mock TCP server that accepts one connection and calls handler.
func NewTestConn(t *testing.T, handlers ...func(msgType int, payload []byte) proto.Response) (clientConn net.Conn, serverConn net.Conn) {
	clientConn, serverConn = net.Pipe()
	var called atomic.Int32

	go func() {
		defer serverConn.Close()
		for _, handler := range handlers {
			// We need to use assert.NoError here because require.NoError doesn't work with goroutines
			msgType, payload, err := proto.ReadFrame(serverConn)
			assert.NoError(t, err)

			resp := handler(msgType, payload)

			data, err := proto.GobEncode(resp)
			assert.NoError(t, err)

			assert.NoError(t, proto.WriteFrame(serverConn, data))
			called.Add(1)
		}
	}()

	t.Cleanup(func() {
		clientConn.Close()
		if called.Load() < int32(len(handlers)) {
			t.Errorf("Only %d of %d handlers were called", called.Load(), len(handlers))
		}
	})

	return
}
