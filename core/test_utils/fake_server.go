package test_utils

import (
	"net"
	"testing"

	"baby-kafka/core/proto"
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
