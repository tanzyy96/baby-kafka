package core

import (
	"fmt"
	"io"
	"net"
	"time"

	"baby-kafka/core/proto"
)

/*
Wire protocol:
- 4 bytes: length of the message (uint32, big-endian)
- 1 byte: message type (produce, consume, create topic, list topics, etc.)
- N bytes: serialized message (using gob encoding)
*/

const (
	MessageTypeProduce     = 1
	MessageTypeConsume     = 2
	MessageTypeCreateTopic = 3
	MessageTypeListTopics  = 4
)

type Server struct {
	listener *net.Listener
	broker   *Broker
}

type Request struct{}

func NewServer(port string, rolloverLimit int64, datadir string) (*Server, error) {
	n, err := net.Listen("tcp", ":"+string(port))
	if err != nil {
		return nil, fmt.Errorf("failed to start network listener: %w", err)
	}
	b, err := NewBroker(datadir, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker: %w", err)
	}
	return &Server{
		listener: &n,
		broker:   b,
	}, nil
}

func (s *Server) Start() {
	fmt.Println("Network manager started, listening for connections...")
	for {
		conn, err := (*s.listener).Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) // Set a read deadline to prevent hanging connections

	for {
		msgType, payload, err := proto.ReadFrame(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
			} else {
				fmt.Printf("Failed to read message frame: %v\n", err)
			}
			return

		}

		var resp []byte

		switch msgType {
		case MessageTypeProduce:
			resp, err = s.handleProduce(payload)
		case MessageTypeConsume:
			resp, err = s.handleConsume(payload)
		case MessageTypeCreateTopic:
			resp, err = s.handleCreateTopic(payload)
		case MessageTypeListTopics:
			resp, err = s.handleListTopics()
		default:
			err = fmt.Errorf("unknown message type: %d", msgType)
		}

		if err != nil {
			proto.WriteError(conn, err)
			continue
		}
		// Response
		proto.WriteFrame(conn, resp)

	}
}

func (s *Server) Stop() error {
	return (*s.listener).Close()
}
