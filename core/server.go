package core

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

func (s *Server) Start(ctx context.Context) error {
	fmt.Println("Network manager started, listening for connections...")

	wg := sync.WaitGroup{}

	// THIS is what unblocks Accept() when ctrl+c is pressed
	go func() {
		<-ctx.Done()
		(*s.listener).Close()
	}()

	for {
		conn, err := (*s.listener).Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				fmt.Println("Received shutdown signal, closing listener...")
				wg.Wait() // wait for connections to close
				return nil
			default:
				return err
			}
		}

		wg.Add(1)
		go func() {
			// Track active connections and wait for them to finish before shutting down the server
			defer wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) // Set a read deadline to prevent hanging connections

	// We need this goroutine to ensure that if the server is shutting down while a client is still connected, we can force close the connection to unblock any pending reads/writes. Otherwise, the server might hang indefinitely waiting for client activity.
	go func() {
		<-ctx.Done()                 // This blocks here until the server is shutting down
		conn.SetDeadline(time.Now()) // Force close the connection when the server is shutting down
	}()

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
