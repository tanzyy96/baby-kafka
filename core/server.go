package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

/*
Wire protocol:
- 4 bytes: length of the message (uint32, big-endian)
- 1 byte:  status (success, error)
- 1 byte: message type (produce, consume, create topic, list topics, etc.)
- N bytes: serialized message (using gob encoding)
*/

const (
	MessageTypeProduce           = 1
	MessageTypeConsume           = 2
	MessageTypeCreateTopic       = 3
	MessageTypeListTopics        = 4
	MessageTypeFetchOffset       = 5
	MessageTypeCommitOffset      = 6
	MessageTypeGetMetadata       = 7
	MessageTypeBroadcastMetadata = 8
	MessageTypeFetchLog          = 9
)

type Server interface {
	Start(ctx context.Context) error
	StartReplication(ctx context.Context) error
	Addr() string
	Stop() error
}

type server struct {
	listener   *net.Listener
	broker     Broker
	numClients atomic.Int32
	logger     *log.Logger
}

type Request struct{}

// TODO: add a Cluster struct to help spin up multiple servers
// HAHA that sounds like docker-compose to Dockerfile

// NewServer creates a new server instance.
func NewServer(cfg *Config, brokerID int32) (Server, error) {
	if brokerID < 0 || brokerID >= int32(len(cfg.Brokers)) {
		return nil, fmt.Errorf("invalid broker index: %d", brokerID)
	}
	brokerConfig := cfg.Brokers[brokerID]

	n, err := net.Listen("tcp", brokerConfig.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start network listener: %w", err)
	}
	// If the address is ":0", we need to update it to the actual address that the listener is bound to.
	if brokerConfig.Addr == ":0" {
		cfg.Brokers[brokerID].Addr = n.Addr().String()
	}

	rootLogger := log.NewWithOptions(os.Stderr, log.Options{})

	b, err := NewBroker(brokerID, cfg, rootLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker: %w", err)
	}

	logger := deriveLogger(rootLogger, fmt.Sprintf("server-%d", brokerID))
	logger.Infof("started on %s", cfg.Brokers[brokerID].Addr)
	return &server{
		listener: &n,
		broker:   b,
		logger:   logger,
	}, nil
}

func (s *server) Start(ctx context.Context) error {
	s.logger.Infof("listening for connections on %s", s.Addr())

	wg := sync.WaitGroup{}

	// THIS is what unblocks Accept() when ctrl+c is pressed
	go func() {
		<-ctx.Done()
		(*s.listener).Close()
	}()

	go s.StartReplication(ctx)

	for {
		conn, err := (*s.listener).Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.logger.Warn("received shutdown signal, closing listener...")
				wg.Wait() // wait for connections to close
				return nil
			default:
				return err
			}
		}

		wg.Add(1)
		s.numClients.Add(1)

		s.logger.Infof("new connection from %s: total %d", conn.RemoteAddr(), s.numClients.Load())
		go func() {
			// Track active connections and wait for them to finish before shutting down the server
			defer s.numClients.Add(-1)
			defer wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}
}

func (s *server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // Set a read deadline to prevent hanging connections

	// We need this goroutine to ensure that if the server is shutting down while a client is still connected, we can force close the connection to unblock any pending reads/writes. Otherwise, the server might hang indefinitely waiting for client activity.
	go func() {
		<-ctx.Done()                 // This blocks here until the server is shutting down
		conn.SetDeadline(time.Now()) // Force close the connection when the server is shutting down
	}()

	for {
		msgType, payload, err := proto.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.logger.Info("client disconnected")
			} else {
				s.logger.Warnf("failed to read message frame: %v", err)
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
		case MessageTypeFetchOffset:
			resp, err = s.handleFetchOffset(payload)
		case MessageTypeCommitOffset:
			resp, err = s.handleCommitOffset(payload)
		case MessageTypeGetMetadata:
			resp, err = s.handleGetMetadata(payload)
		case MessageTypeBroadcastMetadata:
			resp, err = s.handleBroadcastMetadata(payload)
		default:
			s.logger.Warnf("unknown message type received: %d", msgType)
			err = fmt.Errorf("unknown message type: %d", msgType)
		}

		if err != nil {
			proto.WriteError(conn, proto.StatusServerError, err)
			continue
		}
		// Response
		proto.WriteFrame(conn, resp)

		conn.SetReadDeadline(time.Now().Add(30 * time.Second)) // Set a read deadline to prevent hanging connections

	}
}

func (s *server) Addr() string {
	return (*s.listener).Addr().String()
}

func (s *server) Stop() error {
	return (*s.listener).Close()
}

func (s *server) StartReplication(ctx context.Context) error {
	return s.broker.StartReplication(ctx)
}
