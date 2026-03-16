package core

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)


// BrokerClient that only connects on request
//
// This might come back as a problem later when we need to hold a connection open i.e. for long polling -> explore lazy connection with connection reuse to avoid overhead of establishing new connections.
type BrokerClient interface {
	Broadcast(topic string, metadata *TopicMetadata) error
	FetchLog(topic string, brokerID, partitionID int32) (*FetchLogResponse, error)
}

type BrokerClientOption func(*brokerClient)

func WithDialFn(dialFn func(addr string) (net.Conn, error)) BrokerClientOption {
	return func(b *brokerClient) {
		b.dialFn = dialFn
	}
}

type brokerClient struct {
	addr   string
	dialFn func(addr string) (net.Conn, error)
	conn   net.Conn
	w      *bufio.Writer
	logger *log.Logger
}

func NewBrokerClient(addr string, logger *log.Logger, opts ...BrokerClientOption) (BrokerClient, error) {
	b := &brokerClient{
		addr: addr,
		dialFn: func(addr string) (net.Conn, error) {
			return net.Dial("tcp", addr)
		},
		logger: deriveLogger(logger, fmt.Sprintf("broker-client(%s)", addr)),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

// Establishes a connection, instantiates a buffered writer around the connection, and returns both.
func (b *brokerClient) connect() (net.Conn, *bufio.Writer, error) {
	if b.conn != nil && b.w != nil {
		return b.conn, b.w, nil
	}
	conn, err := b.dialFn(b.addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to broker %s: %w", b.addr, err)
	}
	w := bufio.NewWriter(conn)

	b.conn = conn
	b.w = w

	return conn, w, nil
}

// Broadcasts topic metadata to a broker. Closes the connection after sending.
func (b *brokerClient) Broadcast(topic string, metadata *TopicMetadata) error {
	// Lazy connect
	conn, bufferWriter, err := b.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to broker %s: %w", b.addr, err)
	}
	defer conn.Close()

	metaMap := make(map[string]*TopicMetadata)
	metaMap[topic] = metadata

	payload := BroadcastTopicMetadataRequest{
		Metadata: metaMap,
	}
	if err := proto.WriteRequest(bufferWriter, MessageTypeBroadcastMetadata, payload); err != nil {
		return fmt.Errorf("failed to write sync topic metadata request: %w", err)
	}
	if err := bufferWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush sync topic metadata request: %w", err)
	}

	var resp proto.Response

	if err := proto.ReadResponse(conn, &resp); err != nil {
		return fmt.Errorf("failed to read sync topic metadata response: %w", err)
	}
	if resp.Status != proto.StatusOK {
		return fmt.Errorf("sync topic metadata request failed: %s", resp.Error)
	}

	b.logger.Infof("broadcasted topic metadata")

	return nil
}

func (b *brokerClient) FetchLog(topic string, brokerID, partitionID int32) (*FetchLogResponse, error) {
	// Lazy connect
	conn, bufferWriter, err := b.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", b.addr, err)
	}
	defer conn.Close()

	payload := FetchLogRequest{
		Topic:          topic,
		PartitionIndex: partitionID,
		ReplicaID:      brokerID,
	}
	if err := proto.WriteRequest(bufferWriter, MessageTypeFetchLog, payload); err != nil {
		return nil, fmt.Errorf("failed to write fetch log request: %w", err)
	}
	if err := bufferWriter.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush fetch log request: %w", err)
	}
	var (
		resp   proto.Response
		flResp FetchLogResponse
	)
	if err := proto.ReadResponse(conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read fetch log response: %w", err)
	}
	if resp.Status != proto.StatusOK {
		if strings.Contains(resp.Error, ErrOffsetNotFound.Error()) {
			return nil, ErrOffsetNotFound
		}
		return nil, fmt.Errorf("fetch log request failed: %s", resp.Error)
	}
	if err := resp.DecodeData(&flResp); err != nil {
		return nil, fmt.Errorf("failed to decode fetch log response: %w", err)
	}

	b.logger.Infof("fetched log")

	return &flResp, nil
}
