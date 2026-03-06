package core

import (
	"bufio"
	"fmt"
	"net"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

// Broker client that only connects on request
//
// This might come back as a problem later when we need to hold a connection open i.e. for long polling -> explore lazy connection with connection reuse to avoid overhead of establishing new connections.
type BrokerClient interface {
	Broadcast(topic string, metadata *TopicMetadata) error
}

type brokerClient struct {
	addr string
}

func NewBrokerClient(addr string) (BrokerClient, error) {
	return &brokerClient{
		addr: addr,
	}, nil
}

// Establishes a connection, instantiates a buffered writer around the connection, and returns both.
func (b *brokerClient) connect() (net.Conn, *bufio.Writer, error) {
	conn, err := net.Dial("tcp", b.addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to broker %s: %w", b.addr, err)
	}
	w := bufio.NewWriter(conn)
	return conn, w, nil
}

// Broadcasts topic metadata to a broker.
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

	log.Infof("Broadcasted topic metadata to broker %s", b.addr)

	return nil
}
