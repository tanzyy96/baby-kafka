package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"baby-kafka/core"
	"baby-kafka/core/proto"
	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
)

const bootstrapBrokerID = int32(0)

type Producer interface {
	ConnectBootstrap() error
	FetchTopicMetadata(brokerID int32, topic string) (*core.TopicMetadata, error)
	Send(topic string, key, value []byte) (*core.ProduceResponse, error)
	Close() error
}

type producer struct {
	dialFn     func(addr string) (net.Conn, error) // Allows overriding the dial function for testing
	cfg        *core.Config
	brokerConn map[int32]net.Conn
	writers    map[int32]*bufio.Writer // We use a buffered writer to batch writes and improve performance
	logger     *log.Logger

	// Metadata is able to store metadata for different topics
	// If the topic is not found, we will load metadata for that topic
	metadata map[string]*core.TopicMetadata
}

type ProducerOption func(*producer)

func WithProducerDialFn(fn func(addr string) (net.Conn, error)) ProducerOption {
	return func(p *producer) {
		p.dialFn = fn
	}
}

func NewProducer(cfg *core.Config, logger *log.Logger, opts ...ProducerOption) (Producer, error) {
	brokerConn := make(map[int32]net.Conn)
	writers := make(map[int32]*bufio.Writer)
	metadata := make(map[string]*core.TopicMetadata)
	dialFn := func(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }

	p := &producer{
		dialFn:     dialFn,
		cfg:        cfg,
		brokerConn: brokerConn,
		writers:    writers,
		metadata:   metadata,
		logger:     logger,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

func (p *producer) brokerWithLeaderPartition(topic string, partitionID int32) (int32, error) {
	topicData, ok := p.metadata[topic]
	if !ok {
		return -1, fmt.Errorf("topic %s not found", topic)
	}
	for _, partition := range topicData.PartitionMetadata {
		if partition.PartitionIndex == partitionID {
			return partition.Leader, nil
		}
	}
	return -1, fmt.Errorf("no partition found for topic %s", topic)
}

func (p *producer) connFor(brokerID int32) (net.Conn, *bufio.Writer, error) {
	if conn, ok := p.brokerConn[brokerID]; ok {
		writer, wOk := p.writers[brokerID]
		if !wOk {
			return nil, nil, errors.New("connection found but writer missing")
		}
		return conn, writer, nil
	}
	if brokerID < 0 || int(brokerID) >= len(p.cfg.Brokers) {
		return nil, nil, errors.New("illegal brokerID")
	}
	addr := p.cfg.Brokers[brokerID].Addr
	conn, err := p.dialFn(addr)
	if err != nil {
		return nil, nil, err
	}
	p.brokerConn[brokerID] = conn
	p.writers[brokerID] = bufio.NewWriter(conn)
	return conn, p.writers[brokerID], nil
}

// ConnectBootstrap connects to the bootstrap broker first for validation
func (p *producer) ConnectBootstrap() error {
	if _, _, err := p.connFor(bootstrapBrokerID); err != nil {
		return fmt.Errorf("failed to connect to bootstrap broker: %w", err)
	}
	return nil
}

func (p *producer) FetchTopicMetadata(brokerID int32, topic string) (*core.TopicMetadata, error) {
	conn, writer, err := p.connFor(brokerID)
	if err != nil {
		return nil, fmt.Errorf("failed to FetchTopicMetadata: %w", err)
	}
	payload := core.GetMetadataRequest{
		Topic: topic,
	}

	if err := proto.WriteRequest(writer, core.MessageTypeGetMetadata, payload); err != nil {
		return nil, fmt.Errorf("failed to write getTopicMetadata request: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush metadata request: %w", err)
	}

	var (
		mResp core.GetMetadataResponse
		resp  proto.Response
	)
	if err := proto.ReadResponse(conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read consume response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		// Manual check
		return nil, fmt.Errorf("consume request failed: %s", resp.Error)
	}

	if err := resp.DecodeData(&mResp); err != nil {
		return nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	// set metadata and partition index
	p.metadata[topic] = mResp.Metadata
	p.logger.Info("loaded topic metadata", "topic", topic)

	return mResp.Metadata, nil
}

func (p *producer) Send(topic string, key, value []byte) (*core.ProduceResponse, error) {
	// Before sending, ensure metadata is loaded
	// This helps us figure out which broker is holding the leader for the partition
	topicData, ok := p.metadata[topic]
	if !ok {
		var err error
		topicData, err = p.FetchTopicMetadata(bootstrapBrokerID, topic)
		if err != nil {
			return nil, fmt.Errorf("failed to load topic metadata: %w", err)
		}
	}

	partition := utils.PartitionFor(string(key), uint32(topicData.NumPartitions))
	brokerID, err := p.brokerWithLeaderPartition(topic, int32(partition))
	if err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	conn, writer, err := p.connFor(brokerID)
	if err != nil {
		return nil, fmt.Errorf("failed to send message to broker %d: %w", brokerID, err)
	}

	payload := core.ProduceRequest{
		Key:            key,
		Value:          value,
		Topic:          topic,
		PartitionIndex: int32(partition),
	}

	if err := proto.WriteRequest(writer, core.MessageTypeProduce, payload); err != nil {
		return nil, fmt.Errorf("failed to write produce request: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush produce request: %w", err)
	}

	// Read response
	var (
		resp     proto.Response
		prodResp core.ProduceResponse
	)
	if err := proto.ReadResponse(conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read produce response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("produce request failed: %s", resp.Error)
	}

	// Decode resp.Data to core.ProduceResponse
	if err := resp.DecodeData(&prodResp); err != nil {
		return nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	p.logger.Info("sent message", "key", string(key), "partition", partition, "value", string(value), "resp", prodResp)

	return &prodResp, nil
}

func (p *producer) Close() error {
	for _, conn := range p.brokerConn {
		if err := conn.Close(); err != nil {
			p.logger.Warn("failed to close connection", "err", err)
		}
	}
	return nil
}
