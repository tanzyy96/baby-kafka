package client

import (
	"bufio"
	"fmt"
	"net"

	"baby-kafka/core"
	"baby-kafka/core/proto"
	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
)

type Producer struct {
	conn net.Conn
	w    *bufio.Writer // We use a buffered writer to batch writes and improve performance

	// Metadata is able to store metadata for different topics
	// If the topic is not found, we will load metadata for that topic
	metadata map[string]*core.TopicMetadata
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Producer{
		conn:     conn,
		w:        bufio.NewWriter(conn),
		metadata: make(map[string]*core.TopicMetadata),
	}, nil
}

func (c *Producer) LoadTopicMetadata(topic string) (*core.TopicMetadata, error) {
	payload := core.GetMetadataRequest{
		Topic: topic,
	}

	if err := proto.WriteRequest(c.w, core.MessageTypeGetMetadata, payload); err != nil {
		return nil, fmt.Errorf("failed to write getTopicMetadata request: %w", err)
	}

	if err := c.w.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush consume request: %w", err)
	}

	var (
		mResp core.GetMetadataResponse
		resp  proto.Response
	)
	if err := proto.ReadResponse(c.conn, &resp); err != nil {
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
	c.metadata[topic] = mResp.Metadata
	log.Info("Loaded topic metadata", "topic", topic)

	return mResp.Metadata, nil
}

func (p *Producer) Send(topic string, key, value []byte) (*core.ProduceResponse, error) {
	topicData, ok := p.metadata[topic]
	if !ok {
		var err error
		topicData, err = p.LoadTopicMetadata(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to load topic metadata: %w", err)
		}
	}

	partition := utils.PartitionFor(string(key), uint32(topicData.NumPartitions))
	payload := core.ProduceRequest{
		Key:            key,
		Value:          value,
		Topic:          topic,
		PartitionIndex: int32(partition),
	}

	if err := proto.WriteRequest(p.w, core.MessageTypeProduce, payload); err != nil {
		return nil, fmt.Errorf("failed to write produce request: %w", err)
	}
	if err := p.w.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush produce request: %w", err)
	}

	// Read response
	var (
		resp     proto.Response
		prodResp core.ProduceResponse
	)
	if err := proto.ReadResponse(p.conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read produce response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("produce request failed: %s", resp.Error)
	}

	// Decode resp.Data to core.ProduceResponse
	if err := resp.DecodeData(&prodResp); err != nil {
		return nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	log.Info("Sent message", "key", string(key), "partition", partition, "value", string(value), "resp", prodResp)

	return &prodResp, nil
}

func (p *Producer) Close() error {
	return p.conn.Close()
}
