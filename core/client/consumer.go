package client

import (
	"bufio"
	"fmt"
	"net"

	"baby-kafka/core"
	"baby-kafka/core/proto"
)

type Consumer struct {
	conn net.Conn
	w    *bufio.Writer // Buffered writer that batches writes and flushes to connection

	// Track state for polling
	topic          string
	partitionIndex int32
	offset         int64
}

func NewConsumer(addr, topic string, partitionIndex int32, offset int64) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		conn:           conn,
		w:              bufio.NewWriter(conn),
		topic:          topic,
		partitionIndex: partitionIndex,
		offset:         offset,
	}, nil
}

func (c *Consumer) Poll() (key []byte, value []byte, err error) {
	payload := core.ConsumeRequest{
		Topic:          c.topic,
		PartitionIndex: c.partitionIndex,
		Offset:         c.offset,
	}

	if err := writeRequest(c.w, core.MessageTypeConsume, payload); err != nil {
		return nil, nil, fmt.Errorf("failed to write consume request: %w", err)
	}

	if err := c.w.Flush(); err != nil {
		return nil, nil, fmt.Errorf("failed to flush consume request: %w", err)
	}

	var (
		cResp core.ConsumeResponse
		resp  proto.Response
	)
	if err := readResponse(c.conn, &resp); err != nil {
		return nil, nil, fmt.Errorf("failed to read consume response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, nil, fmt.Errorf("consume request failed: %s", resp.Error)
	}

	if err := resp.DecodeData(&cResp); err != nil {
		return nil, nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	c.offset++ // Increment offset for next poll
	return cResp.Key, cResp.Value, nil
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}
