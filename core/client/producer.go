package client

import (
	"bufio"
	"fmt"
	"net"

	"baby-kafka/core"
	"baby-kafka/core/proto"
)

type Producer struct {
	conn net.Conn
	w    *bufio.Writer // We use a buffered writer to batch writes and improve performance
}

func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Producer{
		conn: conn,
		w:    bufio.NewWriter(conn),
	}, nil
}

func (p *Producer) Send(topic string, key, value []byte) (*core.ProduceResponse, error) {
	payload := core.ProduceRequest{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	if err := writeRequest(p.w, core.MessageTypeProduce, payload); err != nil {
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
	if err := readResponse(p.conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read produce response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("produce request failed: %s", resp.Error)
	}

	// Decode resp.Data to core.ProduceResponse
	if err := resp.DecodeData(&prodResp); err != nil {
		return nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	return &prodResp, nil
}

func (p *Producer) Close() error {
	return p.conn.Close()
}
