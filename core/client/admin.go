package client

import (
	"bufio"
	"fmt"
	"net"

	"baby-kafka/core"
	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

type Admin struct {
	conn net.Conn
	w    *bufio.Writer
}

func NewAdmin(addr string) (*Admin, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Admin{
		conn: conn,
		w:    bufio.NewWriter(conn),
	}, nil
}

func (a *Admin) CreateTopic(topic string) (*proto.Response, error) {
	payload := core.CreateTopicRequest{
		Topic: topic,
	}

	if err := writeRequest(a.w, core.MessageTypeCreateTopic, payload); err != nil {
		return nil, fmt.Errorf("failed to write create topic request: %w", err)
	}
	if err := a.w.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush create topic request: %w", err)
	}

	log.Info("Create topic request sent", "topic", topic)

	// Read response
	var resp proto.Response
	if err := readResponse(a.conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read create topic response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("create topic request failed: %s", resp.Error)
	}

	return &resp, nil
}

func (a *Admin) ListTopics() (*core.ListTopicsResponse, error) {
	if err := writeRequest(a.w, core.MessageTypeListTopics, nil); err != nil {
		return nil, fmt.Errorf("failed to write list topics request: %w", err)
	}
	if err := a.w.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush list topics request: %w", err)
	}

	var topicsResp core.ListTopicsResponse
	var resp proto.Response
	if err := readResponse(a.conn, &resp); err != nil {
		return nil, fmt.Errorf("failed to read list topics response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("list topics request failed")
	}

	if err := resp.DecodeData(&topicsResp); err != nil {
		return nil, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	return &topicsResp, nil
}
