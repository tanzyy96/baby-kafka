package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"baby-kafka/core"
	"baby-kafka/core/proto"
)

type Consumer struct {
	conn net.Conn
	w    *bufio.Writer // Buffered writer that batches writes and flushes to connection

	// Track state for polling
	groupId        string
	topic          string
	partitionIndex int32
	offset         int64
}

func NewConsumer(addr, groupId, topic string, partitionIndex int32, offset int64) (*Consumer, error) {
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
		groupId:        groupId,
	}, nil
}

func (c *Consumer) Poll() (key []byte, value []byte, atOffset int64, err error) {
	payload := core.ConsumeRequest{
		GroupId:        c.groupId,
		Topic:          c.topic,
		PartitionIndex: c.partitionIndex,
		Offset:         c.offset,
	}

	atOffset = c.offset

	if err := writeRequest(c.w, core.MessageTypeConsume, payload); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to write consume request: %w", err)
	}

	if err := c.w.Flush(); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to flush consume request: %w", err)
	}

	var (
		cResp core.ConsumeResponse
		resp  proto.Response
	)
	if err := readResponse(c.conn, &resp); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to read consume response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		// Manual check
		if strings.Contains(resp.Error, core.ErrNoMessagesAtOffset.Error()) {
			return nil, nil, 0, core.ErrNoMessagesAtOffset
		}
		return nil, nil, atOffset, fmt.Errorf("consume request failed: %s", resp.Error)
	}

	if err := resp.DecodeData(&cResp); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to decode response.Data: %w", err)
	}

	c.offset++ // Increment offset for next poll
	return cResp.Key, cResp.Value, atOffset, nil
}

func (c *Consumer) CommitOffset(offset int64) error {
	payload := core.CommitOffsetRequest{
		GroupId:        c.groupId,
		Topic:          c.topic,
		PartitionIndex: c.partitionIndex,
		Offset:         offset,
	}

	if err := writeRequest(c.w, core.MessageTypeCommitOffset, payload); err != nil {
		return fmt.Errorf("failed to write commitOffset request: %w", err)
	}

	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush commitOffset request: %w", err)
	}

	var resp proto.Response
	if err := readResponse(c.conn, &resp); err != nil {
		return fmt.Errorf("failed to read commitOffset response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return fmt.Errorf("commitOffset request failed: %s", resp.Error)
	}

	return nil
}

// Fetches latestoffset and sets consumer offset to that value
func (c *Consumer) FetchOffset() (int64, error) {
	payload := core.FetchOffsetRequest{
		GroupId:        c.groupId,
		Topic:          c.topic,
		PartitionIndex: c.partitionIndex,
	}

	if err := writeRequest(c.w, core.MessageTypeFetchOffset, payload); err != nil {
		return 0, fmt.Errorf("failed to write fetchOffset request: %w", err)
	}

	if err := c.w.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush fetchOffset request: %w", err)
	}

	var resp proto.Response
	var fresp core.FetchOffsetResponse
	if err := readResponse(c.conn, &resp); err != nil {
		return 0, fmt.Errorf("failed to read fetchOffset response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		// Manual check for expected errors that aren't systemic errors
		// Currently exceeding the offset limit will return ErrNoMessagesAtOffset
		if strings.Contains(resp.Error, core.ErrNoMessagesAtOffset.Error()) {
			return 0, core.ErrNoMessagesAtOffset
		}
		if strings.Contains(resp.Error, core.ErrOffsetNotFound.Error()) {
			return 0, core.ErrOffsetNotFound
		}

		return 0, fmt.Errorf("fetchOffset request failed: %s", resp.Error)
	}

	if err := resp.DecodeData(&fresp); err != nil {
		return 0, fmt.Errorf("failed to decode fetchOffset.data: %w", err)
	}

	c.offset = fresp.Offset

	return fresp.Offset, nil
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}
