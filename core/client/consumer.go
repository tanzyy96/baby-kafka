package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

type Consumer interface {
	// ConnectBootstrap establishes a connection to the bootstrap broker. Should be called at startup.
	ConnectBootstrap() error
	// Run starts a goroutine for each partition that polls for messages. Should be called after FetchAllOffsets.
	Run(ctx context.Context) <-chan PollResult

	// Poll retrieves a message from the corresponding broker for the given partition.
	Poll(partitionIndex int32) (key []byte, value []byte, atOffset int64, err error)
	CommitOffset(partitionIndex int32, offset int64) error
	FetchOffset(partitionIndex int32) (int64, error)

	PollAll() map[int32]PollResult
	FetchAllOffsets() (partitionOffsets map[int32]int64, err error)

	BrokerFor(partitionIndex int32) (int32, error)

	Close() error
}

type consumer struct {
	id         string
	dialFn     func(addr string) (net.Conn, error)
	cfg        *core.Config
	brokerConn map[int32]net.Conn
	writers    map[int32]*bufio.Writer

	metadata *core.TopicMetadata

	// Track state for polling
	groupID string
	topic   string
	offsets map[int32]int64

	metadataLock sync.RWMutex
	offsetLock   sync.Mutex // Lock for offsets map
	connLock     sync.Mutex // Lock for brokerConn and writers maps
}

type PollResult struct {
	PartitionIndex int32
	Key            []byte
	Value          []byte
	Offset         int64
	Err            error
}

type ConsumerOption func(*consumer)

func WithConsumerDialFn(dialFn func(addr string) (net.Conn, error)) ConsumerOption {
	return func(c *consumer) {
		c.dialFn = dialFn
	}
}

func NewConsumer(id string, cfg *core.Config, groupID, topic string, partitionIndex []int32, opts ...ConsumerOption) (Consumer, error) {
	dialFn := func(addr string) (net.Conn, error) {
		return net.Dial("tcp", addr)
	}
	offsets := make(map[int32]int64)
	for _, idx := range partitionIndex {
		offsets[idx] = 0
	}

	c := &consumer{
		id:         id,
		dialFn:     dialFn,
		cfg:        cfg,
		brokerConn: map[int32]net.Conn{},
		writers:    map[int32]*bufio.Writer{},
		groupID:    groupID,
		topic:      topic,
		offsets:    offsets,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *consumer) Run(ctx context.Context) <-chan PollResult {
	resultChan := make(chan PollResult, 100)

	// For each partition, start a goroutine that constantly polls
	for _, partitionIndex := range c.PartitionIDs() {
		go func(partitionIndex int32) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					log.Debug("Polling partition", "partitionIndex", partitionIndex)
					key, value, atOffset, err := c.Poll(partitionIndex)
					if err != nil {
						if errors.Is(err, core.ErrNoMessagesAtOffset) {
							// TODO: check against config
							time.Sleep(3 * time.Second)
							continue
						}
					}

					// Select allows us to exit early if the context is done
					select {
					case resultChan <- PollResult{PartitionIndex: partitionIndex, Key: key, Value: value, Offset: atOffset, Err: err}:
					case <-ctx.Done():
						return
					}

					// TODO: check against config
					time.Sleep(1 * time.Second)
				}
			}
		}(partitionIndex)
	}
	return resultChan
}

func (c *consumer) ConnectBootstrap() error {
	if _, _, err := c.connFor(bootstrapBrokerID); err != nil {
		return fmt.Errorf("failed to connect to bootstrap broker: %w", err)
	}
	return nil
}

func (c *consumer) PartitionIDs() []int32 {
	res := []int32{}
	for partitionIndex := range c.offsets {
		res = append(res, partitionIndex)
	}
	return res
}

func (c *consumer) connFor(brokerID int32) (net.Conn, *bufio.Writer, error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()
	if conn, ok := c.brokerConn[brokerID]; ok {
		writer, wOk := c.writers[brokerID]
		if !wOk {
			return nil, nil, errors.New("connection found but writer missing")
		}
		return conn, writer, nil
	}
	if brokerID < 0 || int(brokerID) >= len(c.cfg.Brokers) {
		return nil, nil, errors.New("illegal brokerID")
	}
	addr := c.cfg.Brokers[brokerID].Addr
	conn, err := c.dialFn(addr)
	if err != nil {
		return nil, nil, err
	}

	c.brokerConn[brokerID] = conn
	c.writers[brokerID] = bufio.NewWriter(conn)

	return conn, c.writers[brokerID], nil
}

func (c *consumer) fetchTopicMetadata(brokerID int32, topic string) (*core.TopicMetadata, error) {
	conn, writer, err := c.connFor(brokerID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetchTopicMetadata: %w", err)
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
	c.metadataLock.Lock()
	defer c.metadataLock.Unlock()
	c.metadata = mResp.Metadata
	log.Info("Loaded topic metadata", "topic", topic)

	return mResp.Metadata, nil
}

func (c *consumer) brokerWithLeaderPartition(topic string, partitionID int32) (int32, error) {
	c.metadataLock.RLock()
	defer c.metadataLock.RUnlock()

	for _, partition := range c.metadata.PartitionMetadata {
		if partition.PartitionIndex == partitionID {
			return partition.Leader, nil
		}
	}
	return -1, fmt.Errorf("no partition leader found for topic %s", topic)
}

func (c *consumer) getConnection(partitionIndex int32) (conn net.Conn, writer *bufio.Writer, brokerID int32, err error) {
	// If topic metadata is not available, fetch it
	c.metadataLock.RLock()
	currentMetadata := c.metadata
	c.metadataLock.RUnlock()
	if currentMetadata == nil {
		if _, err := c.fetchTopicMetadata(bootstrapBrokerID, c.topic); err != nil {
			// return nil, nil, atOffset, fmt.Errorf("failed to fetch topic metadata: %w", err)
			return nil, nil, -1, fmt.Errorf("failed to fetch topic metadata: %w", err)
		}
	}

	// Get the brokerID and then the corresponding connection for partition
	brokerID, err = c.brokerWithLeaderPartition(c.topic, partitionIndex)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("failed to find broker for partition %d: %w", partitionIndex, err)
	}
	conn, writer, err = c.connFor(brokerID)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("failed to get connection for broker %d: %w", brokerID, err)
	}

	return conn, writer, brokerID, nil
}

func (c *consumer) PollAll() map[int32]PollResult {
	result := make(map[int32]PollResult)
	for _, partitionIndex := range c.PartitionIDs() {
		key, value, atOffset, err := c.Poll(partitionIndex)
		if err != nil {
			result[partitionIndex] = PollResult{Err: err}
			continue
		}
		result[partitionIndex] = PollResult{Key: key, Value: value, Offset: atOffset}
	}
	return result
}

func (c *consumer) Poll(partitionIndex int32) (key []byte, value []byte, atOffset int64, err error) {
	conn, writer, _, err := c.getConnection(partitionIndex)
	if err != nil {
		return nil, nil, -1, fmt.Errorf("failed to get connection for partition %d: %w", partitionIndex, err)
	}

	payload := core.ConsumeRequest{
		GroupId:        c.groupID,
		Topic:          c.topic,
		PartitionIndex: partitionIndex,
	}

	c.offsetLock.Lock()
	atOffset, ok := c.offsets[partitionIndex]
	if !ok {
		atOffset = 0
		c.offsets[partitionIndex] = atOffset
	}
	c.offsetLock.Unlock()
	payload.Offset = atOffset

	if err := proto.WriteRequest(writer, core.MessageTypeConsume, payload); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to write consume request: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return nil, nil, atOffset, fmt.Errorf("failed to flush consume request: %w", err)
	}

	var (
		cResp core.ConsumeResponse
		resp  proto.Response
	)
	if err := proto.ReadResponse(conn, &resp); err != nil {
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

	atOffset++

	c.offsetLock.Lock()
	c.offsets[partitionIndex] = atOffset
	c.offsetLock.Unlock()

	return cResp.Key, cResp.Value, atOffset, nil
}

func (c *consumer) CommitOffset(partitionIndex int32, offset int64) error {
	conn, writer, _, err := c.getConnection(partitionIndex)
	if err != nil {
		return fmt.Errorf("failed to get connection for partition %d: %w", partitionIndex, err)
	}

	payload := core.CommitOffsetRequest{
		GroupId:        c.groupID,
		Topic:          c.topic,
		PartitionIndex: partitionIndex,
		Offset:         offset,
	}

	if err := proto.WriteRequest(writer, core.MessageTypeCommitOffset, payload); err != nil {
		return fmt.Errorf("failed to write commitOffset request: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush commitOffset request: %w", err)
	}

	var resp proto.Response
	if err := proto.ReadResponse(conn, &resp); err != nil {
		return fmt.Errorf("failed to read commitOffset response: %w", err)
	}

	if resp.Status != proto.StatusOK {
		return fmt.Errorf("commitOffset request failed: %s", resp.Error)
	}

	return nil
}

// FetchAllOffsets retrieves the current offset for all partitions and sets them
func (c *consumer) FetchAllOffsets() (map[int32]int64, error) {
	result := make(map[int32]int64)
	for _, partitionIndex := range c.PartitionIDs() {
		offset, err := c.FetchOffset(partitionIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch offset for partition %d: %w", partitionIndex, err)
		}
		result[partitionIndex] = offset
	}
	return result, nil
}

// FetchOffset retrieves the current offset for the consumer and sets it
func (c *consumer) FetchOffset(partitionIndex int32) (int64, error) {
	conn, writer, _, err := c.getConnection(partitionIndex)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection for partition %d: %w", partitionIndex, err)
	}

	payload := core.FetchOffsetRequest{
		GroupId:        c.groupID,
		Topic:          c.topic,
		PartitionIndex: partitionIndex,
	}

	if err := proto.WriteRequest(writer, core.MessageTypeFetchOffset, payload); err != nil {
		return 0, fmt.Errorf("failed to write fetchOffset request: %w", err)
	}

	if err := writer.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush fetchOffset request: %w", err)
	}

	var resp proto.Response
	var fresp core.FetchOffsetResponse
	if err := proto.ReadResponse(conn, &resp); err != nil {
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

	c.offsetLock.Lock()
	c.offsets[partitionIndex] = fresp.Offset
	c.offsetLock.Unlock()

	return fresp.Offset, nil
}

func (c *consumer) BrokerFor(partitionIndex int32) (int32, error) {
	if c.metadata == nil {
		return -1, fmt.Errorf("metadata not loaded")
	}
	for _, partition := range c.metadata.PartitionMetadata {
		if partition.PartitionIndex == partitionIndex {
			return partition.Leader, nil
		}
	}
	return -1, fmt.Errorf("no partition leader found for partition %d", partitionIndex)
}

func (c *consumer) Close() error {
	for _, conn := range c.brokerConn {
		if err := conn.Close(); err != nil {
			log.Warn("Failed to close connection", "err", err)
		}
	}
	return nil
}
