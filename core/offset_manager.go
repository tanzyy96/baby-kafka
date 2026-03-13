package core

import (
	"fmt"
	"os"
	"sync"
	"time"

	"baby-kafka/core/proto"
	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
)

const (
	offsetTopic         = "__consumer_offsets"
	offsetNumPartitions = 1 // NOTE: I have to be honest, I don't fully understand how partitions work for offsetTopic. Will have to revisit this.
)

/*
DURABILITY:
Offsets need to be tracked by (groupID, topicID, partition) to ensure that a consumer that crashes is able to resume at the correct offset.
This should be tracked both on memory and on disk to persist in case of broker crashes.

PERSISTENCE:
We persist the map state to disk via Kafka logfiles using OffsetKey and OffsetValue. We commit it to this dedicated offsetTopic

// TODO: COMPACTION:
As the logs grow, the O(n) restarts will get worse. So we have to do periodic compaction to ensure the offset logs don't get too big

REPLICATION:
By right we should replicate offsetTopic across multiple brokers to ensure everyone has the same understanding of the offsets.
*/

type OffsetKey struct {
	GroupID        string
	Topic          string
	PartitionIndex int32
}

type OffsetValue struct {
	Offset    int64
	CreatedAt int64
}

type OffsetManager interface {
	CommitOffset(groupID string, topicID string, partitionID int32, newOffset int64)
	Offset(groupID string, topicID string, partitionID int32) (int64, bool)
}

type offsetManager struct {
	// map[groupID]map[topicID]map[partitionID]offset
	offsets     map[string]map[string]map[int32]int64
	offsetTopic *Topic
	mutex       sync.RWMutex
}

func NewOffsetManager(basePath string, rolloverLimit int64) (OffsetManager, error) {
	offsets := make(map[string]map[string]map[int32]int64)

	partitionIndices := []int32{}
	for i := range int32(offsetNumPartitions) {
		partitionIndices = append(partitionIndices, i)
	}

	t, err := NewTopic(offsetTopic, partitionIndices, basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init offset manager: %w", err)
	}
	return &offsetManager{
		offsets:     offsets,
		offsetTopic: t,
	}, nil
}

func LoadOffsetManager(basePath string, rolloverLimit int64) (OffsetManager, error) {
	offsetPath := fmt.Sprintf("%s/%s", basePath, offsetTopic)
	if _, err := os.Stat(offsetPath); os.IsNotExist(err) {
		return NewOffsetManager(basePath, rolloverLimit)
	}

	om := &offsetManager{}
	if err := om.restore(basePath, rolloverLimit); err != nil {
		return nil, fmt.Errorf("failed to load offset manager: %w", err)
	}
	log.Info("Loaded offsets")
	return om, nil
}

func (om *offsetManager) updateOffset(groupID string, topicID string, partitionID int32, newOffset int64) {
	om.mutex.Lock()
	defer om.mutex.Unlock()

	_, exists := om.offsets[groupID]
	if !exists {
		om.offsets[groupID] = make(map[string]map[int32]int64)
	}

	_, exists = om.offsets[groupID][topicID]
	if !exists {
		om.offsets[groupID][topicID] = make(map[int32]int64)
	}

	om.offsets[groupID][topicID][partitionID] = newOffset
}

func (om *offsetManager) CommitOffset(groupID string, topicID string, partitionID int32, newOffset int64) {
	om.updateOffset(groupID, topicID, partitionID, newOffset)

	if err := om.persistToLog(groupID, topicID, partitionID, newOffset); err != nil {
		log.Warnf("failed to persist offset to %s", offsetTopic)
	}
}

func (om *offsetManager) Offset(groupID string, topicID string, partitionID int32) (int64, bool) {
	om.mutex.RLock()
	defer om.mutex.RUnlock()
	topics, ok := om.offsets[groupID]
	if !ok {
		return 0, false
	}
	partitions, ok := topics[topicID]
	if !ok {
		return 0, false
	}
	v, ok := partitions[partitionID]
	return v, ok
}

func (om *offsetManager) persistToLog(groupID string, topicID string, partitionID int32, newOffset int64) error {
	now := time.Now()
	key := OffsetKey{
		GroupID:        groupID,
		Topic:          topicID,
		PartitionIndex: partitionID,
	}
	value := OffsetValue{
		Offset:    newOffset,
		CreatedAt: now.Unix(),
	}
	kb, err := proto.GobEncode(key)
	if err != nil {
		return err
	}
	kv, err := proto.GobEncode(value)
	if err != nil {
		return err
	}

	// Special way to distribute offset messages
	offsetPartition := int32(utils.PartitionFor(groupID, offsetNumPartitions))

	if _, err = om.offsetTopic.Append(offsetPartition, *NewMessage(kb, kv)); err != nil {
		return err
	}

	return nil
}

func (om *offsetManager) restore(basePath string, rolloverLimit int64) error {
	t, err := LoadTopic(offsetTopic, fmt.Sprintf("%s/%s", basePath, offsetTopic), rolloverLimit)
	if err != nil {
		return fmt.Errorf("failed to restore offsets from %s: %w", offsetTopic, err)
	}

	om.offsetTopic = t
	om.offsets = make(map[string]map[string]map[int32]int64)

	restored := 0
	skipped := 0

	for _, partition := range t.partitions {
		for _, lg := range partition.Logs() {
			// Go down every message to updateOffset
			offset := lg.baseOffset
			for offset < lg.nextOffset {
				msg, err := lg.Read(offset)
				if err != nil {
					break // should be done reading this log
				}

				key := OffsetKey{}
				value := OffsetValue{}

				if err := proto.GobDecode(msg.Key, &key); err != nil {
					log.Warnf("restore: skipping record at offset %d: failed to decode key: %v", offset, err)
					skipped++
					offset++
					continue
				}

				if err := proto.GobDecode(msg.Value, &value); err != nil {
					log.Warnf("restore: skipping record at offset %d: failed to decode value: %v", offset, err)
					skipped++
					offset++
					continue
				}

				om.updateOffset(key.GroupID, key.Topic, key.PartitionIndex, value.Offset)
				restored++
				offset++
			}
		}
	}

	log.Infof("Restored %d offset(s) from log (%d record(s) skipped)", restored, skipped)
	return nil
}
