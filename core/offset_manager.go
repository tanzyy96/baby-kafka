package core

import (
	"fmt"
	"os"
	"sync"
	"time"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

const (
	offsetTopic         = "__consumer_offsets"
	offsetNumPartitions = 1
)

/*
DURABILITY:
Offsets need to be tracked by (groupId, topicId, partition) to ensure that a consumer that crashes is able to resume at the correct offset.
This should be tracked both on memory and on disk to persist in case of broker crashes.

PERSISTENCE:
We persist the map state to disk via Kafka logfiles using OffsetKey and OffsetValue. We commit it to this dedicated offsetTopic

// TODO: COMPACTION:
As the logs grow, the O(n) restarts will get worse. So we have to do periodic compaction to ensure the offset logs don't get too big
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

type OffsetManager struct {
	// map[groupId]map[topicId]map[partitionId]offset
	offsets     map[string]map[string]map[int32]int64
	offsetTopic *Topic
	mutex       sync.RWMutex
}

func NewOffsetManager(basePath string, rolloverLimit int64) (*OffsetManager, error) {
	offsets := make(map[string]map[string]map[int32]int64)
	t, err := NewTopic(offsetTopic, offsetNumPartitions, basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init offset manager: %w", err)
	}
	return &OffsetManager{
		offsets:     offsets,
		offsetTopic: t,
	}, nil
}

func LoadOffsetManager(basePath string, rolloverLimit int64) (*OffsetManager, error) {
	offsetPath := fmt.Sprintf("%s/%s", basePath, offsetTopic)
	if _, err := os.Stat(offsetPath); os.IsNotExist(err) {
		return NewOffsetManager(basePath, rolloverLimit)
	}

	om := &OffsetManager{}
	if err := om.restore(basePath, rolloverLimit); err != nil {
		return nil, fmt.Errorf("failed to load offset manager: %w", err)
	}
	log.Info("Loaded offsets")
	return om, nil
}

func (om *OffsetManager) updateOffset(groupId string, topicId string, partitionId int32, newOffset int64) {
	om.mutex.Lock()
	defer om.mutex.Unlock()

	_, exists := om.offsets[groupId]
	if !exists {
		om.offsets[groupId] = make(map[string]map[int32]int64)
	}

	_, exists = om.offsets[groupId][topicId]
	if !exists {
		om.offsets[groupId][topicId] = make(map[int32]int64)
	}

	om.offsets[groupId][topicId][partitionId] = newOffset
}

func (om *OffsetManager) CommitOffset(groupId string, topicId string, partitionId int32, newOffset int64) {
	om.updateOffset(groupId, topicId, partitionId, newOffset)

	if err := om.persistToLog(groupId, topicId, partitionId, newOffset); err != nil {
		log.Warnf("failed to persist offset to %s", offsetTopic)
	}
}

func (om *OffsetManager) Offset(groupId string, topicId string, partitionId int32) (int64, bool) {
	om.mutex.RLock()
	defer om.mutex.RUnlock()
	topics, ok := om.offsets[groupId]
	if !ok {
		return 0, false
	}
	partitions, ok := topics[topicId]
	if !ok {
		return 0, false
	}
	v, ok := partitions[partitionId]
	return v, ok
}

func (om *OffsetManager) persistToLog(groupId string, topicId string, partitionId int32, newOffset int64) error {
	now := time.Now()
	key := OffsetKey{
		GroupID:        groupId,
		Topic:          topicId,
		PartitionIndex: partitionId,
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
	if _, _, err = om.offsetTopic.Append(*NewMessage(kb, kv)); err != nil {
		return err
	}

	return nil
}

func (om *OffsetManager) restore(basePath string, rolloverLimit int64) error {
	t, err := LoadTopic(offsetTopic, fmt.Sprintf("%s/%s", basePath, offsetTopic), rolloverLimit)
	if err != nil {
		return fmt.Errorf("failed to restore offsets from %s: %w", offsetTopic, err)
	}

	om.offsetTopic = t
	om.offsets = make(map[string]map[string]map[int32]int64)

	restored := 0
	skipped := 0

	for _, partition := range t.partitions {
		for _, lg := range partition.logs {
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
