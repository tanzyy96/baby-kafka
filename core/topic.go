package core

import (
	"fmt"
	"os"
	"sync/atomic"

	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
)

/*
Topic is a logical grouping of messages ie. "orders", "payments", "users".
Producers write to topics and consumers read from topics.

Each topic can have multiple partitions, and each partition is an ordered, immutable sequence of messages that is continually appended to. The messages in the partitions are assigned a sequential id number called the offset that uniquely identifies each message within the partition. The purpose of topics is to allow for parallelism and scalability, as messages can be distributed across multiple partitions and consumed by multiple consumers in parallel.
*/
type Topic struct {
	Key string

	folderPath    string
	partitions    map[int32]*Partition
	numPartitions int32
	// TODO: load from disk
	// configFile    *os.File

	// Counter for round-robin partition assignment
	// Atomic helps it remain thread-safe when we have multiple producers writing to the same topic
	counter atomic.Uint64
}

// Creates topic and corresponding topic folder
func NewTopic(key string, numPartition int32, folderPath string, rolloverLimit int64) (*Topic, error) {
	topicPath := fmt.Sprintf("%s/%s", folderPath, key)
	if err := os.Mkdir(topicPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create topic folder: %w", err)
	}
	partitions := make(map[int32]*Partition)
	for i := range numPartition {
		p, err := NewPartition(i, topicPath, rolloverLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to create partitions for topic: %w", err)
		}
		partitions[i] = p
	}

	return &Topic{
		Key:           key,
		folderPath:    topicPath,
		partitions:    partitions,
		numPartitions: numPartition,
	}, nil
}

func LoadTopic(topic, topicPath string, rolloverLimit int64) (*Topic, error) {
	partitions, err := LoadPartitions(topicPath, rolloverLimit)
	if err != nil {
		return nil, err
	}
	numPartitions := len(partitions)

	if topic != offsetTopic {
		log.Info("Loaded", "topic", topic, "numPartitions", numPartitions)
	}

	return &Topic{
		Key:           topic,
		folderPath:    topicPath,
		partitions:    partitions,
		numPartitions: int32(numPartitions),
		counter:       atomic.Uint64{},
	}, nil
}

// Scan the directories to restore the topic states
func LoadTopics(basePath string, rolloverLimit int64) (map[string]*Topic, error) {
	folders, err := os.ReadDir(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load topics: %w", err)
	}
	topics := make(map[string]*Topic)

	for _, folder := range folders {
		if !folder.IsDir() || folder.Name() == offsetTopic {
			continue
		}
		folderPath := fmt.Sprintf("%s/%s", basePath, folder.Name())

		t, err := LoadTopic(folder.Name(), folderPath, rolloverLimit)
		if err != nil {
			log.Warnf("failed to load partition at %s: %s", folder.Name(), err.Error())
			continue
		}
		topics[t.Key] = t
	}

	return topics, nil
}

func (t *Topic) nextPartition(key *string) (*Partition, error) {
	partitions := t.partitions
	if key == nil {
		// Round robin
		partitionIndex := t.counter.Add(1) % uint64(len(partitions))
		return partitions[int32(partitionIndex)], nil
	} else {
		// hash(key) % numPartitions
		partitionIndex := utils.PartitionFor(*key, uint32(len(partitions)))
		return partitions[int32(partitionIndex)], nil
	}
}

func (t *Topic) Append(msg Message) (partitionIndex int32, offset int64, err error) {
	var key *string
	if len(msg.Key) > 0 {
		str := string(msg.Key)
		key = &str
	}
	partition, err := t.nextPartition(key)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get next partition: %w", err)
	}
	offset, err = partition.Append(msg)
	return partition.Index, offset, err
}

func (t *Topic) ReadAt(partitionIndex int32, offset int64) (*Message, error) {
	partition, exists := t.partitions[partitionIndex]
	if !exists {
		return nil, fmt.Errorf("partition index out of range: %d", partitionIndex)
	}
	return partition.ReadAt(offset)
}

func (t *Topic) NumPartitions() int32 {
	return t.numPartitions
}
