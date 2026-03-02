package core

import (
	"errors"
	"fmt"
	"os"

	"github.com/charmbracelet/log"
)

var ErrOffsetNotFound = errors.New("offset not found")

// We don't really need this, but more for my own reference
type BrokerInterface interface {
	CreateTopic(name string, numPartitions int32) error
	GetTopic(name string) (*Topic, error)
	ListTopics() []string

	// For messaging
	Produce(topicName string, msg Message) error
	Consume(topicName string, partitionIndex int32, offset int64) (*Message, error)

	// For offsets
	FetchOffset(groupId, topic string, partitionIndex int32) (int64, error)
	CommitOffset(groupId, topic string, partitionIndex int32, newOffset int64) error
}

type Broker struct {
	topics        map[string]*Topic
	basePath      string
	rolloverLimit int64
	offsetManager *OffsetManager
}

func NewBroker(basePath string, rolloverLimit int64) (*Broker, error) {
	// Try creating the base path for the broker if it doesn't exist
	if err := os.Mkdir(basePath, 0o755); err != nil {
		if os.IsExist(err) {
			log.Infof("Base path already exists, loading existing directory: %s", basePath)
			return LoadBroker(basePath, rolloverLimit)
		} else {
			return nil, fmt.Errorf("failed to init broker: %w", err)
		}
	}
	topics := make(map[string]*Topic)
	om, err := NewOffsetManager(basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init broker: %w", err)
	}
	return &Broker{
		topics:        topics,
		basePath:      basePath,
		rolloverLimit: rolloverLimit,
		offsetManager: om,
	}, nil
}

func LoadBroker(basePath string, rolloverLimit int64) (*Broker, error) {
	topics, err := LoadTopics(basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to load broker: %w", err)
	}
	om, err := LoadOffsetManager(basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to load broker: %w", err)
	}

	return &Broker{
		topics:        topics,
		basePath:      basePath,
		rolloverLimit: rolloverLimit,
		offsetManager: om,
	}, nil
}

func (b *Broker) CreateTopic(key string, numPartitions int32) error {
	if numPartitions == 0 {
		return fmt.Errorf("numPartitions must be greater than zero")
	}
	if _, exists := b.topics[key]; exists {
		return fmt.Errorf("topic with key already exists: %s", key)
	}

	topic, err := NewTopic(key, numPartitions, b.basePath, b.rolloverLimit)
	if err != nil {
		return err
	}

	b.topics[key] = topic
	log.Info("Created topic", "key", key, "numPartitions", numPartitions)
	return nil
}

func (b *Broker) GetTopic(key string) (*Topic, error) {
	topic, exists := b.topics[key]
	if !exists {
		return nil, fmt.Errorf("topic with key not found: %s", key)
	}
	return topic, nil
}

func (b *Broker) ListTopics() []string {
	keys := make([]string, 0, len(b.topics))
	for key := range b.topics {
		keys = append(keys, key)
	}
	return keys
}

func (b *Broker) Produce(topicName string, msg Message) (int32, int64, error) {
	topic, err := b.GetTopic(topicName)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to produce message: %w", err)
	}
	return topic.Append(msg)
}

func (b *Broker) Consume(topicName string, partitionIndex int32, offset int64) (*Message, error) {
	topic, err := b.GetTopic(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to consume message: %w", err)
	}
	return topic.ReadAt(partitionIndex, offset)
}

func (b *Broker) FetchOffset(groupId, topic string, partitionIndex int32) (int64, error) {
	offset, ok := b.offsetManager.Offset(groupId, topic, partitionIndex)
	if !ok {
		return 0, ErrOffsetNotFound
	}

	return offset, nil
}

func (b *Broker) CommitOffset(groupId, topic string, partitionIndex int32, newOffset int64) error {
	b.offsetManager.CommitOffset(groupId, topic, partitionIndex, newOffset)
	return nil
}
