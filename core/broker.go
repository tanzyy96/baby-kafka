package core

import (
	"fmt"
)

// We don't really need this, but more for my own reference
type BrokerInterface interface {
	CreateTopic(name string, numPartitions int32) error
	GetTopic(name string) (*Topic, error)
	ListTopics() []string

	// For messaging
	Produce(topicName string, msg Message) error
	Consume(topicName string, partitionIndex int32, offset int64) (*Message, error)
}

type Broker struct {
	topics        map[string]*Topic
	basePath      string
	rolloverLimit int64
	// TODO: NetworkManager
}

// TODO: I feel like this should be some config struct instead
func NewBroker(basePath string, rolloverLimit int64) (*Broker, error) {
	topics := make(map[string]*Topic)
	return &Broker{
		topics:        topics,
		basePath:      basePath,
		rolloverLimit: rolloverLimit,
	}, nil
}

func (b *Broker) CreateTopic(key string, numPartitions int32) error {
	if _, exists := b.topics[key]; exists {
		return fmt.Errorf("topic with key already exists: %s", key)
	}

	topic, err := NewTopic(key, numPartitions, b.basePath, b.rolloverLimit)
	if err != nil {
		return err
	}

	b.topics[key] = topic
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
