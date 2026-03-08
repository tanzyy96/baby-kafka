package core

import (
	"errors"
	"fmt"
	"os"

	"github.com/charmbracelet/log"
)

var ErrOffsetNotFound = errors.New("offset not found")

type Broker interface {
	CreateTopic(name string, numPartitions int32) error
	GetTopic(name string) (*Topic, error)
	ListTopics() []string

	// For messaging
	Produce(topicName string, partitionIndex int32, msg Message) (int64, error)
	Consume(topicName string, partitionIndex int32, offset int64) (*Message, error)

	// For offsets
	FetchOffset(groupID, topic string, partitionIndex int32) (int64, error)
	CommitOffset(groupID, topic string, partitionIndex int32, newOffset int64) error

	GetTopicMetadata(topicName string) (*TopicMetadata, error)
	BroadcastMetadata(topic string) error
	UpdateMetadata(map[string]*TopicMetadata) error
}

type broker struct {
	id                int32
	topics            map[string]*Topic
	basePath          string
	rolloverLimit     int64
	replicationFactor int32

	brokerConfigs []BrokerConfig
	brokerClients map[int32]BrokerClient

	offsetManager   *OffsetManager
	metadataManager MetadataManager
}

func NewBroker(id int32, cfg *Config) (Broker, error) {
	brokerPath := fmt.Sprintf("%s/broker-%d", cfg.BasePath, id)
	// Try creating the base path for the broker if it doesn't exist
	if err := os.Mkdir(brokerPath, 0o755); err != nil {
		if os.IsExist(err) {
			log.Infof("Broker path already exists, loading existing directory: %s", brokerPath)
			return LoadBroker(id, cfg)
		} else {
			return nil, fmt.Errorf("failed to init broker: %w", err)
		}
	}
	log.Infof("Created broker directory: %s", brokerPath)
	topics := make(map[string]*Topic)
	om, err := NewOffsetManager(brokerPath, cfg.RolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init broker: %w", err)
	}
	mm, err := NewMetadataManager(brokerPath, cfg.RolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init broker: %w", err)
	}
	return &broker{
		id:              id,
		topics:          topics,
		basePath:        brokerPath,
		rolloverLimit:   cfg.RolloverLimit,
		brokerConfigs:   cfg.Brokers,
		offsetManager:   om,
		metadataManager: mm,
		brokerClients:   initBrokerClients(id, cfg.Brokers),
	}, nil
}

func LoadBroker(id int32, cfg *Config) (Broker, error) {
	brokerPath := fmt.Sprintf("%s/broker-%d", cfg.BasePath, id)
	topics, err := LoadTopics(brokerPath, cfg.RolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to load broker: %w", err)
	}
	om, err := LoadOffsetManager(brokerPath, cfg.RolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to load broker: %w", err)
	}
	mm, err := LoadMetadataManager(brokerPath, cfg.RolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to load broker: %w", err)
	}

	return &broker{
		id:                id,
		topics:            topics,
		basePath:          brokerPath,
		rolloverLimit:     cfg.RolloverLimit,
		brokerConfigs:     cfg.Brokers,
		offsetManager:     om,
		metadataManager:   mm,
		brokerClients:     initBrokerClients(id, cfg.Brokers),
		replicationFactor: cfg.ReplicationFactor,
	}, nil
}

func initBrokerClients(id int32, configs []BrokerConfig) map[int32]BrokerClient {
	brokerClients := make(map[int32]BrokerClient)
	for _, config := range configs {
		if config.Index == id {
			continue
		}
		client, err := NewBrokerClient(config.Addr)
		if err != nil {
			log.Warnf("Failed to create broker client %d for port %s: %v", config.Index, config.Addr, err)
			continue
		}
		brokerClients[config.Index] = client
	}
	return brokerClients
}

// CreateTopic creates the topic metadata and initializes the topic folders.
func (b *broker) CreateTopic(key string, numPartitions int32) error {
	if numPartitions == 0 {
		return fmt.Errorf("numPartitions must be greater than zero")
	}
	if _, exists := b.topics[key]; exists {
		return fmt.Errorf("topic with key already exists: %s", key)
	}

	if err := b.metadataManager.Init(b.brokerConfigs, key, numPartitions, b.replicationFactor); err != nil {
		return fmt.Errorf("failed to init metadata manager: %w", err)
	}

	partitionIndices := b.metadataManager.PartitionsResponsibleFor(b.id, key)

	topic, err := NewTopic(key, partitionIndices, b.basePath, b.rolloverLimit)
	if err != nil {
		return err
	}

	if err := b.BroadcastMetadata(key); err != nil {
		return fmt.Errorf("failed to broadcast metadata on createTopic: %w", err)
	}

	b.topics[key] = topic
	log.Info("Created topic", "key", key, "numPartitions", numPartitions)
	return nil
}

func (b *broker) GetTopic(key string) (*Topic, error) {
	topic, exists := b.topics[key]
	if !exists {
		return nil, fmt.Errorf("topic with key not found: %s", key)
	}
	return topic, nil
}

func (b *broker) ListTopics() []string {
	keys := make([]string, 0, len(b.topics))
	for key := range b.topics {
		keys = append(keys, key)
	}
	return keys
}

func (b *broker) Produce(topicName string, partitionIndex int32, msg Message) (int64, error) {
	topic, err := b.GetTopic(topicName)
	if err != nil {
		return 0, fmt.Errorf("failed to produce message: %w", err)
	}
	return topic.Append(partitionIndex, msg)
}

func (b *broker) Consume(topicName string, partitionIndex int32, offset int64) (*Message, error) {
	topic, err := b.GetTopic(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to consume message: %w", err)
	}
	return topic.ReadAt(partitionIndex, offset)
}

func (b *broker) FetchOffset(groupID, topic string, partitionIndex int32) (int64, error) {
	offset, _ := b.offsetManager.Offset(groupID, topic, partitionIndex)
	return offset, nil
}

func (b *broker) CommitOffset(groupID, topic string, partitionIndex int32, newOffset int64) error {
	b.offsetManager.CommitOffset(groupID, topic, partitionIndex, newOffset)
	return nil
}

func (b *broker) GetTopicMetadata(topicName string) (*TopicMetadata, error) {
	if meta := b.metadataManager.Get(topicName); meta != nil {
		return meta, nil
	}
	// Fallback for topics that predate the metadata manager
	topic, err := b.GetTopic(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}
	return &TopicMetadata{
		Topic:         topicName,
		NumPartitions: topic.numPartitions,
	}, nil
}

// BroadcastMetadata syncs topic metadata across all brokers
func (b *broker) BroadcastMetadata(topic string) error {
	topicMetadata := b.metadataManager.Get(topic)
	for _, client := range b.brokerClients {
		if err := client.Broadcast(topic, topicMetadata); err != nil {
			log.Warnf("Failed to sync topic metadata with broker client: %v. Should retry soon.", err)
		}
	}

	return nil
}

func (b *broker) InsertMetadata(newMeta map[string]*TopicMetadata) error {
	// We receive the broadcasted metadata update from another broker.
	// If any new topic is added, create the topic and any partitions this broker is responsible for.
	newTopics := []string{}
	for _, meta := range newMeta {
		if _, exists := b.topics[meta.Topic]; !exists {
			newTopics = append(newTopics, meta.Topic)
		}
	}

	b.metadataManager.Update(newMeta)

	for _, topic := range newTopics {
		log.Debug("New metadata", "metadata", b.metadataManager.Get(topic))
		partitionIndices := b.metadataManager.PartitionsResponsibleFor(b.id, topic)
		log.Debugf("Broker is in charge of %v for topic %s", partitionIndices, topic)
		topic, err := NewTopic(topic, partitionIndices, b.basePath, b.rolloverLimit)
		if err != nil {
			return err
		}
		b.topics[topic.Key] = topic
	}

	log.Infof("Updated metadata for broker %d: currently has %d topics", b.id, len(b.topics))

	return nil
}
