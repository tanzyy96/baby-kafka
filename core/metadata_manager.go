package core

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sync"
	"time"

	"baby-kafka/core/proto"
	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
)

const (
	metadataTopic         = "__topic_metadata"
	metadataNumPartitions = 1
)

/*
How does the CreateTopic flow work in a distributed system?
1. Broker receives CreateTopic request and validates
2. Broker does SetTopicMetadata, then CreateTopic
3. Broker does SyncTopicMetadata to other brokers via client.Broker -> Other brokers will do SetTopicMetadata & CreateTopic
4. Broker does SaveMetadata
*/

type MetadataKey struct {
	Topic string
}

type MetadataValue struct {
	TopicMetadata
	CreatedAt int64
}

type TopicMetadata struct {
	Topic         string
	NumPartitions int32

	PartitionMetadata []PartitionMetadata
}

type PartitionMetadata struct {
	PartitionIndex int32
	/*
		Index of the leader broker for given partition
	*/
	Leader int32
	/*
		Address of the leader broker for given partition
	*/
	LeaderAddr string
	/*
		Indexes of the replicas for given partition
	*/
	Replicas []int32
}

// MetadataManager is responsible for managing topic metadata.
//
// This guy here needs to assign partitions and leader on CreateTopic.
// This guy needs to handle propagation of metadata changes.
// This guy needs to handle backup of metadata to __topic_metadata.
type MetadataManager interface {
	Get(topic string) *TopicMetadata
	GetAll() map[string]*TopicMetadata
	Init(brokerConfigs []BrokerConfig, topic string, numPartitions, replicationFactor int32) error
	Update(metadata map[string]*TopicMetadata)
	// Returns the partitions that a given broker is responsible for
	PartitionsResponsibleFor(brokerID int32, topic string) []int32
}

type metadataManager struct {
	metadata      map[string]*TopicMetadata
	metadataTopic *Topic
	mutex         sync.RWMutex
}

// NewMetadataManager creates a new MetadataManager.
func NewMetadataManager(basePath string, rolloverLimit int64) (MetadataManager, error) {
	m := make(map[string]*TopicMetadata)

	partitionIndices := []int32{}
	for i := range int32(metadataNumPartitions) {
		partitionIndices = append(partitionIndices, i)
	}

	t, err := NewTopic(metadataTopic, partitionIndices, basePath, rolloverLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to init offset manager: %w", err)
	}

	return &metadataManager{metadata: m, metadataTopic: t}, nil
}

func LoadMetadataManager(basePath string, rolloverLimit int64) (MetadataManager, error) {
	metadataPath := fmt.Sprintf("%s/%s", basePath, metadataTopic)
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return NewMetadataManager(basePath, rolloverLimit)
	}

	var mm metadataManager
	if err := mm.restore(metadataPath, rolloverLimit); err != nil {
		return nil, fmt.Errorf("failed to restore metadata: %w", err)
	}
	log.Info("Loaded metadata")
	return &mm, nil
}

func (m *metadataManager) persistToLog(metadata map[string]*TopicMetadata) error {
	now := time.Now()
	for topic, meta := range metadata {
		key := MetadataKey{
			Topic: topic,
		}
		value := MetadataValue{
			TopicMetadata: *meta,
			CreatedAt:     now.Unix(),
		}

		kb, err := proto.GobEncode(key)
		if err != nil {
			return err
		}
		kv, err := proto.GobEncode(value)
		if err != nil {
			return err
		}

		// Special way to distribute metadata messages
		// However, typically this is 0 as we keep to 1 partition for metadata due to the low number of topic messages
		// This is just for consistency sakes
		offsetPartition := int32(utils.PartitionFor(topic, metadataNumPartitions))

		if _, err = m.metadataTopic.Append(offsetPartition, *NewMessage(kb, kv)); err != nil {
			return err
		}
	}

	return nil
}

func (m *metadataManager) Get(topic string) *TopicMetadata {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.metadata[topic]
}

func (m *metadataManager) GetAll() map[string]*TopicMetadata {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.metadata
}

func (m *metadataManager) createAndAssign(brokerConfigs []BrokerConfig, topic string, numPartitions int32, replicationFactor int32) TopicMetadata {
	numBrokers := int32(len(brokerConfigs))

	topicMetadata := TopicMetadata{
		Topic:             topic,
		NumPartitions:     numPartitions,
		PartitionMetadata: []PartitionMetadata{},
	}

	// Random pick a leader broker
	leaderBrokerID := int32(rand.Intn(int(numPartitions - 1)))
	for i := range numPartitions {
		leader := (leaderBrokerID + i) % numBrokers
		partitionMetadata := PartitionMetadata{
			PartitionIndex: i,
			Leader:         leader,
			LeaderAddr:     brokerConfigs[leader].Addr,
			Replicas:       []int32{},
		}

		// Add non-leader replicas
		for j := range replicationFactor {
			partitionMetadata.Replicas = append(partitionMetadata.Replicas, (leader+j+1)%numBrokers)
		}
		topicMetadata.PartitionMetadata = append(topicMetadata.PartitionMetadata, partitionMetadata)
	}

	return topicMetadata
}

// Perform round robin assignment
// For each partition, it should be located in one leader broker and (replicationFactor) replica brokers
// So for example:
// Partition 1 -> Broker 1 (Leader) + Broker 2 & 3 (Followers)
// This also means that replicationFactor <= numPartitions - 1, else it wouldnt make sense
func (m *metadataManager) Init(brokerConfigs []BrokerConfig, topic string, numPartitions int32, replicationFactor int32) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if replicationFactor > numPartitions-1 {
		return fmt.Errorf("replicationFactor cannot be GTE numPartitions")
	}

	mt := m.createAndAssign(brokerConfigs, topic, numPartitions, replicationFactor)

	m.metadata[topic] = &mt

	log.Debug("Created metadata", "metadata", mt)

	if err := m.persistToLog(map[string]*TopicMetadata{
		topic: &mt,
	}); err != nil {
		log.Warnf("failed to persist topic metadata to %s", metadataTopic)
	}

	return nil
}

// Returns the required partition indices to manage for a given broker.
// Each broker should contain either partitions that its a leader for or a replica for.
// i.e. 3 partitions, replicationFactor=2
//
// → Partition 0: leader=broker-0, replica=broker-1
// → Partition 1: leader=broker-1, replica=broker-2
// → Partition 2: leader=broker-2, replica=broker-0
// Broker 0 should only own partitions 0 and 2.
func (m *metadataManager) PartitionsResponsibleFor(brokerID int32, topic string) []int32 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	topicMetadata, ok := m.metadata[topic]
	if !ok {
		return nil
	}

	var indices []int32
	for _, partition := range topicMetadata.PartitionMetadata {
		if partition.Leader == brokerID || slices.Contains(partition.Replicas, brokerID) {
			indices = append(indices, partition.PartitionIndex)
		}
	}
	return indices
}

func (m *metadataManager) update(metadata map[string]*TopicMetadata) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for topic, meta := range metadata {
		if _, ok := m.metadata[topic]; ok {
			log.Warnf("Topic %s already exists. Overwritting..", topic)
		}
		m.metadata[topic] = meta
	}
}

// Overwrites existing metadata for given topics. This will also create the new topic.
func (m *metadataManager) Update(metadata map[string]*TopicMetadata) {
	m.update(metadata)
	if err := m.persistToLog(metadata); err != nil {
		log.Warnf("failed to persist metadata update to %s", metadataTopic)
	}
}

func (m *metadataManager) restore(basePath string, rolloverLimit int64) error {
	t, err := LoadTopic(metadataTopic, basePath, rolloverLimit)
	if err != nil {
		return fmt.Errorf("failed to load metadata topic: %w", err)
	}

	m.metadataTopic = t
	m.metadata = make(map[string]*TopicMetadata)

	restored := 0
	skipped := 0

	// Read metadata logs and run through them to populate metadata map
	for _, partition := range t.partitions {
		for _, lg := range partition.logs {
			// Go down every message to updateOffset
			offset := lg.baseOffset
			for offset < lg.nextOffset {
				msg, err := lg.Read(offset)
				if err != nil {
					break // should be done reading this log
				}

				key := MetadataKey{}
				value := MetadataValue{}

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

				meta := make(map[string]*TopicMetadata)
				meta[key.Topic] = &value.TopicMetadata

				m.update(meta)

				restored++
				offset++
			}
		}
	}

	log.Infof("Restored %d offset(s) from log (%d record(s) skipped)", restored, skipped)

	return nil
}
