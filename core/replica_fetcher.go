package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/charmbracelet/log"
)


type ReplicaFetcher interface {
	Start(ctx context.Context) error
}

type replicaFetcher struct {
	config    *Config
	topic     string
	partition Partition

	// Leader broker that we're replicating from
	leaderBrokerID int32
	// Replica broker aka this broker
	replicaBrokerID int32

	brokerClient BrokerClient
	logger       *log.Logger
}

// NewReplicaFetcher wraps the replication logic for a single partition
// I need to create a connection and perform polling against it for batched offsets
// 1. Load / create replica partition
// 2. Create connecction with leader broker
// 3. Poll for offsets
func NewReplicaFetcher(
	cfg *Config,
	replicaPartition Partition,
	brokerClient BrokerClient,
	topic string,
	replicaID, targetBrokerID int32,
	logger *log.Logger,
) ReplicaFetcher {
	return &replicaFetcher{
		leaderBrokerID:  targetBrokerID,
		replicaBrokerID: replicaID,
		config:          cfg,
		topic:           topic,
		partition:       replicaPartition,
		brokerClient:    brokerClient,
		logger:          deriveLogger(logger, fmt.Sprintf("replica-%d", replicaID)),
	}
}

// For each partition, I want to start a new connection with the leader broker and do my thing
func (rf *replicaFetcher) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := rf.brokerClient.FetchLog(rf.topic, rf.replicaBrokerID, rf.partition.ID())
			if err != nil {
				if errors.Is(err, ErrOffsetNotFound) {
					rf.logger.Debug("no offset found at leader replica")
					continue
				}
				return fmt.Errorf("failed to fetch offset: %w", err)
			}

			// After getting messages, you should write to partition
			for _, msg := range resp.Messages {
				if err := rf.partition.AppendReplicated(*msg); err != nil {
					return fmt.Errorf("failed to append replicated messages: %w", err)
				}
			}

			length := len(resp.Messages)
			if length > 0 {
				rf.logger.Infof("replicated %d messages from leader-%d", length, rf.leaderBrokerID)
			}

			time.Sleep(time.Duration(rf.config.ReplicationDelay) * time.Second)

		}
	}
}
