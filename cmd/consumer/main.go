package main

import (
	"errors"
	"flag"
	"sync"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"

	"github.com/charmbracelet/log"
)

func main() {
	cfg := core.LoadConfig()

	count := flag.Int("count", 1, "number of consumers")
	topic := flag.String("topic", "test", "topic")
	partition := flag.Int("partition", 0, "partition")
	groupId := flag.String("group", "testGroup", "consumer group id")

	// TODO: consumer should precalculate the broker address based GetTopicMetadata + topic + partition
	// Consumers only target one leader partition, so it needs to figure that out
	// For now we hardcode the index
	index := flag.Int("index", 0, "index of target broker")

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *count; i++ {
		wg.Add(1)

		go func(id int) {
			if *index < 0 || *index >= len(cfg.Brokers) {
				log.Fatalf("Invalid index %d for broker address list", *index)
			}
			port := cfg.Brokers[*index].Addr

			defer wg.Done()
			runConsumer(id, port, *groupId, *topic, int32(*partition))
		}(i)
	}

	wg.Wait()
}

func runConsumer(id int, addr, groupId, topic string, partition int32) {
	c, err := client.NewConsumer(addr, groupId, topic, partition, 0)
	if err != nil {
		log.Fatalf("Failed to create consumer %d: %s", id, err)
	}

	offset, err := c.FetchOffset()
	if err != nil {
		if errors.Is(err, core.ErrOffsetNotFound) {
			log.Info("No prior offset found.")
		} else {
			log.Warnf("Failed to fetch previous offset, restarting from 0: %s", err.Error())
		}
	}

	log.Infof("Resuming from offset: %d", offset)

	for {
		log.Info("Polling...", "id", id, "groupId", groupId, "topic", topic, "partition", partition)
		key, value, offset, err := c.Poll()
		if err != nil {
			if errors.Is(err, core.ErrNoMessagesAtOffset) {
				time.Sleep(3 * time.Second) // Backoff before polling again
				continue                    // No messages, just poll again
			} else {
				log.Warnf("Consumer %d failed to poll: %s", id, err)
			}
		}
		log.Info("Consumer received message", "id", id, "partition", partition, "offset", offset, "key", string(key), "value", string(value))
		if err := c.CommitOffset(offset); err != nil {
			log.Warn("Consumer failed to commit offset")
		}
		log.Info("Committed offset", "offset", offset)
		time.Sleep(1 * time.Second)
	}
}
