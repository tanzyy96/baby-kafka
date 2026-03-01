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

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *count; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			runConsumer(id, cfg.ServerPort, *topic, int32(*partition))
		}(i)
	}

	wg.Wait()
}

func runConsumer(id int, addr, topic string, partition int32) {
	c, err := client.NewConsumer(addr, topic, partition, 0)
	if err != nil {
		log.Fatalf("Failed to create consumer %d: %s", id, err)
	}

	for {
		log.Info("Polling...", "id", id, "topic", topic, "partition", partition)
		key, value, offset, err := c.Poll()
		if err != nil {
			if errors.Is(err, core.ErrNoMessagesAtOffset) {
				log.Info("Nothing found. Waiting befor retrying...")
				time.Sleep(3 * time.Second) // Backoff before polling again
				continue                    // No messages, just poll again
			} else {
				log.Warnf("Consumer %d failed to poll: %s", id, err)
			}
		}
		log.Info("Consumer received message", "id", id, "partition", partition, "offset", offset, "key", string(key), "value", string(value))
		time.Sleep(1 * time.Second)
	}
}
