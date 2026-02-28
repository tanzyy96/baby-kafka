package main

import (
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
		key, value, err := c.Poll()
		if err != nil {
			if err == core.ErrNoMessagesAtOffset {
				time.Sleep(5 * time.Second) // Backoff before polling again
				continue                    // No messages, just poll again
			} else {
				log.Fatalf("Consumer %d failed to poll: %s", id, err)
			}
		}
		log.Infof("Consumer %d received message: key=%s value=%s", id, string(key), string(value))
	}
}
