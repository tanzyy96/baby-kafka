package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"

	"github.com/charmbracelet/log"
)

func main() {
	cfg := core.LoadConfig()

	numProducers := flag.Int("producers", 1, "number of producers")
	topic := flag.String("topic", "test", "topic")
	key := flag.String("key", "key", "key for message")
	count := flag.Int("count", 1, "number of messages to send")

	// TODO: producer should precalculate index of producer with GetTopicMetadata
	// Each producer can send to any broker, depending on the key
	// For now we hardcode the index
	index := flag.Int("index", 0, "index of target broker")

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *numProducers; i++ {
		wg.Add(1)

		go func(id int) {
			if *index >= len(cfg.Brokers) {
				log.Fatalf("Invalid index %d for brokers", *index)
			}
			addr := cfg.Brokers[*index].Port
			defer wg.Done()

			runProducer(id, addr, *topic, *key, *count)
		}(i)
	}

	wg.Wait()
}

func runProducer(id int, addr, topic, key string, numMessages int) {
	p, err := client.NewProducer(addr)
	if err != nil {
		log.Fatalf("Failed to create producer %d: %s", id, err)
	}

	hr, min, sec := time.Now().Clock()
	value := fmt.Sprintf("Sent by producer %d at %d:%d:%d", id, hr, min, sec)

	for i := 0; i < numMessages; i++ {
		if _, err := p.Send(topic, []byte(key), []byte(value)); err != nil {
			log.Fatal("Failed to send message:", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
