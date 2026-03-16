package main

import (
	"flag"
	"fmt"
	"os"
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

	// Producer should call one broker aka bootstrap, and then fetch the topic metadata
	// It should get all the partition distribution such that for given topic + key,
	// it can figure out which broker to send the message to

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *numProducers; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			runProducer(id, cfg, *topic, *key, *count)
		}(i)
	}

	wg.Wait()
}

func runProducer(id int, cfg *core.Config, topic, key string, numMessages int) {
	p, err := client.NewProducer(cfg, log.NewWithOptions(os.Stderr, log.Options{}))
	if err != nil {
		log.Fatalf("Failed to create producer %d: %s", id, err)
	}
	defer p.Close()

	if err := p.ConnectBootstrap(); err != nil {
		log.Fatalf("Failed to connect to bootstrap broker for producer %d: %s", id, err)
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
