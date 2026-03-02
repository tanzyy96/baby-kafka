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

	count := flag.Int("count", 1, "number of consumers")
	topic := flag.String("topic", "test", "topic")
	key := flag.String("key", "key", "key for message")

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *count; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			runProducer(id, cfg.ServerPort, *topic, *key)
		}(i)
	}

	wg.Wait()
}

func runProducer(id int, addr, topic, key string) {
	p, err := client.NewProducer(addr)
	if err != nil {
		log.Fatalf("Failed to create producer %d: %s", id, err)
	}

	hr, min, sec := time.Now().Clock()
	value := fmt.Sprintf("Sent by producer %d at %d:%d:%d", id, hr, min, sec)
	if _, err := p.Send(topic, []byte(key), []byte(value)); err != nil {
		log.Fatal("Failed to send message:", err)
	}
}
