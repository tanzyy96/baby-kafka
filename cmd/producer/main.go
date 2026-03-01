package main

import (
	"flag"
	"fmt"
	"sync"

	"baby-kafka/core"
	"baby-kafka/core/client"

	"github.com/charmbracelet/log"
)

func main() {
	cfg := core.LoadConfig()

	count := flag.Int("count", 1, "number of consumers")
	topic := flag.String("topic", "test", "topic")

	flag.Parse()

	wg := sync.WaitGroup{}

	for i := 0; i < *count; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			runProducer(id, cfg.ServerPort, *topic)
		}(i)
	}

	wg.Wait()
}

func runProducer(id int, addr, topic string) {
	p, err := client.NewProducer(addr)
	if err != nil {
		log.Fatalf("Failed to create producer %d: %s", id, err)
	}

	key := "key"
	value := fmt.Sprintf("Sent by producer %d", id)
	if _, err := p.Send(topic, []byte(key), []byte(value)); err != nil {
		log.Fatal("Failed to send message:", err)
	}
}
