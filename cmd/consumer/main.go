package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"

	"github.com/charmbracelet/log"
)

func main() {
	cfg := core.LoadConfig()

	topic := flag.String("topic", "test", "topic")
	partitions := flag.String("partitions", "0", "comma-separated list of partitions")
	groupID := flag.String("group", "testGroup", "consumer group id")
	debug := flag.Bool("debug", false, "enable debug logging")

	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
		log.Info("Debug logging enabled")
	}

	partitionList := strings.Split(*partitions, ",")
	partitionIDs := make([]int32, len(partitionList))

	for i, p := range partitionList {
		id, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			log.Fatalf("Invalid partition ID: %s", p)
		}
		partitionIDs[i] = int32(id)
	}

	// Random id
	id := int(rand.Int31n(1000))

	runConsumer(id, cfg, *groupID, *topic, partitionIDs)
}

func runConsumer(id int, cfg *core.Config, groupID, topic string, partitionIDs []int32) {
	consumerID := fmt.Sprintf("%s-%d", groupID, id)
	c, err := client.NewConsumer(consumerID, cfg, groupID, topic, partitionIDs)
	if err != nil {
		log.Fatalf("Failed to create consumer %d: %s", id, err)
	}

	offsets, err := c.FetchAllOffsets()
	if err != nil {
		log.Warnf("Failed to fetch previous offset, restarting from 0: %s", err.Error())
	}

	for partitionIndex, offset := range offsets {
		broker, err := c.BrokerFor(partitionIndex)
		if err != nil {
			log.Warnf("Failed to get broker for partition %d: %s", partitionIndex, err.Error())
			continue
		}
		log.Info("Resuming offset", "partitionIndex", partitionIndex, "offset", offset, "broker", broker)
	}

	// Setup channel to receive messsages
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := c.Run(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	errDelay := 1 * time.Second

	for {
		select {
		case result := <-resultCh:
			errMsg := ""
			if result.Err == nil {
				log.Info("Consumer received message", "id", id, "topic", topic, "partition", result.PartitionIndex, "offset", result.Offset, "key", string(result.Key), "value", string(result.Value), "err", errMsg)

				time.Sleep(cfg.Consumer.PollInterval * time.Second)
				errDelay = 1 * time.Second
			} else {
				errMsg = result.Err.Error()
				log.Warn("Consumer failed to receive message", "error", errMsg)

				time.Sleep(errDelay)
				if errDelay*2 < cfg.Consumer.MaxErrorInterval {
					errDelay *= 2
				} else {
					errDelay = cfg.Consumer.MaxErrorInterval
				}
			}

		case <-quit:
			cancel()
			c.Close()
			return
		}
	}
}
