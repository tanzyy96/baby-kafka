package main

import (
	"flag"
	"os"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

type CreateFlag struct {
	Topic string
	IsSet bool
}

func (cf *CreateFlag) String() string {
	return cf.Topic
}

func (cf *CreateFlag) Set(value string) error {
	cf.Topic = value
	cf.IsSet = true
	return nil
}

func main() {
	cfg := core.LoadConfig()

	// Support --create and --list commands
	list := flag.Bool("list", false, "List topics")
	create := &CreateFlag{
		Topic: "test",
	}

	flag.Var(create, "create", "Create a topic with the given name")
	numPartitions := flag.Int("num", 1, "numPartitions")

	// TODO: I think this is supposed to actually propagate throughout all the brokers
	index := flag.Int("index", 0, "index of target broker")

	flag.Parse()

	if *index < 0 || *index >= len(cfg.Brokers) {
		log.Fatalf("Invalid index: %d", *index)
	}

	if *list {
		log.Printf("Listing topic...")
		adminListTopics(cfg.Brokers[*index].Addr)
	} else if create.IsSet {
		adminCreateTopic(cfg.Brokers[*index].Addr, create.Topic, *numPartitions)
	} else {
		log.Printf("No command provided. Use --create to create a topic or --list to list topics.")
	}
}

func adminCreateTopic(addr, topic string, numPartitions int) {
	admin, err := client.NewAdmin(addr, log.NewWithOptions(os.Stderr, log.Options{}))
	if err != nil {
		log.Fatalf("Failed to create admin client: %s", err)
	}

	resp, err := admin.CreateTopic(topic, int32(numPartitions))
	if err != nil {
		log.Fatalf("Failed to create topic: %s", err)
	}

	if resp.Status != proto.StatusOK {
		log.Fatalf("Failed to create topic: %s", resp.Error)
	}
	log.Infof("Topic '%s' created successfully", topic)
}

func adminListTopics(addr string) {
	admin, err := client.NewAdmin(addr, log.NewWithOptions(os.Stderr, log.Options{}))
	if err != nil {
		log.Fatalf("Failed to create admin client: %s", err)
	}

	resp, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %s", err)
	}

	log.Printf("Topics: %v", resp.Topics)
}
