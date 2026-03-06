package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	"baby-kafka/core"

	"github.com/charmbracelet/log"
	"github.com/common-nighthawk/go-figure"
)

func main() {
	figure := figure.NewFigure("babykafka", "rectangles", true)
	figure.Print()

	debug := flag.Bool("debug", false, "enable debug logs")
	index := flag.Int("index", 0, "broker index")
	datadir := flag.String("datadir", "", "path to data directory")
	replicationFactor := flag.Int("replication", 0, "replication factor for topics")

	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	overrides := []core.Option{}
	if *datadir != "" {
		overrides = append(overrides, core.WithBasePath(*datadir))
	}
	if *replicationFactor != 0 {
		overrides = append(overrides, core.WithReplicationFactor(int32(*replicationFactor)))
	}

	cfg := core.LoadConfig(overrides...)
	log.Info("Loaded config", "config", *cfg)

	if err := os.Mkdir(cfg.BasePath, 0o755); err != nil {
		log.Warnf("Base path %s already exists", cfg.BasePath)
	}

	srv, err := core.NewServer(cfg, int32(*index))
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatal("Server stopped with error:", err)
	}
}
