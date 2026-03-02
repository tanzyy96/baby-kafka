package main

import (
	"context"
	"flag"
	"os"
	"os/signal"

	"baby-kafka/core"

	"github.com/charmbracelet/log"
)

func main() {
	debug := flag.Bool("debug", false, "enable debug logs")

	flag.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	cfg := core.LoadConfig()
	srv, err := core.NewServer(cfg)
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatal("Server stopped with error:", err)
	}
}
