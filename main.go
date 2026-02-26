package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"baby-kafka/core"
)

func main() {
	// For now, we can just start the server and listen for connections
	// In a real implementation, we would also want to handle graceful shutdowns, configuration, etc.
	// But for the sake of this exercise, we can keep it simple
	srv, err := core.NewServer("8080", 1024*1024*10, "data") // 10MB rollover for testing
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		fmt.Println("Server stopped with error:", err)
	}

	srv.Stop()
}
