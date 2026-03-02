# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**baby-kafka** is an educational Go implementation of core Kafka components from scratch. It implements a simplified message broker with topics, partitions, append-only logs, and a TCP wire protocol.

## Commands

```bash
# Run tests
go test ./...
go test ./core/... -v          # verbose tests for core package
go test ./core/... -run TestX  # run a single test

# Start the broker server
go run main.go

# Makefile shortcuts
make run-broker    # Start broker
make run-producer  # Run producer client
make run-consumer  # Run consumer client
make setup         # Create test topic and send 5 messages
make fresh         # Clean data dir and restart broker
make clean         # Remove ./data directory
```

**CLI tools** (each reads `config.json` automatically):
```bash
go run cmd/admin/main.go --create=mytopic --num=3   # Create topic with 3 partitions
go run cmd/admin/main.go --list                     # List topics
go run cmd/producer/main.go --topic=mytopic --key=mykey
go run cmd/consumer/main.go --topic=mytopic --partition=0 --offset=0
```

## Architecture

```
TCP Server (:8080)
    └── Broker          (owns map of topics)
         └── Topic      (routes to partitions)
              └── Partition  (manages log segments)
                   ├── Log       (append-only, length-prefixed)
                   └── LogIndex  (offset → byte position)
```

### Key Components

- **`core/server.go`** — TCP server; dispatches requests to broker; graceful shutdown on SIGINT
- **`core/broker.go`** — Owns the topic map; routes Produce/Consume/Admin requests
- **`core/topic.go`** — Routes messages: keyed → `fnv32a(key) % numPartitions`; unkeyed → round-robin atomic counter
- **`core/partition.go`** — Ordered, immutable message sequence; manages segment rollover; `sync.RWMutex` for concurrency; recovers from disk on startup
- **`core/log.go`** — Single append-only log file; encoding: 8-byte length prefix + gob-encoded `Message`
- **`core/log_index.go`** — Index file: 8 bytes per entry (4-byte relative offset + 4-byte byte position); enables O(1) reads
- **`core/proto/proto.go`** — Wire protocol: 4-byte length prefix + 1-byte message type + gob payload
- **`core/client/`** — Producer, Consumer, Admin clients used by CLI commands

### Wire Protocol

Request: `[4-byte length][1-byte type][gob payload]`
Response: `[4-byte length][gob-encoded Response{Status, Error, Data}]`

Message types: `1=Produce`, `2=Consume`, `3=CreateTopic`, `4=ListTopics`

### Data on Disk

```
./data/<topic>/partition-<N>/
    00000000000000000000.log    # append-only message store
    00000000000000000000.index  # offset→position index
    00000000000000000100.log    # rolled-over segment at offset 100
    00000000000000000100.index
```

Log rollover is triggered by `rollover_limit` bytes (default 1MB) from `config.json`.

### Configuration (`config.json`)

```json
{
  "base_path": "./data",
  "rollover_limit": 1048576,
  "server_port": ":8080"
}
```

## In Progress / Not Yet Implemented

- Consumer group offset persistence (`core/offset_manager.go` exists but not fully integrated)
- CRC32 checksums, retry logic, metrics, replication
