# baby-kafka

A from-scratch implementation of core Apache Kafka concepts in Go — built as a practical alternative to interview prep grinding, and as a way to genuinely understand a commonly used technology in distributed systems.

## Motivation

Kafka seems to be a rather popular technology used in large companies tackling scalable systems.

Rather than reading another blog post or grinding through more algorithm problems, I decided to build a simplified version from scratch. It's a more engaging way to study a system I'd been encountering in interview prep, and it gave me a chance to revisit Go and work on something I rarely touch in typical web development: low-level binary encoding and a custom TCP wire protocol.

## What's Implemented

### Append-Only Log

At its core, Kafka is a file. Messages are appended to a `.log` file using a length-prefixed binary format. Alongside each log file is a `.index` file mapping offsets to byte positions, enabling O(1) seeks without scanning the entire log.

### Partitions and Log Segments

A partition is a sequence of log segments. When a segment exceeds a configurable size threshold (`rollover_limit`), a new segment is created — named after its starting offset. On startup, the partition reconstructs its state purely from the filenames on disk, which is how Kafka survives restarts without a separate metadata store.

### Broker and Topic Routing

The broker owns a map of topics; each topic owns its partitions. Routing follows Kafka's actual semantics:
- **Keyed messages** always route to the same partition via `fnv32a(key) % numPartitions`, guaranteeing ordering per key.
- **Unkeyed messages** round-robin across partitions via an atomic counter.

### TCP Wire Protocol

A custom binary framing protocol over raw TCP:

```
Request:  [4-byte length][1-byte message type][gob-encoded payload]
Response: [4-byte length][gob-encoded Response{Status, Error, Data}]
```

Message types: `1=Produce`, `2=Consume`, `3=CreateTopic`, `4=ListTopics`

Each connection is handled in a goroutine. This was the most interesting part of the project — thinking carefully about how to frame and delimit messages on a stream, rather than relying on HTTP or an existing RPC framework.

### Clients

Producer, consumer, and admin CLI clients that communicate with the broker over the wire protocol. End-to-end produce and consume flows work.

## Architecture

```
TCP Server (:8080)
    └── Broker          (owns topic map)
         └── Topic      (routes to partitions)
              └── Partition  (manages log segments)
                   ├── Log       (append-only, length-prefixed)
                   └── LogIndex  (offset → byte position)
```

## Data on Disk

```
./data/<topic>/partition-<N>/
    00000000000000000000.log    # segment starting at offset 0
    00000000000000000000.index
    00000000000000000100.log    # rolled over at offset 100
    00000000000000000100.index
```

The segment filename encodes its base offset. This is how the partition knows where each segment begins when recovering from disk on startup.

## Running It

```bash
make run-broker        # Start the broker
make setup             # Create a topic and send test messages
make run-producer      # Send a message interactively
make run-consumer      # Read messages back
make fresh             # Wipe data directory and restart
```

Or use the CLI tools directly:

```bash
go run cmd/admin/main.go --create=mytopic --num=3
go run cmd/producer/main.go --topic=mytopic --key=mykey
go run cmd/consumer/main.go --topic=mytopic --partition=0 --offset=0
```

Configuration lives in `config.json`:

```json
{
  "base_path": "./data",
  "rollover_limit": 1048576,
  "server_port": ":8080"
}
```

## In Progress

- **Consumer group offset persistence** — `core/offset_manager.go` exists but isn't fully integrated
- **Replication** — leader/follower, ISR, leader election

See [TODO.md](./TODO.md) for the full list.
