# Mini-Kafka in Go — Claude Session Summary

This is a summary of the back-and-forth between me and Claude to understand the fundamentals of Kafka. As this is just a conversation summary and not actual reflection of the implementation, don't take this as a summary of the code. The purpose of this is more for note-taking. These are mostly for the basic internals around messaging and data storage, for replication knowledge head over to REPLICATION.md.

## What We're Building

A mini-Kafka implementation in Go to learn Kafka internals. The goal is understanding the storage engine, networking, and core concepts — not a production system. 

---

## Core Concepts

### Kafka Mental Model
- **Topic** — a named stream of messages
- **Partition** — topics split into partitions for parallelism; ordering guaranteed within a partition only
- **Segment** — how a partition is physically stored on disk; a partition is a collection of segment files
- **Offset** — logical sequential message number within a partition
- **Consumer Group** — multiple consumers sharing partitions; each partition goes to one consumer in the group

### Ordering
Kafka only guarantees ordering **within a single partition**. Use message keys to route related messages to the same partition: `hash(key) % numPartitions`. All events for the same key land in the same partition, in order.

---

## Storage Layer

### Message Format
```go
type Message struct {
    Key   []byte
    Value []byte
}
```
Using `gob` encoding for simplicity. Gob is not self-delimiting — wrap each encoded message with a **length prefix**:
```
┌──────────┬─────────────────┐
│ len (4B) │   gob payload   │
└──────────┴─────────────────┘
```

### Segment vs Partition
- **Segment** — a single append-only `.log` file + its `.index` file. The unit of storage on disk.
- **Partition** — owns an ordered slice of segments. Exposes `Append` and `Read`. Only the active (newest) segment accepts writes; all others are closed and immutable.

A partition rolls to a new segment when the active one exceeds `maxBytes`.

### Segment Naming
```go
fmt.Sprintf("%020d", baseOffset)  // e.g. 00000000000001048576
```
The filename **is** the base offset. Zero-padded to 20 digits so lexicographic sort = numeric sort. This is load-bearing — it enables segment discovery, offset routing, and crash recovery without any metadata file.

### The Index
Fixed-size binary entries mapping relative offset → byte position in the log file:
```
┌──────────────────┬──────────────────┐
│ rel offset (4B)  │  byte pos (4B)   │
└──────────────────┴──────────────────┘
         8 bytes per entry
```
Relative offset = absolute offset − segment base offset. Entry `n` is always at byte `n * 8` — O(1) lookup, no scanning.

**Lookup flow:**
```
Consumer requests offset 1003
  → findSegment(1003) finds segment with baseOffset 1000
  → index.Read(1003): relativeOffset = 3, jump to byte 24
  → bytePos = 140
  → log.ReadAt(140) → decode → Message
```

### File I/O Layers
- `os.File` — raw file access; `ReadAt`/`WriteAt` are concurrent-safe
- `bufio.Writer` — batch writes in memory, flush as one syscall
- `encoding/binary` — structured serialization
- `io.ReadFull` — always use this over `Read` for network/file reads; `Read` may return fewer bytes than requested

### Durability (Current Phase)
Not worrying about `fsync` yet. Focus is on **recovery on restart**:
1. Scan partition directory, parse filenames as base offsets
2. Reconstruct ordered segment list
3. Read last index entry → `nextOffset = lastOffset + 1`
4. Highest base offset segment = active segment

Topic config (partition count, max segment size) stored in a `config.json` per topic directory — separate from state which is reconstructed from filenames.

---

## Project Structure

```
cmd/
  broker/main.go      ← starts TCP server
  producer/main.go    ← sends messages
  consumer/main.go    ← polls messages
internal/
  log/
    segment.go        ← Segment, Index, validateFilename, segmentPath
    index.go
  partition/
    partition.go
  broker/
    broker.go
  protocol/
    protocol.go
```

`internal/` is a special Go directory — packages inside can only be imported within the same module. Enforced by the compiler.

---

## Broker, Topic, Partition Hierarchy

```
Broker
  └── Topic "orders"
        ├── Partition 0  (many segments)
        └── Partition 1  (many segments)
  └── Topic "payments"
        └── Partition 0
```

- **Broker** — registry of topics + TCP server. The seam between storage and networking. Storage layer has no knowledge of networking; protocol layer has no knowledge of segments.
- **Topic** — owns partitions, routes messages. Round-robin for keyless messages (use `atomic.Uint64` for the counter). Key-based routing: `hash(key) % numPartitions`. Use `fnv.New32a` from stdlib.
- **Partition** — owns segments. Protected by `sync.RWMutex` — multiple consumers can read simultaneously, writes are exclusive.

**Why RWMutex:** Many consumer goroutines + producer goroutines + (later) replication goroutines all hit the same partition concurrently. Each TCP connection gets its own goroutine.

---

## TCP Server + Wire Protocol

### Request Frame
```
┌──────────────┬─────────────┬──────────────────┐
│  length (4B) │  type (1B)  │  payload (var)   │
└──────────────┴─────────────┴──────────────────┘
```
Length covers type + payload.

### Response Frame — always a fixed envelope
```go
type Response struct {
    Status  uint8   // 0 = success, 1 = error
    Err     string  // populated on error
    Data    []byte  // gob encoded operation-specific struct
}
```
Client always decodes `Response` first, checks `Status`, then decodes `Data` into the operation-specific struct.

### Request Types
```go
const (
    TypeProduce     uint8 = 1
    TypeFetch       uint8 = 2
    TypeCreateTopic uint8 = 3
    TypeCommitOffset uint8 = 4
    TypeFetchOffset  uint8 = 5
)
```

### Key Implementation Notes
- Use `io.ReadFull` not `conn.Read` — TCP may return partial data
- Wrap conn in `bufio.Writer` for responses, flush at end of each response
- `go handleConn(conn)` per accepted connection
- Reset read deadline before each read if using deadlines; don't set once on accept

---

## Producer & Consumer Clients

### Producer
- Single TCP connection per instance, established in constructor
- `Send(topic, key, value)` returns `(partition, offset, error)`
- Not safe for concurrent use — one producer per goroutine
- Advance state only after successful response

### Consumer
- Owns current offset — the core piece of state
- `Poll()` fetches the message at current offset, advances offset only on success
- Returns `ErrNoMessages` sentinel when caught up — caller sleeps and retries
- Starting offset: `earliest` (0), `latest`, or a specific number
- On reconnect: re-dial, keep offset unchanged — resume from same position

### Handling Connection Errors
```go
if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
    consumer.reconnect()  // recoverable
    continue
}
// otherwise fatal
```

---

## Gob Gotchas

Always pass a **pointer to a concrete type** to `gob.Decode` — never an interface value:
```go
var resp ProduceResponse
readResponse(r, &resp)  // correct — pass *ProduceResponse
```
Passing `resp interface{}` causes gob to see `*interface{}` which it cannot decode into.

Use `bytes.NewReader` (not `bytes.NewBuffer`) when only reading from a byte slice — it's designed for read-only use and supports seeking.

---

## Makefile

```makefile
.PHONY: setup clean run-broker run-producer run-consumer

DATA_DIR  := ./data
TOPIC     := test
COUNT     := 10
PARTITION := 0
ADDR      := localhost:9092

setup:
	mkdir -p $(DATA_DIR)

clean:
	rm -rf $(DATA_DIR)
	mkdir -p $(DATA_DIR)

run-broker:
	go run cmd/broker/main.go --addr=:9092 --data=$(DATA_DIR)

run-producer:
	go run cmd/producer/main.go --addr=$(ADDR) --topic=$(TOPIC) --count=$(COUNT)

run-consumer:
	go run cmd/consumer/main.go --addr=$(ADDR) --topic=$(TOPIC) --partition=$(PARTITION)
```

Override variables at the command line: `make run-producer TOPIC=orders COUNT=100`

Use `$(MAKE)` not `make` when calling targets from other targets. Dependencies (`target: dep1 dep2`) are not sequential commands — put sequential commands as shell lines under the target.

---

## Build Phases

| Phase | Focus |
|---|---|
| 1 | Segment + Index — append-only log with offset lookup |
| 2 | Partition — segment rolling, multi-segment reads, recovery |
| 3 | Broker — topic/partition registry, key routing, round-robin |
| 4 | TCP Server — wire protocol, produce/fetch/create handlers |
| 5 | Clients — Producer and Consumer over the network |
| 6 | Offset Management — commit/fetch offsets, persist to disk |
| 7 | Hardening — CRC checksums, index rebuild, metrics, stress test |
| 8 | Replication — leader/follower, ISR, leader election |

Phases 1–3 = working in-process Kafka. Phases 1–6 = real networked system.
