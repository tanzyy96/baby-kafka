# baby-kafka
Baby learning Kafka internals

Just me trying to learn about Kafka by implementing simple version of its core components.

## Components
- [ ] Broker
	- [x] Partition
	- [x] Segment
	- [x] Segment index
	- [ ] NetworkParser
- [ ] Producer
- [ ] Consumer

## Basic Features
- [x] Append-only log
- [x] Log indexing
- [x] Supporting partition restarts
- [ ] Send messages to a topic
- [ ] Consume messages from a topic
- [ ] Durability — data survives broker restarts

# Roadmap
### Phase 1: The Log
- [x] `Message` struct with `Key` and `Value`
- [x] `Segment` — append to log file with length-prefixed gob encoding
- [x] `Segment` — read from log file by byte position
- [x] `Index` — write fixed-size binary entries (relative offset → byte position)
- [x] `Index` — read byte position by absolute offset
- [ ] `Index` — `LastOffset()` for recovery
- [x] Wire `Segment` + `Index` together — `Append` writes to both, `Read` consults index first
- [x] Segment naming convention (`%020d.log`, `%020d.index`)
- [x] Tests for append/read round-trip
- [x] Tests for durability (close and reopen)

### Phase 2: The Partition
- [x] `Partition` struct owns ordered slice of segments + pointer to active segment
- [x] `Partition.Append` — write to active segment
- [x] `Partition.Read` — find correct segment by base offset, then read
- [ ] `findSegment(offset)` — binary search segments by base offset
- [x] Segment rolling — open new segment when active exceeds `maxBytes`
- [x] Partition recovery on startup — reconstruct segments from directory filenames
- [x] Concurrent read safety with `sync.RWMutex`
- [x] Tests for reads spanning multiple segments
- [x] Tests for recovery after restart

### Phase 3: The Broker
- [x] `Topic` struct owns map of partitions
- [x] `Broker` struct owns map of topics
- [x] Create topic API — specify partition count
- [x] Partition assignment — `hash(key) % numPartitions` for keyed, round-robin for unkeyed
- [x] Integration test — produce 1000 messages, read them all back across partitions

- [ ] Introduce `Config` struct to manage broker configs

### Phase 4: TCP Server + Wire Protocol
- [ ] TCP server that accepts connections
- [ ] Length-prefixed binary request/response protocol
- [ ] `Produce` request handler
- [ ] `Fetch` request handler
- [ ] `ListTopics` request handler
- [ ] Concurrent connection handling with goroutines
- [ ] Graceful shutdown

### Phase 5: Clients
- [ ] `Producer` client — connect to broker, send messages
- [ ] `Consumer` client — connect to broker, fetch from a given offset
- [ ] Retry logic on connection failure
- [ ] End-to-end test — producer and consumer over the network

### Phase 6: Offset Management
- [ ] `OffsetStore` — maps `(groupID, topic, partition)` → committed offset
- [ ] `CommitOffset` request type
- [ ] `FetchOffset` request type
- [ ] Persist offset store to disk
- [ ] Consumer resumes from committed offset on restart
- [ ] Tests for crash recovery — commit, restart, verify resume point

### Phase 7: Hardening
- [ ] Segment rolling by time (not just size)
- [ ] CRC32 checksums on messages — verify on read
- [ ] Index rebuild from log if index is missing or corrupt
- [ ] Structured logging
- [ ] Basic metrics — messages/sec, consumer lag
- [ ] Stress test — concurrent producers and consumers, no messages lost or duplicated

### Phase 8: Replication
- [ ] Each partition has a leader and N followers
- [ ] Followers replicate by fetching from leader
- [ ] Leader tracks in-sync replicas (ISR)
- [ ] Message only committed once all ISR acknowledge
- [ ] Leader election on failure
- [ ] Test — kill leader, verify follower takes over, no data loss
