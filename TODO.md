# TODO

## Phase 1: The Log
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

## Phase 2: The Partition
- [x] `Partition` struct owns ordered slice of segments + pointer to active segment
- [x] `Partition.Append` — write to active segment
- [x] `Partition.Read` — find correct segment by base offset, then read
- [ ] `findSegment(offset)` — binary search segments by base offset
- [x] Segment rolling — open new segment when active exceeds `maxBytes`
- [x] Partition recovery on startup — reconstruct segments from directory filenames
- [x] Concurrent read safety with `sync.RWMutex`
- [x] Tests for reads spanning multiple segments
- [x] Tests for recovery after restart

## Phase 3: The Broker
- [x] `Topic` struct owns map of partitions
- [x] `Broker` struct owns map of topics
- [x] Create topic API — specify partition count
- [x] Partition assignment — `hash(key) % numPartitions` for keyed, round-robin for unkeyed
- [x] Introduce `Config` struct to manage broker configs
- [ ] Integration test — produce 1000 messages, read them all back across partitions
- [x] Persist broker configs in `config.json`
- [x] Loads broker if existing data directories exist via `LoadBroker`

## Phase 4: TCP Server + Wire Protocol
- [x] TCP server that accepts connections
- [x] Length-prefixed binary request/response protocol
- [x] `Produce` request handler
- [x] `Fetch` request handler
- [x] `ListTopics` request handler
- [x] Concurrent connection handling with goroutines
- [x] Graceful shutdown

## Phase 5: Clients
- [x] `Producer` client — connect to broker, send messages
- [x] `Consumer` client — connect to broker, fetch from a given offset
- [x] End-to-end test — producer and consumer over the network
- [ ] Retry logic on connection failure

## Phase 6: Offset Management
- [x] `OffsetStore` — maps `(groupID, topic, partition)` → committed offset
- [x] `CommitOffset` request type
- [x] `FetchOffset` request type
- [x] Persist offset store to disk
- [x] Consumer resumes from committed offset on restart
- [x] Tests for crash recovery — commit, restart, verify resume point

## Phase 7: Hardening
- [ ] Segment rolling by time (not just size)
- [x] CRC32 checksums on messages — verify on read
- [ ] Index rebuild from log if index is missing or corrupt
- [x] Structured logging
- [ ] Basic metrics — messages/sec, consumer lag
- [ ] Stress test — concurrent producers and consumers, no messages lost or duplicated

## Phase 7.5: __consumer_offsets partitioning
- [x] Route offset commits/fetches to correct partition via hash(groupId) % numPartitions
- [x] Create __consumer_offsets through normal CreateTopic flow (gets partition assignment + replication)
- [x] Forward commits/fetches to the correct partition leader (group coordinator pattern)
- [x] See REPLICATION_CONCEPTS.md for full explanation

## Phase 7.9 Cleanup
- [ ] Refactor partition logic to producer-side instead of broker side

## Phase 8: Replication
- [x] Each partition has a leader and N followers
- [x] Followers replicate by fetching from leader
- [ ] Leader tracks in-sync replicas (ISR)
- [ ] Message only committed once all ISR acknowledge
- [ ] Leader election on failure
- [ ] Test — kill leader, verify follower takes over, no data loss
