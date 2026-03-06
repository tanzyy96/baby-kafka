# Replication Concepts

## Why Replication?

Single broker = single point of failure. If it crashes, all data is gone. Replication keeps copies of each partition's log on multiple brokers so the system survives failures.

---

## Key Terms

**Leader** — the broker that accepts writes for a partition. There is exactly one leader per partition at any time.

**Follower** — a broker that holds a copy of a partition's log by replicating from the leader.

**Replica** — any broker holding a copy of a partition (leader + followers combined).

**Replication Factor** — how many total brokers hold a copy of each partition (including the leader).
```
replicationFactor=1  →  leader only, no redundancy (current state)
replicationFactor=2  →  leader + 1 follower
replicationFactor=3  →  leader + 2 followers  (standard production)
```

**ISR (In-Sync Replicas)** — the subset of replicas that are caught up to the leader. A message is only "committed" once all ISR members have it.

**High Watermark (HW)** — the highest offset that all ISR members agree on. Consumers can only read up to HW — messages above it may not be replicated yet.

**Election Term** — a monotonically increasing number that increments on every election. Every inter-broker message carries the current term. A broker receiving a message with a stale term (lower than its own) knows the sender is out of date and rejects it. Prevents split-brain when an old leader comes back.

---

## Partition Math

For a topic with 5 partitions and replicationFactor=3 across 3 brokers:

```
5 partitions × 3 replicas = 15 total replicas

  5  leader replicas   (1 per partition)
  10 follower replicas (2 per partition)

broker-0: leads P0, P3        follows P1, P2, P4
broker-1: leads P1, P4        follows P0, P2, P3
broker-2: leads P2            follows P0, P1, P3, P4
```

Every broker simultaneously leads some partitions and follows others. Both roles run in the same process.

Constraint: `replicationFactor <= numBrokers`. You can't put 2 copies on the same broker.

---

## Partition Assignment (Round-Robin)

When a topic is created, partitions are assigned to brokers via round-robin. A random starting broker is picked to spread leadership evenly across topics:

```go
start := rand.Intn(numBrokers)
for i := 0; i < numPartitions; i++ {
    leader  = brokers[(start + i) % numBrokers]
    replica = brokers[(start + i + 1) % numBrokers]  // for each additional replica
}
```

The leader is just the first broker in the replica list — no separate election needed at creation time.

---

## Two Types of State

### BrokerConfig (static, from config.json)
Who exists in the cluster. Doesn't change while running.
```json
{ "brokers": [
    {"index": 0, "port": ":8080"},
    {"index": 1, "port": ":8081"},
    {"index": 2, "port": ":8082"}
]}
```
This is the **phonebook** — maps broker IDs to addresses.

### TopicMetadata (dynamic, changes over time)
Who owns what. Changes on CreateTopic, ISR updates, and leader election.
```
"orders" partition 0 → leader=broker-0, replicas=[broker-1, broker-2], ISR=[broker-0, broker-1], term=1
"orders" partition 1 → leader=broker-1, replicas=[broker-2, broker-0], ISR=[broker-1, broker-2], term=1
```
This is the **ownership map** — references brokers by ID, look up addresses in BrokerConfig.

The assignment algorithm sits at the boundary: reads BrokerConfig to know what brokers exist, writes TopicMetadata to record who owns what.

---

## Propagation Models

Three types of data, three different propagation strategies:

| Data | Direction | Why |
|---|---|---|
| TopicMetadata | **Push** on change | Critical, must be immediately consistent, changes rarely |
| Message logs | **Pull** by followers | High volume, eventual consistency ok, follower controls rate |
| `__consumer_offsets` | **Pull** by followers | It's log data — same category as message logs |

**Push** = the broker that has new info immediately sends it to all peers.
**Pull** = followers periodically ask "give me everything since offset X."

### Why `__consumer_offsets` is Pull (not push)
Even though it feels like an event ("a commit just happened"), it's stored as a topic with partitions and logs — structurally identical to user topic data. So it replicates the same way. The worst case of staleness is a consumer re-reads a few messages (at-least-once delivery), which is acceptable.

---

## Special Internal Topics

### `__consumer_offsets`
Stores consumer group committed offsets. Key = `(groupId, topic, partition)`, value = `offset`.

On restore: replay the whole log, last-write-wins per key (same as current `OffsetManager.restore()`).

Since every broker replicates this topic, any broker can answer `FetchOffset` for any consumer group — no need to route to a specific broker. This means `FetchOffset` and `CommitOffset` don't need metadata routing.

### `__topic_metadata`
Stores partition assignment (who leads what). Same key/value log pattern. Every broker replicates it, so any broker has the full cluster picture and can answer `GetMetadata`.

---

## Concurrent CreateTopic Problem

If two brokers each receive `CreateTopic("orders")` simultaneously, they compute different round-robin assignments and overwrite each other's metadata — inconsistent state.

**Real Kafka solution**: A single **controller broker** handles all metadata changes. All `CreateTopic` requests are forwarded to it, serialised, then broadcast. No concurrent writes possible.

**KRaft (Kafka 3.x)**: Uses Raft consensus on a `__cluster_metadata` log. Total ordering guaranteed.

**Baby-kafka approach**: Per-topic upsert (each sync only touches one topic key). Prevents different-topic conflicts. Same-topic concurrent creates are prevented by the duplicate check in `CreateTopic` — acknowledged limitation that the check itself has a race window.

---

## Every Broker Plays Both Roles

A broker running with 3 topics might be:
- Leader for `orders` partition 0 → accepts writes, tracks ISR, manages HW
- Follower for `orders` partition 1 → runs fetch loop against broker-1
- Leader for `payments` partition 2 → accepts writes, tracks ISR
- Follower for `events` partition 0 → runs fetch loop against broker-2

Both the leader machinery (ISR tracking, HW advancement) and follower machinery (fetch loops, acks) run concurrently in the same process for different partitions.

---

## Flow Summary

```
CreateTopic("orders", numPartitions=3, replicationFactor=2):
  1. Round-robin assign → P0:broker0, P1:broker1, P2:broker2 (with replicas)
  2. Save to local __topic_metadata
  3. Push TopicMetadata to all peers (they save too)
  4. Each broker creates local partition dirs for only the partitions it owns

Produce to "orders":
  1. Producer calls GetMetadata → learns broker-0 leads partition 0
  2. Producer sends write to broker-0
  3. Broker-0 writes to local log at offset N
  4. Follower (broker-1) polls: FetchLog(orders, 0, N) → gets message → writes locally → Ack(N)
  5. Leader advances HW to N once all ISR acked
  6. Producer receives OK

Consume from "orders":
  1. Consumer calls GetMetadata → learns broker-0 leads partition 0
  2. Consumer reads from broker-0 at offset <= HW only

Leader fails:
  1. Follower detects missed heartbeat → starts election (term++)
  2. Candidate = ISR member with highest ackedOffset
  3. Wins majority vote → becomes new leader
  4. Broadcasts updated TopicMetadata with new leader + new term
  5. Old leader recovers → sees stale term → steps down to follower
```
