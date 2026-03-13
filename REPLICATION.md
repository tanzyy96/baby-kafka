# Replication Flow

## Chapter 1: Distributed partition-leadership knowledge
The objective is to ensure different components have distributed knowledge of partition leadership.

### Chapter 1.1 Get To Know Each Other
All brokers need to know of each other during start-up phase. This is simplified to using config.json (vs Zookeeper).
When adminClient fires CreateTopic to a single broker, he needs to:
1. Create and assign in-memory partition distribution across available brokers
2. Inform other brokers of this distribution
3. Persist this distribution struct to disk

For simplicity sake, we will do a single topic broadcast from the first broker. In real-world, we have a ControllerBroker that gets forwarded that CreateTopic request first then it initiates the broadcast. This is to prevent race condition if multiple CreateTopics(sameTopicId) were to be received by different brokers ie.
1. Broker1 gets CreateTopic("events"), Broker2 gets CreateTopic("events")
2. Broker1.Broadcast(metadata.events), Broker2.Broadcast(metadata.events)
3. Broker1 overwrites its own metadata when receiving Broker2 and vice versa -> state becomes inconsistent

### Chapter 1.2 Create Topic And Broadcast
```
handleCreateTopic:
    1. initTopicPartitions(topic, numPartitions, replicationFactor, brokers)
         → computes TopicMetadata (round-robin assignment)
         → saves to __topic_metadata (MetadataManager)
         → returns TopicMetadata

    2. broker.CreateTopic(topic, numPartitions)
         → creates local partition dirs + logs (already works)

    3. for each peer broker:
         → push TopicMetadata to peer via inter-broker RPC
         → peer creates its local storage for the partitions it owns
```

#### Checklist
1. Brokers need to know each other and their addresses
    - [x] Config-based discovery: each broker config includes its own ID/address and a peers list
    - [x] This is stored in Broker as `map[int32]BrokerClient` to maintain the network connections
2. Brokers need to know the topics they are leading
    - [x] Assignment on CreateTopic: round-robin across known brokers, respects replicationFactor
    - [x] Validation: replicationFactor <= number of known brokers, else error
    - [x] MetadataManager: persists assignment to `__topic_metadata` (key=TopicMetadataKey, value=TopicMetadataValue)
    - [x] LoadMetadataManager: On broker startup, restore MetadataManager from `__topic_metadata` log 
3. All brokers share this knowledge
    - [x] BroadcastMetadata: On CreateTopic: receiving broker propagates metadata record to all peers via inter-broker RPC
    - [x] InsertMetadata: Each peer writes to their own `__topic_metadata` log


### Chapter 1.3 Client Discovery
Clients can discover partition leadership via GetMetadata.
- [x] GetMetadata request/response: returns leader + replicas (later with ISR) per partition
- [x] Multibroker Support for Producers
    - [x] Producer fetches topic metadata from bootstrap broker
    - [x] For a given key, producer knows which broker has the corresponding partition and sends the message there -> Send(topic, key, value)
    - [x] Perform lazy connection when it has to send to a new broker
- [x] Multibroker Support for Consumers
    - [x] Consumer fetches topic metadata from bootstrap broker
    - [x] Consumer is initialised with groupID and target partitions. We'll spin up the correct number of partitions from the `main.go` side for now. Later on we'll dynamically allocate partitions based on consumer group size via a coordinator.

## Chapter 2: Log replication (leader → followers)
The objective is to keep follower logs identical to the leader log. The main implementation revolves around goroutine to pull logs from leader to follower.

1. Each follower runs a fetch loop for each partition it follows -- pull mechanism
    - [ ] Background goroutine started on broker startup (after metadata restore)
    - [ ] Loop: send FetchLog(topic, partition, myNextOffset) to leader → receive batch → write to local log → repeat
    - [ ] New message type: MessageTypeFetchLog
    - [ ] New request type: FetchLogRequest{Topic, PartitionIndex, FromOffset, MaxBytes}
    - [ ] New response type: FetchLogResponse{Messages []Message, NextOffset int64}
2. Leader serves FetchLog requests
    - [ ] handleFetchLog: reads from partition log starting at FromOffset, returns up to MaxBytes
    - [ ] Reads local log directly — no special path needed, same log consumers read from
3. Follower applies fetched messages
    - [ ] Writes each message to its local partition log in order
    - [ ] Sends Ack(topic, partition, ackedOffset) back to leader after writing
    - [ ] New message type: MessageTypeReplicaAck
    - [ ] New request type: ReplicaAckRequest{Topic, PartitionIndex, AckedOffset, BrokerId}

## Chapter 3: ISR and High Watermark
The objective is to define what "committed" means and ensure consumers only see durable data.

1. Leader tracks acked offsets per follower
    - [ ] In-memory map: ackedOffset[brokerId] = highest offset that follower has confirmed
    - [ ] Updated on each ReplicaAck received
2. In-Sync Replica (ISR) set
    - [ ] A follower is "in-sync" if its ackedOffset >= leader's latestOffset - lagThreshold
    - [ ] Leader periodically checks and removes lagging followers from ISR
    - [ ] Removed followers continue fetching and re-join ISR once caught up
    - [ ] ISR changes are written to `__topic_metadata` log
3. High Watermark (HW)
    - [ ] HW = min(ackedOffset) across all ISR members
    - [ ] Advances as followers ack new offsets
    - [ ] Consumers can only read messages at offset <= HW (messages above may not be replicated yet)
4. Producer acknowledgement
    - [ ] Leader writes message to local log immediately
    - [ ] Leader waits until HW advances past the message's offset (all ISR have acked)
    - [ ] Only then responds OK to producer
    - [ ] If ISR shrinks to just the leader (all followers lag/die), leader can still commit alone

## Chapter 4: Leader election
The objective is to recover leadership when the current leader fails.

1. Heartbeat mechanism
    - [ ] Leader sends periodic heartbeat to all followers
    - [ ] Follower tracks time since last heartbeat
    - [ ] If no heartbeat for leaderTimeout, follower considers leader dead
2. Candidate selection
    - [ ] Among ISR members, the follower with the highest ackedOffset is best candidate
    - [ ] This guarantees no data loss — the new leader has all committed messages
3. Election
    - [ ] Candidate broadcasts RequestVote(topic, partition, candidateId, myAckedOffset) to all peers
    - [ ] Peers vote yes if candidate's ackedOffset >= their own
    - [ ] Candidate wins if it receives votes from majority of ISR
    - [ ] Winner updates `__topic_metadata` with new leader, broadcasts updated metadata
4. After election
    - [ ] New leader starts accepting writes for that partition
    - [ ] Old followers re-read metadata and point their fetch loops at new leader
    - [ ] Old leader (if it recovers) sees it is no longer leader and becomes a follower

## Chapter 5: Tests
- [ ] Two brokers, create topic with replicationFactor=2, produce messages, verify both brokers have identical logs
- [ ] Verify consumer can only read up to High Watermark
- [ ] Kill leader, verify follower detects failure, election completes, new leader accepts writes
- [ ] Revive old leader, verify it rejoins as follower with no data loss
