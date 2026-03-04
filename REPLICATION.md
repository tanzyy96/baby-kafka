# Replication Flow

## Chapter 1: Distributed partition-leadership knowledge
The objective is to ensure different components have distributed knowledge of partition leadership.

1. Brokers need to know each other and their addresses
    - [x] Config-based discovery: each broker config includes its own ID/address and a peers list
2. Brokers need to know the topics they are leading
    - [ ] Assignment on CreateTopic: round-robin across known brokers, respects replicationFactor
    - [ ] Validation: replicationFactor <= number of known brokers, else error
    - [ ] MetadataManager: persists assignment to `__topic_metadata` (key=TopicMetadataKey, value=TopicMetadataValue)
    - [ ] On broker startup: restore MetadataManager from `__topic_metadata` log
3. All brokers share this knowledge
    - [ ] On CreateTopic: receiving broker propagates metadata record to all peers via inter-broker RPC
    - [ ] Each peer writes to their own `__topic_metadata` log
4. Clients can discover partition leadership
    - [ ] GetMetadata request/response (already wired): returns leader + replicas + ISR per partition
    - [ ] Producers use this to know which broker to send writes to (must be leader)
    - [ ] Consumers use this to know which broker to read from

## Chapter 2: Log replication (leader → followers)
The objective is to keep follower logs identical to the leader log.

1. Each follower runs a fetch loop for each partition it follows
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
