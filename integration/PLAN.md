# Integration Tests Plan

## Goal

Spin up a real TCP server with real disk I/O and hit it with the `client.Producer` / `client.Consumer` / `client.Admin` packages. No mocks, no fakes — full vertical slice from wire to disk.

---

## Pre-requisites: Bug Fixes & Missing Pieces

Before the tests can be written, several issues in the existing codebase need fixing:

### 1. `core/partition.go` — `LoadPartitions` is broken

Two bugs:

**a) Inverted skip condition** — currently skips partition dirs, processes everything else:
```go
// Current (wrong): skips dirs named "partition-*"
if !f.IsDir() || strings.Contains(f.Name(), "partition-") {
    continue
}

// Fixed: skip non-dirs and dirs NOT named "partition-*"
if !f.IsDir() || !strings.Contains(f.Name(), "partition-") {
    continue
}
```

**b) Nil map** — `partitions` is never initialised before writing into it (causes a panic):
```go
// Add this before the loop:
partitions = make(map[int32]*Partition)
```

**c) Wrong path argument to `LoadPartition`** — `LoadPartitions` passes the full partition path but `LoadPartition` re-appends `partition-N` onto it, doubling the suffix. Fix: pass `basePath` (the topic folder) rather than the pre-built partition subfolder:
```go
// Current (wrong): passes data/topic/partition-0 to LoadPartition
folderPath := fmt.Sprintf("%s/%s", basePath, f.Name())
p, err := LoadPartition(int32(pidx), folderPath, maxSize)

// Fixed: pass topic path; LoadPartition builds the rest
p, err := LoadPartition(int32(pidx), basePath, maxSize)
```

### 2. `core/offset_manager.go` — `LoadOffsetManager` is missing

`LoadBroker` calls `LoadOffsetManager()` which doesn't exist. Since offset persistence isn't implemented yet, it should just alias `NewOffsetManager`:
```go
func LoadOffsetManager() *OffsetManager {
    return NewOffsetManager()
}
```

### 3. `core/broker.go` — `NewBroker` ignores existing data on disk

When a server restarts pointing at an existing data directory, `NewBroker` silently creates an empty topic map (there's a `TODO: LoadBroker()` comment). Fix it to delegate to `LoadBroker` when the directory already exists:
```go
func NewBroker(basePath string, rolloverLimit int64) (*Broker, error) {
    if err := os.Mkdir(basePath, 0o755); err != nil {
        if os.IsExist(err) {
            log.Infof("Base path exists, loading: %s", basePath)
            return LoadBroker(basePath, rolloverLimit)
        }
        return nil, fmt.Errorf("failed to create base path: %w", err)
    }
    // fresh broker ...
}
```

### 4. `core/server.go` — add `Addr()` method

Tests need to discover the port after the listener binds to `:0`:
```go
func (s *Server) Addr() string {
    return (*s.listener).Addr().String()
}
```

### 5. `core/client/admin.go` — add `Close()` method

`Producer` and `Consumer` both have `Close()`; `Admin` is missing it:
```go
func (a *Admin) Close() error {
    return a.conn.Close()
}
```

---

## New File: `integration/integration_test.go`

Package: `package integration_test`

### Test helper

```go
// startServer spins up a real server in a goroutine using the given data dir.
// Cancels the context on t.Cleanup to shut it down.
func startServer(t *testing.T, dir string) string {
    cfg := core.DefaultConfig()
    cfg.BasePath = dir
    cfg.ServerPort = ":0"

    s, err := core.NewServer(cfg)
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(cancel)

    go s.Start(ctx)
    return s.Addr()
}
```

### Tests

| Test | What it verifies |
|------|-----------------|
| `TestIntegration_ProduceAndConsume` | Round-trip: create topic → produce → consume; key + value match |
| `TestIntegration_MultipleMessages_InOrder` | N messages produced in order arrive in the same order |
| `TestIntegration_KeyedPartitionRouting` | Same key always routes to the same partition (via `ProduceResponse.PartitionIndex`) |
| `TestIntegration_ConsumeFromMidOffset` | Consumer starting at offset 3 sees only msgs 3 & 4; next Poll returns `ErrNoMessagesAtOffset` |
| `TestIntegration_BrokerRestart` | Messages produced to server A survive a context cancel + restart to server B with the same data dir |

---

## Files to Change

| File | Change |
|------|--------|
| `core/partition.go` | Fix `LoadPartitions`: condition, nil map, wrong path arg |
| `core/offset_manager.go` | Add `LoadOffsetManager()` |
| `core/broker.go` | Fix `NewBroker` to call `LoadBroker` when dir exists |
| `core/server.go` | Add `Addr() string` |
| `core/client/admin.go` | Add `Close() error` |
| `integration/integration_test.go` | New file — all integration tests |

---

## Verification

```bash
go test ./integration/... -v -run TestIntegration
go test ./...   # ensure nothing else broke
```
