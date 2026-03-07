package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/internal/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProducer(t *testing.T, addr string) client.Producer {
	t.Helper()
	cfg := &core.Config{Brokers: []core.BrokerConfig{{Index: 0, Addr: addr}}}
	p, err := client.NewProducer(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { p.Close() })
	return p
}

// startServer spins up a real server in a goroutine using the given data dir.
// Cancels the context on t.Cleanup to shut it down.
// TODO: support multiple brokers cuz consumers don't work with multiple brokers yet
func startServer(t *testing.T, dir string) string {
	t.Helper()
	cfg := core.DefaultConfig()
	cfg.BasePath = dir
	cfg.Brokers = []core.BrokerConfig{{Index: 0, Addr: ":0"}}

	s, err := core.NewServer(cfg, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go s.Start(ctx)
	return s.Addr()
}

func TestIntegration_CreateTopic_AppearsInListTopics(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 3)
	require.NoError(t, err)

	topics, err := admin.ListTopics()
	require.NoError(t, err)
	assert.Contains(t, topics.Topics, "events")
}

func TestIntegration_CreateTopic_Duplicate_Fails(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 3)
	require.NoError(t, err)

	_, err = admin.CreateTopic("events", 3)
	require.Error(t, err)
}

func TestIntegration_CreateTopic_ZeroPartitions_Fails(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 0)
	require.Error(t, err)
}

func TestIntegration_CreateTopic_MultiplePartitions_MessagesRouteCorrectly(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	const numPartitions = 3
	_, err = admin.CreateTopic("multi-topic", numPartitions)
	require.NoError(t, err)

	producer := newTestProducer(t, addr)

	// Send messages with known keys and track expected partition per key
	keys := []string{"alpha", "beta", "gamma", "delta"}
	offsetPerPartition := make(map[int32]int64)

	for _, key := range keys {
		_, err := producer.Send("multi-topic", []byte(key), []byte("value-"+key))
		require.NoError(t, err)
	}

	// Verify each key can be read from its expected partition
	for _, key := range keys {
		partition := int32(utils.PartitionFor(key, numPartitions))
		offset := offsetPerPartition[partition]
		offsetPerPartition[partition]++

		consumer, err := client.NewConsumer(addr, "group1", "multi-topic", partition, offset)
		require.NoError(t, err)

		k, v, _, err := consumer.Poll()
		consumer.Close()

		require.NoError(t, err)
		assert.Equal(t, []byte(key), k)
		assert.Equal(t, []byte("value-"+key), v)
	}
}

func TestIntegration_CreateTopic_PersistsAfterRestart(t *testing.T) {
	dir := t.TempDir()

	// Start first server, create topic, shut down
	cfg := core.DefaultConfig()
	cfg.BasePath = dir
	cfg.Brokers = []core.BrokerConfig{{Index: 0, Addr: ":0"}}

	s, err := core.NewServer(cfg, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Start(ctx)
	}()

	admin, err := client.NewAdmin(s.Addr())
	require.NoError(t, err)
	_, err = admin.CreateTopic("durable-events", 2)
	require.NoError(t, err)
	admin.Close()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down in time")
	}

	// Restart with same data dir, verify topic is still there
	addr := startServer(t, dir)

	admin2, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin2.Close()

	topics, err := admin2.ListTopics()
	require.NoError(t, err)
	assert.Contains(t, topics.Topics, "durable-events")
}

func TestIntegration_ProduceAndConsume(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("test-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, addr)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Offset)

	consumer, err := client.NewConsumer(addr, "group1", "test-topic", 0, 0)
	require.NoError(t, err)
	defer consumer.Close()

	key, value, atOffset, err := consumer.Poll()
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), key)
	assert.Equal(t, []byte("value1"), value)
	assert.Equal(t, int64(0), atOffset)
}

func TestIntegration_MultipleMessages_InOrder(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("ordered-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, addr)

	const n = 5
	for i := range n {
		_, err := producer.Send("ordered-topic", fmt.Appendf(nil, "key%d", i), fmt.Appendf(nil, "msg%d", i))
		require.NoError(t, err)
	}

	consumer, err := client.NewConsumer(addr, "group1", "ordered-topic", 0, 0)
	require.NoError(t, err)
	defer consumer.Close()

	for i := range n {
		key, value, atOffset, err := consumer.Poll()
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("key%d", i)), key)
		assert.Equal(t, []byte(fmt.Sprintf("msg%d", i)), value)
		assert.Equal(t, int64(i), atOffset)
	}
}

func TestIntegration_ConsumeFromMidOffset(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("mid-offset-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, addr)

	for i := 0; i < 5; i++ {
		_, err := producer.Send("mid-offset-topic", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	consumer, err := client.NewConsumer(addr, "group1", "mid-offset-topic", 0, 3)
	require.NoError(t, err)
	defer consumer.Close()

	key, value, atOffset, err := consumer.Poll()
	require.NoError(t, err)
	assert.Equal(t, []byte("k3"), key)
	assert.Equal(t, []byte("v3"), value)
	assert.Equal(t, int64(3), atOffset)

	key, value, atOffset, err = consumer.Poll()
	require.NoError(t, err)
	assert.Equal(t, []byte("k4"), key)
	assert.Equal(t, []byte("v4"), value)
	assert.Equal(t, int64(4), atOffset)

	_, _, _, err = consumer.Poll()
	assert.ErrorIs(t, err, core.ErrNoMessagesAtOffset)
}

func TestIntegration_BrokerRestart(t *testing.T) {
	dir := t.TempDir()

	// --- Server A ---
	cfgA := core.DefaultConfig()
	cfgA.BasePath = dir
	cfgA.Brokers = []core.BrokerConfig{{Index: 0, Addr: ":0"}}

	sA, err := core.NewServer(cfgA, 0)
	require.NoError(t, err)

	ctxA, cancelA := context.WithCancel(context.Background())
	addrA := sA.Addr()

	serverADone := make(chan struct{})
	go func() {
		defer close(serverADone)
		sA.Start(ctxA)
	}()

	admin, err := client.NewAdmin(addrA)
	require.NoError(t, err)
	_, err = admin.CreateTopic("durable-topic", 1)
	require.NoError(t, err)
	admin.Close()

	producer := newTestProducer(t, addrA)
	for i := 0; i < 3; i++ {
		_, err := producer.Send("durable-topic", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	cancelA()
	select {
	case <-serverADone:
	case <-time.After(5 * time.Second):
		t.Fatal("server A did not shut down in time")
	}

	// --- Server B (same data dir) ---
	addrB := startServer(t, dir)

	consumer, err := client.NewConsumer(addrB, "group1", "durable-topic", 0, 0)
	require.NoError(t, err)
	defer consumer.Close()

	for i := 0; i < 3; i++ {
		key, value, _, err := consumer.Poll()
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("k%d", i)), key)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), value)
	}
}
