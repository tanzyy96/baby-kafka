package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/internal/utils"

	"github.com/charmbracelet/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConfig(t *testing.T) *core.Config {
	t.Helper()
	cfg := core.DefaultConfig()
	cfg.BasePath = t.TempDir()
	for i := range cfg.Brokers {
		cfg.Brokers[i].Addr = ":0"
	}

	return cfg
}

func newTestProducer(t *testing.T, cfg *core.Config) client.Producer {
	t.Helper()
	p, err := client.NewProducer(cfg, log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	t.Cleanup(func() { p.Close() })
	return p
}

func newTestConsumer(t *testing.T, cfg *core.Config, groupID, topic string, partitions []int32) client.Consumer {
	t.Helper()
	c, err := client.NewConsumer("test-consumer", cfg, groupID, topic, partitions, log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// startServer spins up a real server in a goroutine using the given data dir.
// Cancels the context on t.Cleanup to shut it down.
func startServer(t *testing.T, cfg *core.Config, id int) string {
	t.Helper()

	s, err := core.NewServer(cfg, int32(id))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go s.Start(ctx)
	return s.Addr()
}

func startServers(t *testing.T, cfg *core.Config) []string {
	t.Helper()
	var addrs []string
	for i := range cfg.Brokers {
		addr := startServer(t, cfg, i)
		cfg.Brokers[i].Addr = addr
		addrs = append(addrs, addr)
	}
	return addrs
}

func TestIntegration_CreateTopic_AppearsInListTopics(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 3)
	require.NoError(t, err)

	topics, err := admin.ListTopics()
	require.NoError(t, err)
	assert.Contains(t, topics.Topics, "events")
}

func TestIntegration_CreateTopic_Duplicate_Fails(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 3)
	require.NoError(t, err)

	_, err = admin.CreateTopic("events", 3)
	require.Error(t, err)
}

func TestIntegration_CreateTopic_ZeroPartitions_Fails(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("events", 0)
	require.Error(t, err)
}

func TestIntegration_CreateTopic_MultiplePartitions_MessagesRouteCorrectly(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	const numPartitions = 3
	_, err = admin.CreateTopic("multi-topic", numPartitions)
	require.NoError(t, err)

	log.Infof("WHAT ARE THESE PORT ADDRESSES: %+v", cfg.Brokers)

	producer := newTestProducer(t, cfg)

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, key := range keys {
		_, err := producer.Send("multi-topic", []byte(key), []byte("value-"+key))
		require.NoError(t, err)
	}

	// Single consumer tracking offsets across all partitions
	consumer := newTestConsumer(t, cfg, "group1", "multi-topic", []int32{0, 1, 2})
	for _, key := range keys {
		partition := int32(utils.PartitionFor(key, numPartitions))
		k, v, _, err := consumer.Poll(partition)
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
	// cfg.Brokers = []core.BrokerConfig{{Index: 0, Addr: ":0"}}

	s, err := core.NewServer(cfg, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Start(ctx)
	}()

	admin, err := client.NewAdmin(s.Addr(), log.NewWithOptions(os.Stderr, log.Options{}))
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
	addr := startServer(t, cfg, 0)

	admin2, err := client.NewAdmin(addr, log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin2.Close()

	topics, err := admin2.ListTopics()
	require.NoError(t, err)
	assert.Contains(t, topics.Topics, "durable-events")
}

func TestIntegration_ProduceAndConsume(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("test-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, cfg)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.Offset)

	consumer := newTestConsumer(t, cfg, "group1", "test-topic", []int32{0})

	key, value, _, err := consumer.Poll(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("key1"), key)
	assert.Equal(t, []byte("value1"), value)
}

func TestIntegration_MultipleMessages_InOrder(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("ordered-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, cfg)

	const n = 5
	for i := range n {
		_, err := producer.Send("ordered-topic", fmt.Appendf(nil, "key%d", i), fmt.Appendf(nil, "msg%d", i))
		require.NoError(t, err)
	}

	consumer := newTestConsumer(t, cfg, "group1", "ordered-topic", []int32{0})

	for i := range n {
		key, value, _, err := consumer.Poll(0)
		require.NoError(t, err)
		assert.Equal(t, fmt.Appendf(nil, "key%d", i), key)
		assert.Equal(t, fmt.Appendf(nil, "msg%d", i), value)
	}
}

func TestIntegration_ConsumeFromMidOffset(t *testing.T) {
	cfg := newTestConfig(t)
	addrs := startServers(t, cfg)

	admin, err := client.NewAdmin(addrs[0], log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("mid-offset-topic", 1)
	require.NoError(t, err)

	producer := newTestProducer(t, cfg)

	for i := 0; i < 5; i++ {
		_, err := producer.Send("mid-offset-topic", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	consumer := newTestConsumer(t, cfg, "group1", "mid-offset-topic", []int32{0})

	// Discard messages 0, 1, 2
	for i := 0; i < 3; i++ {
		_, _, _, err := consumer.Poll(0)
		require.NoError(t, err)
	}

	key, value, _, err := consumer.Poll(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("k3"), key)
	assert.Equal(t, []byte("v3"), value)

	key, value, _, err = consumer.Poll(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("k4"), key)
	assert.Equal(t, []byte("v4"), value)

	_, _, _, err = consumer.Poll(0)
	assert.ErrorIs(t, err, core.ErrOffsetNotFound)
}

func TestIntegration_BrokerRestart(t *testing.T) {
	cfg := newTestConfig(t)

	sA, err := core.NewServer(cfg, 0)
	require.NoError(t, err)
	sB, err := core.NewServer(cfg, 1)
	require.NoError(t, err)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	addrA := sA.Addr()

	serverADone := make(chan struct{})
	go func() {
		defer close(serverADone)
		sA.Start(ctxA)
	}()
	go sB.Start(ctxB)

	admin, err := client.NewAdmin(addrA, log.NewWithOptions(os.Stderr, log.Options{}))
	require.NoError(t, err)
	_, err = admin.CreateTopic("durable-topic", 1)
	require.NoError(t, err)
	admin.Close()

	producer := newTestProducer(t, cfg)
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

	// --- Restart Server A on a new port ---
	cfg.Brokers[0].Addr = ":0"
	startServer(t, cfg, 0)

	consumer := newTestConsumer(t, cfg, "group1", "durable-topic", []int32{0})
	for i := 0; i < 3; i++ {
		key, value, _, err := consumer.Poll(0)
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("k%d", i)), key)
		assert.Equal(t, []byte(fmt.Sprintf("v%d", i)), value)
	}
}
