package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"baby-kafka/core"
	"baby-kafka/core/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startServer spins up a real server in a goroutine using the given data dir.
// Cancels the context on t.Cleanup to shut it down.
func startServer(t *testing.T, dir string) string {
	t.Helper()
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

func TestIntegration_ProduceAndConsume(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("test-topic", 1)
	require.NoError(t, err)

	producer, err := client.NewProducer(addr)
	require.NoError(t, err)
	defer producer.Close()

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.PartitionIndex)
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

	producer, err := client.NewProducer(addr)
	require.NoError(t, err)
	defer producer.Close()

	const n = 5
	for i := 0; i < n; i++ {
		_, err := producer.Send("ordered-topic", []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("msg%d", i)))
		require.NoError(t, err)
	}

	consumer, err := client.NewConsumer(addr, "group1", "ordered-topic", 0, 0)
	require.NoError(t, err)
	defer consumer.Close()

	for i := 0; i < n; i++ {
		key, value, atOffset, err := consumer.Poll()
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("key%d", i)), key)
		assert.Equal(t, []byte(fmt.Sprintf("msg%d", i)), value)
		assert.Equal(t, int64(i), atOffset)
	}
}

func TestIntegration_KeyedPartitionRouting(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("keyed-topic", 3)
	require.NoError(t, err)

	producer, err := client.NewProducer(addr)
	require.NoError(t, err)
	defer producer.Close()

	const n = 10
	var firstPartition int32 = -1
	for i := 0; i < n; i++ {
		resp, err := producer.Send("keyed-topic", []byte("stable-key"), []byte("value"))
		require.NoError(t, err)
		if firstPartition == -1 {
			firstPartition = resp.PartitionIndex
		} else {
			assert.Equal(t, firstPartition, resp.PartitionIndex, "same key should always route to the same partition")
		}
	}
}

func TestIntegration_ConsumeFromMidOffset(t *testing.T) {
	addr := startServer(t, t.TempDir())

	admin, err := client.NewAdmin(addr)
	require.NoError(t, err)
	defer admin.Close()

	_, err = admin.CreateTopic("mid-offset-topic", 1)
	require.NoError(t, err)

	producer, err := client.NewProducer(addr)
	require.NoError(t, err)
	defer producer.Close()

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
	cfgA.ServerPort = ":0"

	sA, err := core.NewServer(cfgA)
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

	producer, err := client.NewProducer(addrA)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := producer.Send("durable-topic", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}
	producer.Close()

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
