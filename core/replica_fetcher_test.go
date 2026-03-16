package core_test

import (
	"context"
	"net"
	"testing"

	"baby-kafka/core"
	"baby-kafka/core/proto"
	testutils "baby-kafka/core/test_utils"

	"github.com/charmbracelet/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeReplicaFetcher(t *testing.T, dialFn func(string) (net.Conn, error)) (core.ReplicaFetcher, core.Partition) {
	t.Helper()
	cfg := testutils.SharedTestConfig(t, 2)

	partition, err := core.NewPartition(0, t.TempDir(), cfg.RolloverLimit, testutils.TestLogger())
	require.NoError(t, err)

	client, err := core.NewBrokerClient(":0", testutils.TestLogger(), core.WithDialFn(dialFn))
	require.NoError(t, err)

	rf := core.NewReplicaFetcher(cfg, partition, client, "test-topic", 1, 0, testutils.TestLogger())

	return rf, partition
}

// TestReplicaFetcher_ContextCancel verifies that Start returns nil when the context
// is cancelled before any fetch loop iteration runs.
func TestReplicaFetcher_ContextCancel(t *testing.T) {
	dialFn := func(_ string) (net.Conn, error) {
		c, _ := net.Pipe()
		return c, nil
	}

	rf, _ := makeReplicaFetcher(t, dialFn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rf.Start(ctx)
	require.NoError(t, err)
}

// TestReplicaFetcher_ReplicatesMessages verifies that messages returned by the leader
// are written to the local partition in order.
func TestReplicaFetcher_ReplicatesMessages(t *testing.T) {
	want := []*core.MessageWithOffset{
		{Message: core.NewMessage([]byte("k1"), []byte("v1")), Offset: 0},
		{Message: core.NewMessage([]byte("k2"), []byte("v2")), Offset: 1},
		{Message: core.NewMessage([]byte("k3"), []byte("v3")), Offset: 2},
	}

	fetchHandler := func(_ int, payload []byte) proto.Response {
		var req core.FetchLogRequest
		assert.NoError(t, proto.GobDecode(payload, &req))

		data, err := proto.GobEncode(core.FetchLogResponse{Messages: want})
		assert.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	// NewTestConn handles exactly one request then closes the server side,
	// causing Start to return a connection error after writing the messages.
	clientConn, _ := testutils.NewTestConn(t, fetchHandler)
	dialFn := func(_ string) (net.Conn, error) { return clientConn, nil }

	rf, partition := makeReplicaFetcher(t, dialFn)

	// Start returns a non-nil error once the server closes — that's expected here.
	// We only care that the messages landed in the partition before the error.
	rf.Start(context.Background())

	for i, w := range want {
		msg, err := partition.ReadAt(int64(i))
		require.NoError(t, err)
		log.Info("Keys", "want", w.Message.Key, "got", msg.Key)
		assert.Equal(t, w.Message.Key, msg.Key)
		assert.Equal(t, w.Message.Value, msg.Value)
	}
}
