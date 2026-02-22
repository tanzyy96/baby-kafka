package babykafka_test

import (
	"os"
	"testing"

	babykafka "baby-kafka"

	"github.com/stretchr/testify/require"
)

const testPartitionDir = "babykafka_partition_test"

func newTestPartition(t *testing.T, maxSize int64) *babykafka.Partition {
	// Create a temporary directory for the partition
	dir, err := os.MkdirTemp("", testPartitionDir)
	require.NoError(t, err)

	// Create a new partition instance
	partition, err := babykafka.NewPartition(0, dir, maxSize) // Set maxSize to 1 byte to trigger rollover quickly, else 0 to use default
	require.NoError(t, err)

	t.Cleanup(func() {
		// Clean up the temporary directory after the test
		os.RemoveAll(dir)
	})
	return partition
}

func TestNewPartition(t *testing.T) {
	// New partition should create a new log and set it as the active log
	partition := newTestPartition(t, 0)

	// The log should be created in the partition directory
	require.FileExists(t, partition.Path+"/00000000000000000000.log")
}

func TestPartitionAppendAndReadAt(t *testing.T) {
	partition := newTestPartition(t, 0)

	messages := []babykafka.Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	for i, msg := range messages {
		offset, err := partition.Append(msg)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
	}

	for i, msg := range messages {
		readMsg, err := partition.ReadAt(int64(i))
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)
	}
}

func TestPartitionAppend_Rollover(t *testing.T) {
	// Set a small max size to trigger rollover on every message
	partition := newTestPartition(t, 1)

	// Append messages until we trigger a rollover
	for i := 0; i < 5; i++ {
		msg := babykafka.Message{Key: []byte("key"), Value: []byte("value")}
		_, err := partition.Append(msg)
		require.NoError(t, err)
	}

	// We should have rolled over to a new log file
	require.FileExists(t, partition.Path+"/00000000000000000000.log")
	require.FileExists(t, partition.Path+"/00000000000000000001.log")
	require.FileExists(t, partition.Path+"/00000000000000000002.log")
	require.FileExists(t, partition.Path+"/00000000000000000003.log")
	require.FileExists(t, partition.Path+"/00000000000000000004.log")
}

func TestPartitionReadAtWithRollover(t *testing.T) {
	// Set a small max size to trigger rollover on every message
	partition := newTestPartition(t, 1)

	messages := []babykafka.Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	for _, msg := range messages {
		_, err := partition.Append(msg)
		require.NoError(t, err)
	}

	require.FileExists(t, partition.Path+"/00000000000000000000.log")
	require.FileExists(t, partition.Path+"/00000000000000000000.index")
	require.FileExists(t, partition.Path+"/00000000000000000001.log")
	require.FileExists(t, partition.Path+"/00000000000000000001.index")

	for i, msg := range messages {
		readMsg, err := partition.ReadAt(int64(i))
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)
	}
}
