package core_test

import (
	"os"
	"testing"

	"baby-kafka/core"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

const testPartitionDir = "babykafka_partition_test"

func newTestPartition(t *testing.T, maxSize int64) core.Partition {
	// Create a temporary directory for the partition
	dir, err := os.MkdirTemp("", testPartitionDir)
	require.NoError(t, err)

	// Create a new partition instance
	partition, err := core.NewPartition(0, dir, maxSize, testutils.TestLogger()) // Set maxSize to 1 byte to trigger rollover quickly, else 0 to use default
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
	require.FileExists(t, partition.BasePath()+"/00000000000000000000.log")
}

func TestPartitionAppendAndReadAt(t *testing.T) {
	partition := newTestPartition(t, 0)

	messages := []core.Message{
		*core.NewMessage([]byte("key1"), []byte("value1")),
		*core.NewMessage([]byte("key2"), []byte("value2")),
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
		msg := core.Message{Key: []byte("key"), Value: []byte("value")}
		_, err := partition.Append(msg)
		require.NoError(t, err)
	}

	// We should have rolled over to a new log file
	require.FileExists(t, partition.BasePath()+"/00000000000000000000.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000001.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000002.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000003.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000004.log")
}

func TestPartitionReadAtWithRollover(t *testing.T) {
	// Set a small max size to trigger rollover on every message
	partition := newTestPartition(t, 1)

	messages := []core.Message{
		*core.NewMessage([]byte("key1"), []byte("value1")),
		*core.NewMessage([]byte("key2"), []byte("value2")),
	}

	for _, msg := range messages {
		_, err := partition.Append(msg)
		require.NoError(t, err)
	}

	require.FileExists(t, partition.BasePath()+"/00000000000000000000.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000000.index")
	require.FileExists(t, partition.BasePath()+"/00000000000000000001.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000001.index")

	for i, msg := range messages {
		readMsg, err := partition.ReadAt(int64(i))
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)
	}
}

func TestLoadPartition(t *testing.T) {
	// Set a small max size to trigger rollover on every message
	partition := newTestPartition(t, 1)

	messages := []core.Message{
		*core.NewMessage([]byte("key1"), []byte("value1")),
		*core.NewMessage([]byte("key2"), []byte("value2")),
	}

	for _, msg := range messages {
		_, err := partition.Append(msg)
		require.NoError(t, err)
	}

	require.FileExists(t, partition.BasePath()+"/00000000000000000000.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000000.index")
	require.FileExists(t, partition.BasePath()+"/00000000000000000001.log")
	require.FileExists(t, partition.BasePath()+"/00000000000000000001.index")

	for i, msg := range messages {
		readMsg, err := partition.ReadAt(int64(i))
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)
	}

	// Now we load the partition again and verify we can read the messages
	folderPath := partition.BasePath()[:len(partition.BasePath())-len("/partition-0")]
	loadedPartition, err := core.LoadPartition(0, folderPath, 1, testutils.TestLogger())
	require.NoError(t, err)

	// Writing and reading new messages as well
	newMessages := []core.Message{
		*core.NewMessage([]byte("key3"), []byte("value3")),
	}

	messages = append(messages, newMessages...)

	_, err = loadedPartition.Append(newMessages[0])
	require.NoError(t, err)

	for i, msg := range messages {
		readMsg, err := loadedPartition.ReadAt(int64(i))
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)
	}
}
