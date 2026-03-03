package core_test

import (
	"os"
	"testing"

	core "baby-kafka/core"
	"baby-kafka/internal/utils"

	"github.com/stretchr/testify/require"
)

func newTestTopic(t *testing.T) *core.Topic {
	// Create a temporary directory for the log files
	dir, err := os.MkdirTemp("", testLogDir)
	require.NoError(t, err)

	rolloverSize := int64(1024 * 1024) // 1MB for testing

	topic, err := core.NewTopic("test", 2, dir, rolloverSize)
	require.NoError(t, err)

	return topic
}

func TestTopicAppendAndReadWithKey(t *testing.T) {
	tests := []struct {
		name string
		msg  core.Message
	}{
		{"msg1", *core.NewMessage([]byte("key1"), []byte("value1"))},
		{"msg2", *core.NewMessage([]byte("key1"), []byte("value2"))},
		{"msg3", *core.NewMessage([]byte("key2"), []byte("value3"))},
	}

	for _, tt := range tests {
		t.Run("Append "+tt.name, func(t *testing.T) {
			topic := newTestTopic(t)

			// Append the message to the log
			_, _, err := topic.Append(tt.msg)
			require.NoError(t, err)
		})
	}

	for _, tt := range tests {
		t.Run("Read "+tt.name, func(t *testing.T) {
			targetPartition := utils.PartitionFor(string(tt.msg.Key), 2)
			topic := newTestTopic(t)

			// Append the message to the log
			partitionIndex, offset, err := topic.Append(tt.msg)
			require.NoError(t, err)
			require.Equal(t, int32(targetPartition), partitionIndex)

			// Read the message back
			readMsg, err := topic.ReadAt(int32(targetPartition), offset)
			require.NoError(t, err)
			require.Equal(t, tt.msg.Key, readMsg.Key)
			require.Equal(t, tt.msg.Value, readMsg.Value)
		})
	}
}

func TestTopicAppendAndReadWithoutKey(t *testing.T) {
	// This should be round-robin
	topic := newTestTopic(t)

	messages := []core.Message{
		*core.NewMessage(nil, []byte("value1")),
		*core.NewMessage(nil, []byte("value2")),
	}

	for _, msg := range messages {
		partitionIndex, offset, err := topic.Append(msg)
		require.NoError(t, err)

		readMsg, err := topic.ReadAt(partitionIndex, offset)
		require.NoError(t, err)
		require.Equal(t, []byte(nil), readMsg.Key) // this is the decoded empty key
		require.Equal(t, msg.Value, readMsg.Value)
	}
}
