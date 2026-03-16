package core_test

import (
	"os"
	"testing"

	core "baby-kafka/core"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

func newTestTopic(t *testing.T) *core.Topic {
	// Create a temporary directory for the log files
	dir, err := os.MkdirTemp("", testLogDir)
	require.NoError(t, err)

	rolloverSize := int64(1024 * 1024) // 1MB for testing

	topic, err := core.NewTopic("test", []int32{0, 1}, dir, rolloverSize, testutils.TestLogger())
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
			_, err := topic.Append(0, tt.msg)
			require.NoError(t, err)
		})
	}

	for _, tt := range tests {
		t.Run("Read "+tt.name, func(t *testing.T) {
			topic := newTestTopic(t)

			// Append the message to the log
			offset, err := topic.Append(0, tt.msg)
			require.NoError(t, err)

			// Read the message back
			readMsg, err := topic.ReadAt(0, offset)
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
		offset, err := topic.Append(0, msg)
		require.NoError(t, err)

		readMsg, err := topic.ReadAt(0, offset)
		require.NoError(t, err)
		require.Equal(t, []byte(nil), readMsg.Key) // this is the decoded empty key
		require.Equal(t, msg.Value, readMsg.Value)
	}
}
