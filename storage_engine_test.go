package babykafka_test

import (
	"os"
	"path/filepath"
	"testing"

	babykafka "baby-kafka"

	"github.com/stretchr/testify/require"
)

const TEST_LOG_DIR = "babykafka_test"

func newTestLog(t *testing.T) *babykafka.Log {
	// Create a temporary directory for the log files
	dir, err := os.MkdirTemp("", TEST_LOG_DIR)
	require.NoError(t, err)

	// Create a new log instance
	logPath := filepath.Join(dir, "log-0")
	log, err := babykafka.NewLog(logPath)
	require.NoError(t, err)

	t.Cleanup(func() {
		// Clean up the temporary directory after the test
		os.RemoveAll(dir)
	})
	return log
}

func TestAppend_VariousPayloads(t *testing.T) {
	tests := []struct {
		name string
		msg  babykafka.Message
	}{
		{"simple message", babykafka.Message{Key: []byte("key1"), Value: []byte("value1")}},
		{"empty key", babykafka.Message{Key: []byte{}, Value: []byte("value1")}},
		{"empty value", babykafka.Message{Key: []byte("key1"), Value: []byte{}}},
		{"both empty", babykafka.Message{Key: []byte{}, Value: []byte{}}},
		{"large value", babykafka.Message{Key: []byte("key1"), Value: make([]byte, 1024*1024)}}, // 1MB value
		{"binary data", babykafka.Message{Key: []byte{0x00, 0xFF, 0xAA}, Value: []byte{0x01, 0x02, 0x03}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := newTestLog(t)

			// Append the message to the log
			_, _, err := log.Append(tt.msg)
			require.NoError(t, err)
		})
	}
}

func TestReadAt_ReturnsOriginalMessage(t *testing.T) {
	log := newTestLog(t)

	// Append a message to the log
	msg := babykafka.Message{Key: []byte("key1"), Value: []byte("value1")}
	_, _, err := log.Append(msg)
	require.NoError(t, err)

	// Read the message back from the log
	readMsg, err := log.ReadAt(0)
	require.NoError(t, err)
	require.Equal(t, msg.Key, readMsg.Key)
	require.Equal(t, msg.Value, readMsg.Value)
}

func TestReadAt_MultipleMessages(t *testing.T) {
	// append multiple messages and read them back to ensure offsets are correct
	log := newTestLog(t)
	messages := []babykafka.Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	for _, msg := range messages {
		_, _, err := log.Append(msg)
		require.NoError(t, err)
	}

	// Read each message back using the correct bytePos
	var bytePos int64
	for _, msg := range messages {
		readMsg, err := log.ReadAt(bytePos)
		require.NoError(t, err)
		require.Equal(t, msg.Key, readMsg.Key)
		require.Equal(t, msg.Value, readMsg.Value)

		// Calculate the next offset (current offset + length prefix + message length)
		bytePos += readMsg.SerializedLength()
	}
}

func TestReadAt_InvalidOffset(t *testing.T) {
	log := newTestLog(t)

	// Append a message to the log
	msg := babykafka.Message{Key: []byte("key1"), Value: []byte("value1")}
	_, _, err := log.Append(msg)
	require.NoError(t, err)

	// Attempt to read from an invalid offset
	_, err = log.ReadAt(9999) // Offset beyond the end of the log
	require.Error(t, err)
}
