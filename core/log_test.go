package core_test

import (
	"os"
	"testing"

	"baby-kafka/core"

	"github.com/stretchr/testify/require"
)

const testLogDir = "babykafka_test"

func newTestLog(t *testing.T) *core.Log {
	// Create a temporary directory for the log files
	dir, err := os.MkdirTemp("", testLogDir)
	require.NoError(t, err)

	// Create a new log instance
	log, err := core.NewLog(0, dir)
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
		msg  core.Message
	}{
		{"simple message", core.Message{Key: []byte("key1"), Value: []byte("value1")}},
		{"empty key", core.Message{Key: []byte{}, Value: []byte("value1")}},
		{"empty value", core.Message{Key: []byte("key1"), Value: []byte{}}},
		{"both empty", core.Message{Key: []byte{}, Value: []byte{}}},
		{"large value", core.Message{Key: []byte("key1"), Value: make([]byte, 1024*1024)}}, // 1MB value
		{"binary data", core.Message{Key: []byte{0x00, 0xFF, 0xAA}, Value: []byte{0x01, 0x02, 0x03}}},
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
	msg := *core.NewMessage([]byte("key1"), []byte("value1"))
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
	messages := []core.Message{
		*core.NewMessage([]byte("key1"), []byte("value1")),
		*core.NewMessage([]byte("key2"), []byte("value2")),
		*core.NewMessage([]byte("key3"), []byte("value3")),
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
	msg := core.Message{Key: []byte("key1"), Value: []byte("value1")}
	_, _, err := log.Append(msg)
	require.NoError(t, err)

	// Attempt to read from an invalid offset
	_, err = log.ReadAt(9999) // Offset beyond the end of the log
	require.Error(t, err)
}

func TestReadAt_NewMessage_PassesChecksumVerification(t *testing.T) {
	// Messages created via NewMessage should round-trip through the log
	// without triggering checksum errors
	lg := newTestLog(t)

	msg := core.NewMessage([]byte("key"), []byte("value"))
	_, _, err := lg.Append(*msg)
	require.NoError(t, err)

	readMsg, err := lg.ReadAt(0)
	require.NoError(t, err)
	require.Equal(t, msg.Key, readMsg.Key)
	require.Equal(t, msg.Value, readMsg.Value)
}

func TestReadAt_CorruptChecksum_ReturnsError(t *testing.T) {
	// If a message is written with an intentionally wrong checksum,
	// reading it back should return a checksum error
	lg := newTestLog(t)

	msg := core.Message{
		Key:      []byte("key"),
		Value:    []byte("value"),
		Checksum: 99999, // intentionally wrong
	}
	_, _, err := lg.Append(msg)
	require.NoError(t, err)

	_, err = lg.ReadAt(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum")
}
