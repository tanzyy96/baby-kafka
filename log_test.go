package babykafka_test

import (
	"os"
	"path/filepath"
	"testing"

	babykafka "baby-kafka"

	"github.com/stretchr/testify/require"
)

const testIndexLogDir = "babykafka_log_index_test"

func newTestIndex(t *testing.T) *babykafka.LogIndex {
	// Create a temporary directory for the log files
	dir, err := os.MkdirTemp("", testIndexLogDir)
	require.NoError(t, err)

	// Create a new log instance
	logPath := filepath.Join(dir, "log-0")
	index, err := babykafka.NewLogIndex(logPath)
	require.NoError(t, err)

	t.Cleanup(func() {
		// Clean up the temporary directory after the test
		os.RemoveAll(dir)
	})
	return index
}

func TestAppendAndRead(t *testing.T) {
	index := newTestIndex(t)

	// Append some offsets and byte positions
	require.NoError(t, index.Append(0, 0))
	require.NoError(t, index.Append(1, 37))
	require.NoError(t, index.Append(2, 82))

	// Read the byte positions back
	bytePos, err := index.Read(0)
	require.NoError(t, err)
	require.Equal(t, int32(0), bytePos)

	bytePos, err = index.Read(1)
	require.NoError(t, err)
	require.Equal(t, int32(37), bytePos)

	bytePos, err = index.Read(2)
	require.NoError(t, err)
	require.Equal(t, int32(82), bytePos)
}
