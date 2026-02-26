package proto_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"baby-kafka/core/proto"

	"github.com/stretchr/testify/require"
)

func TestProtoWriteAndReadFrame(t *testing.T) {
	// OOOH: we can use bytes.Buffer as an in-memory stream to test our proto package's ReadFrame and WriteFrame functions!
	b := &bytes.Buffer{}

	testData := []byte{1, 2, 3, 4, 5}

	err := proto.WriteFrame(b, testData)
	require.NoError(t, err)

	msgType, payload, err := proto.ReadFrame(b)
	require.NoError(t, err)
	require.Equal(t, 1, msgType)            // The first byte of testData is 1
	require.Equal(t, testData[1:], payload) // The payload should be the rest of testData
}

func TestProtoWriteError(t *testing.T) {
	// We can also test the WriteError function by writing an error and then reading it back as a frame
	b := &bytes.Buffer{}

	testErr := "This is a test error"
	err := proto.WriteError(b, fmt.Errorf(testErr))
	require.NoError(t, err)

	// We cannot use ReadFrame here because the error message is not in the expected frame format, but we can read the raw bytes and check the content
	frame := make([]byte, b.Len())
	_, err = io.ReadFull(b, frame)
	require.NoError(t, err)

	// We expect first 4 bytes to be length prefix, and the rest to be the error message
	expectedLength := uint32(len("ERROR:" + testErr))
	expectedErrorResp := "ERROR:" + testErr
	require.Equal(t, expectedLength, binary.BigEndian.Uint32(frame[:4]))
	require.Equal(t, expectedErrorResp, string(frame[4:]))
}
