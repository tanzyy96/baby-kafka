package proto_test

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"testing"

	"baby-kafka/core/proto"

	"github.com/stretchr/testify/require"
)

func TestProtoWriteFrame(t *testing.T) {
	// OOOH: we can use bytes.Buffer as an in-memory stream to test our proto package's ReadFrame and WriteFrame functions!
	b := &bytes.Buffer{}

	testData := []byte{1, 2, 3, 4, 5}

	resp := proto.Response{
		Status: proto.StatusBadRequest,
		Error:  "bad request",
		Data:   testData,
	}

	respBuffer := new(bytes.Buffer)
	err := gob.NewEncoder(respBuffer).Encode(resp)
	require.NoError(t, err)

	err = proto.WriteFrame(b, respBuffer.Bytes())
	require.NoError(t, err)

	// First 4 bytes: length
	// N bytes: Encoded Response
	var length uint32
	err = binary.Read(b, binary.BigEndian, &length)
	require.NoError(t, err)

	frame := make([]byte, length)
	_, err = io.ReadFull(b, frame)
	require.NoError(t, err)

	var decoded proto.Response
	r := bytes.NewReader(frame)
	err = gob.NewDecoder(r).Decode(&decoded)
	require.NoError(t, err)
	require.Equal(t, decoded.Status, resp.Status)
	require.Equal(t, decoded.Error, resp.Error)
	require.Equal(t, decoded.Data, resp.Data)
}

func TestProtoReadFrame(t *testing.T) {
	msgType := 1
	resp := proto.Response{
		Status: proto.StatusOK,
		Error:  "good request",
		Data:   []byte{1, 2, 3},
	}
	respBuffer := &bytes.Buffer{}
	err := gob.NewEncoder(respBuffer).Encode(&resp)
	require.NoError(t, err)

	length := uint32(len(respBuffer.Bytes()) + 1)

	b := &bytes.Buffer{}

	// Write length-prefix
	err = binary.Write(b, binary.BigEndian, length)
	require.NoError(t, err)

	// Write msg type
	err = b.WriteByte(byte(msgType))
	require.NoError(t, err)

	// Write encoded body
	_, err = b.Write(respBuffer.Bytes())
	require.NoError(t, err)

	mt, payload, err := proto.ReadFrame(b)
	require.NoError(t, err)
	require.Equal(t, mt, msgType)
	require.Equal(t, payload, respBuffer.Bytes())
}

func TestProtoWriteError(t *testing.T) {
	// We can also test the WriteError function by writing an error and then reading it back as a frame
	b := &bytes.Buffer{}

	testErr := "This is a test error"
	err := proto.WriteError(b, proto.StatusBadRequest, fmt.Errorf(testErr))
	require.NoError(t, err)

	// First 4 bytes: length
	// N bytes: Encoded Response
	var length uint32
	err = binary.Read(b, binary.BigEndian, &length)
	require.NoError(t, err)

	frame := make([]byte, length)
	_, err = io.ReadFull(b, frame)
	require.NoError(t, err)

	var decoded proto.Response
	r := bytes.NewReader(frame)
	err = gob.NewDecoder(r).Decode(&decoded)
	require.NoError(t, err)
	require.Equal(t, decoded.Status, proto.StatusBadRequest)
	require.Equal(t, decoded.Error, testErr)
}
