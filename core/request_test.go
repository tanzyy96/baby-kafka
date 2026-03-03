package core

import (
	"bytes"
	"encoding/gob"
	"testing"

	"baby-kafka/core/proto"

	"github.com/stretchr/testify/require"
)

func TestProduceRequest(t *testing.T) {
	r := ProduceRequest{Key: []byte("k"), Value: []byte("v"), Topic: "t"}
	if r.Topic != "t" {
		t.Error("topic mismatch")
	}
}

// helpers

func encodePayload(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(v))
	return buf.Bytes()
}

func decodeProtoResponse(t *testing.T, data []byte) proto.Response {
	t.Helper()
	var resp proto.Response
	require.NoError(t, gob.NewDecoder(bytes.NewBuffer(data)).Decode(&resp))
	return resp
}

func TestHandleFetchOffset_GoodPayload(t *testing.T) {
	s := newTestServer(t)
	b := encodePayload(t, FetchOffsetRequest{
		GroupId:        "group-1",
		Topic:          "my-topic",
		PartitionIndex: 0,
	})
	resp, err := s.handleFetchOffset(b)
	require.NoError(t, err)
	decoded := decodeProtoResponse(t, resp)
	require.Equal(t, proto.StatusOK, decoded.Status)

	var req FetchOffsetResponse
	err = decoded.DecodeData(&req)
	require.NoError(t, err)
	require.Equal(t, int64(0), req.Offset)
}

// handleFetchOffset

func TestHandleFetchOffset_BadPayload(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.handleFetchOffset([]byte("not-gob"))
	require.Error(t, err)
	decoded := decodeProtoResponse(t, resp)
	require.Equal(t, proto.StatusServerError, decoded.Status)
	require.NotEmpty(t, decoded.Error)
}

// handleCommitOffset

func TestHandleCommitOffset_BadPayload(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.handleCommitOffset([]byte("not-gob"))
	require.Error(t, err)
	decoded := decodeProtoResponse(t, resp)
	require.Equal(t, proto.StatusServerError, decoded.Status)
	require.NotEmpty(t, decoded.Error)
}
