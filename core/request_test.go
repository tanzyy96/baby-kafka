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

func TestHandleCommitOffset_UnknownGroup(t *testing.T) {
	// TODO: add a happy path test once OffsetManager can be pre-seeded with entries
	s := newTestServer(t)
	payload := encodePayload(t, CommitOffsetRequest{
		GroupId:        "group-1",
		Topic:          "my-topic",
		PartitionIndex: 0,
		Offset:         42,
	})
	resp, err := s.handleCommitOffset(payload)
	require.Error(t, err)
	decoded := decodeProtoResponse(t, resp)
	require.Equal(t, proto.StatusServerError, decoded.Status)
	require.NotEmpty(t, decoded.Error)
}
