package client_test

import (
	"testing"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/core/proto"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

func TestProducerSend(t *testing.T) {
	addr := testutils.NewMockServer(t, func(msgType int, _ []byte) proto.Response {
		var (
			data []byte
			err  error
		)
		switch msgType {
		case core.MessageTypeGetMetadata:
			data, err = proto.GobEncode(core.GetMetadataResponse{
				Metadata: &core.TopicMetadata{
					Topic:         "test-topic",
					NumPartitions: 1,
				},
			})
		case core.MessageTypeProduce:
			data, err = proto.GobEncode(core.ProduceResponse{Offset: 2})
		}
		require.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	})

	producer, err := client.NewProducer(addr)
	require.NoError(t, err)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.Offset)
}

func TestProducer_Send(t *testing.T) {
	getMetadataFunc := func(msgType int, _ []byte) proto.Response {
		require.Equal(t, core.MessageTypeGetMetadata, msgType)
		data, err := proto.GobEncode(core.GetMetadataResponse{
			Metadata: &core.TopicMetadata{
				Topic:         "test-topic",
				NumPartitions: 1,
			},
		})

		require.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	produceFunc := func(msgType int, _ []byte) proto.Response {
		require.Equal(t, core.MessageTypeProduce, msgType)
		data, err := proto.GobEncode(core.ProduceResponse{Offset: 3})
		require.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	clientConn, _ := testutils.NewTestConn(t, getMetadataFunc, produceFunc)

	producer, err := client.NewProducerFromConn(clientConn)
	require.NoError(t, err)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Offset)
}
