package client_test

import (
	"testing"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/core/proto"
	"baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

func TestProducerSend(t *testing.T) {
	addr := test_utils.NewMockServer(t, func(msgType int, _ []byte) proto.Response {
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
