package client_test

import (
	"net"
	"testing"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/core/proto"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_Send(t *testing.T) {
	dir := t.TempDir()
	getMetadataFunc := func(msgType int, req []byte) proto.Response {
		assert.Equal(t, core.MessageTypeGetMetadata, msgType)

		var reqMsg core.GetMetadataRequest
		err := proto.GobDecode(req, &reqMsg)
		assert.NoError(t, err)
		assert.Equal(t, "test-topic", reqMsg.Topic)

		data, err := proto.GobEncode(core.GetMetadataResponse{
			Metadata: &core.TopicMetadata{
				Topic:         "test-topic",
				NumPartitions: 1,
				PartitionMetadata: []core.PartitionMetadata{
					{LeaderAddr: ":0", Leader: 0, Replicas: []int32{1}},
				},
			},
		})

		assert.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	produceFunc := func(msgType int, req []byte) proto.Response {
		assert.Equal(t, core.MessageTypeProduce, msgType)

		var reqMsg core.ProduceRequest
		err := proto.GobDecode(req, &reqMsg)
		assert.NoError(t, err)
		assert.Equal(t, "test-topic", reqMsg.Topic)
		assert.Equal(t, []byte("key1"), reqMsg.Key)
		assert.Equal(t, []byte("value1"), reqMsg.Value)

		data, err := proto.GobEncode(core.ProduceResponse{Offset: 3})
		assert.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	clientConn, _ := testutils.NewTestConn(t, getMetadataFunc, produceFunc)
	testDialFn := func(addr string) (net.Conn, error) {
		return clientConn, nil
	}

	cfg := &core.Config{
		BasePath: dir,
		Brokers: []core.BrokerConfig{
			{Index: 0, Addr: ":0"},
			{Index: 1, Addr: ":0"},
		},
		ReplicationFactor: 1,
	}

	producer, err := client.NewProducer(cfg, client.WithDialFn(testDialFn))
	require.NoError(t, err)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Offset)
}
