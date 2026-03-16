package core_test

import (
	"net"
	"testing"

	core "baby-kafka/core"
	"baby-kafka/core/proto"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/assert"
)

// TODO: TestBrokerClient_Broadcast

func TestBrokerClient_FetchLog(t *testing.T) {
	fetchLogFunc := func(msgType int, req []byte) proto.Response {
		var reqMsg core.FetchLogRequest
		err := proto.GobDecode(req, &reqMsg)
		assert.NoError(t, err)

		respMsg := core.FetchLogResponse{
			Messages: []*core.MessageWithOffset{
				{
					Message: &core.Message{
						Key:   []byte("key"),
						Value: []byte("value"),
					},
					Offset: 0,
				},
			},
		}

		data, err := proto.GobEncode(respMsg)
		assert.NoError(t, err)
		return proto.Response{Status: proto.StatusOK, Data: data}
	}

	clientConn, _ := testutils.NewTestConn(t, fetchLogFunc)

	testDialFn := func(addr string) (net.Conn, error) {
		return clientConn, nil
	}

	client, _ := core.NewBrokerClient(":0", testutils.TestLogger(), core.WithDialFn(testDialFn))
	resp, err := client.FetchLog("topic", 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, []*core.MessageWithOffset{
		{
			Message: &core.Message{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
			Offset: 0,
		},
	},
		resp.Messages)
}
