package client_test

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"net"
	"testing"

	"baby-kafka/core"
	"baby-kafka/core/client"
	"baby-kafka/core/proto"
	"baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

func TestProducerSend(t *testing.T) {
	addr := test_utils.NewMockServer(t, func(conn net.Conn) {
		// Receive the message and respond with 4 bytes prefixLength + N bytes payload (gob-encoded ProduceResponse)
		pResp := core.ProduceResponse{
			// Response:       proto.Response{Status: proto.StatusOK},
			PartitionIndex: 1,
			Offset:         2,
		}
		data, err := pResp.Encode()
		require.NoError(t, err)

		resp := proto.Response{
			Status: proto.StatusOK,
			Data:   data,
		}

		buf := new(bytes.Buffer)
		err = gob.NewEncoder(buf).Encode(resp)
		require.NoError(t, err)

		err = binary.Write(conn, binary.BigEndian, uint32(buf.Len()))
		require.NoError(t, err)
		_, err = conn.Write(buf.Bytes())
		require.NoError(t, err)
	})

	// Create a producer and send a message
	producer, err := client.NewProducer(addr)
	require.NoError(t, err)

	resp, err := producer.Send("test-topic", []byte("key1"), []byte("value1"))
	require.NoError(t, err)
	require.Equal(t, int32(1), resp.PartitionIndex)
	require.Equal(t, int64(2), resp.Offset)
}
