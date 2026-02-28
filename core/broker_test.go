package core_test

import (
	"os"
	"testing"

	core "baby-kafka/core"

	"github.com/stretchr/testify/require"
)

func newTestBroker(t *testing.T) *core.Broker {
	dir, err := os.MkdirTemp("", "testLogDir")
	require.NoError(t, err)

	rolloverSize := int64(1024 * 1024) // 1MB for testing

	broker, err := core.NewBroker(dir, rolloverSize)
	require.NoError(t, err)
	return broker
}

func TestBrokerCreateTopicAndGetTopic(t *testing.T) {
	b := newTestBroker(t)
	err := b.CreateTopic("test", 1)
	require.NoError(t, err)

	topic, err := b.GetTopic("test")
	require.NoError(t, err)
	require.Equal(t, "test", topic.Key)
}

func TestBrokerListTopics(t *testing.T) {
	b := newTestBroker(t)

	err := b.CreateTopic("test", 1)
	require.NoError(t, err)

	err = b.CreateTopic("test2", 1)
	require.NoError(t, err)

	topics := b.ListTopics()
	require.Contains(t, topics, "test")
	require.Contains(t, topics, "test2")
}
