package core_test

import (
	"os"
	"testing"

	core "baby-kafka/core"

	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T) core.Server {
	dir, err := os.MkdirTemp("", "babykafka_test")
	require.NoError(t, err)

	cfg := core.DefaultConfig()
	cfg.BasePath = dir
	cfg.Brokers = []core.BrokerConfig{{Index: 0, Addr: ":0"}}
	s, err := core.NewServer(cfg, 0)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return s
}

func TestNewServer(t *testing.T) {
	s := newTestServer(t)
	require.NotNil(t, s)
}
