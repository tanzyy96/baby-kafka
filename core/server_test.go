package core

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T) *Server {
	dir, err := os.MkdirTemp("", "babykafka_test")
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.BasePath = dir
	cfg.Brokers = []BrokerConfig{{Index: 0, Port: ":0"}}
	s, err := NewServer(cfg, 0)
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
