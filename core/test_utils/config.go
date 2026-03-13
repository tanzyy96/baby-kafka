// Package testutils provides shared test configurations for Baby Kafka.
package testutils

import (
	"fmt"
	"os"
	"testing"

	"baby-kafka/core"

	"github.com/stretchr/testify/require"
)

// SharedTestConfig returns a test configuration with the specified number of brokers.
// It also initialises the test directory, which is accessible via the BasePath field.
func SharedTestConfig(t *testing.T, numBrokers int) *core.Config {
	dir, err := os.MkdirTemp("", "testLogDir")
	require.NoError(t, err)

	defaultConfig := core.DefaultConfig()

	brokerConfigs := make([]core.BrokerConfig, numBrokers)
	for i := range numBrokers {
		brokerConfigs[i] = core.BrokerConfig{Index: int32(i), Addr: fmt.Sprintf(":%d", 9090+i)}
	}

	defaultConfig.BasePath = dir
	defaultConfig.Brokers = brokerConfigs

	return defaultConfig
}
