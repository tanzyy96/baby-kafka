package core_test

import (
	"os"
	"testing"

	core "baby-kafka/core"
	testutils "baby-kafka/core/test_utils"

	"github.com/stretchr/testify/require"
)

func newMetadataManager(t *testing.T) core.MetadataManager {
	dir, err := os.MkdirTemp("", "testLogDir")
	require.NoError(t, err)

	rolloverLimit := int64(1024 * 1024) // 1MB for testing

	tm, err := core.NewMetadataManager(dir, rolloverLimit, testutils.TestLogger())
	require.NoError(t, err)

	return tm
}

func TestMetadataManagerAssignPartitions(t *testing.T) {
	// tm := core.NewMetadataManager()

	// testCases := []struct {
	// 	Description       string
	// 	Topic             string
	// 	NumBrokers        int
	// 	NumPartitions     int
	// 	ReplicationFactor int
	// }{
	// 	// {"test-topic"},
	// }
}

func TestMetadataManager_PartitionsResponsibleFor(t *testing.T) {
	tm := newMetadataManager(t)

	err := tm.Init(
		[]core.BrokerConfig{{Addr: ":0", Index: 0}, {Addr: ":0", Index: 1}},
		"test",
		2,
		1,
	)
	require.NoError(t, err)

	partitions := tm.PartitionsResponsibleFor(0, "test")
	require.Len(t, partitions, 2)
}

func TestMetadataManager_PartitionsResponsibleFor_More(t *testing.T) {
	tm := newMetadataManager(t)

	err := tm.Init(
		[]core.BrokerConfig{{Addr: ":0", Index: 0}, {Addr: ":0", Index: 1}},
		"test",
		3,
		1,
	)
	require.NoError(t, err)

	partitions := tm.PartitionsResponsibleFor(0, "test")
	partitions = append(partitions, tm.PartitionsResponsibleFor(1, "test")...)
	require.Len(t, partitions, 6) // 3 partitions per broker
}
