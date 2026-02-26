package utils

import "testing"

func TestPartitionFor(t *testing.T) {
	// Test cases
	tests := []struct {
		name          string
		key           string
		numPartitions uint32
		expected      uint32
	}{
		{"Partition for 'key1' with 4 partitions", "key1", 4, 3},
		{"Partition for 'key2' with 4 partitions", "key2", 4, 2},
		{"Partition for 'key3' with 4 partitions", "key3", 4, 1},
		{"Partition for 'key4' with 4 partitions", "key4", 4, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PartitionFor(tt.key, tt.numPartitions)
			if result != tt.expected {
				t.Errorf("Expected partition %d but got %d", tt.expected, result)
			}
		})
	}
}
