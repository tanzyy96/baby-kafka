package utils

import (
	"hash/fnv"
)

// PartitionFor gets the partition index for message key given numPartitions.
// We use uint32 to avoid overflows
func PartitionFor(key string, numPartitions uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))

	return uint32(h.Sum32()) % numPartitions
}
