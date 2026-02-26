package utils

import "hash/fnv"

// Partitioning with hashkey, return uint to avoid potential overflows
func PartitionFor(key string, numPartitions uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))

	return uint32(h.Sum32()) % numPartitions
}
