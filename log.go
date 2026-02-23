package babykafka

import (
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strings"
)

/*
How do indexes in Kafka work?
Each entry is just 2 numbers:
┌─────────────────┬──────────────────┐
│ relative offset │ byte position    │
│    (4 bytes)    │    (4 bytes)     │
└─────────────────┴──────────────────┘

	8 bytes total per entry

So example would be [0,0], [1,37], [2,82]
Real Kafka uses a sparse index that only stores every Nth offset, but for simplicity we can store every offset in our implementation. This allows us to quickly find the byte position of a message given its offset, which is crucial for efficient reads.
*/

const entryWidth = 8

type LogIndex struct {
	file *os.File
	size int64
}

func NewLogIndex(filePrefix string) (*LogIndex, error) {
	indexPath := fmt.Sprintf("%s.index", filePrefix)
	index, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}
	return &LogIndex{
		file: index,
		size: 0,
	}, nil
}

func LoadLogIndex(filepath string) (*LogIndex, error) {
	if err := validateIndexPath(filepath); err != nil {
		return nil, err
	}
	index, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	stat, err := index.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	return &LogIndex{
		file: index,
		size: stat.Size(),
	}, nil
}

func (i *LogIndex) Append(offset int32, bytePos int32) error {
	// 4 bytes for offset, 4 bytes for byte position
	if err := binary.Write(i.file, binary.BigEndian, offset); err != nil {
		return fmt.Errorf("failed to write offset to index: %w", err)
	}

	if err := binary.Write(i.file, binary.BigEndian, bytePos); err != nil {
		return fmt.Errorf("failed to write byte position to index: %w", err)
	}

	i.size += 8

	return nil
}

func (i *LogIndex) Read(offset int32) (bytePos int32, err error) {
	// Jump by n * 8 bytes
	indexBytePos := offset * entryWidth
	if _, err := i.file.Seek(int64(indexBytePos), 0); err != nil {
		return 0, fmt.Errorf("failed to seek to offset in index: %w", err)
	}

	var foundOffset int32
	if err := binary.Read(i.file, binary.BigEndian, &foundOffset); err != nil {
		return 0, fmt.Errorf("failed to read foundOffset on index: %w", err)
	}
	if foundOffset != offset {
		return 0, fmt.Errorf("found incorrect offset on index: %d instead of %d", foundOffset, offset)
	}

	if err := binary.Read(i.file, binary.BigEndian, &bytePos); err != nil {
		return 0, fmt.Errorf("failed to read bytePos on index: %w", err)
	}

	return bytePos, nil
}

// Returns the number of entries in the index, which is the size of the index file divided by the width of each entry (8 bytes).
func (i *LogIndex) Count() int64 {
	return i.size / entryWidth
}

func (i *LogIndex) Close() error {
	return i.file.Close()
}

func validateIndexPath(filePath string) error {
	// filename must be 20 digit followed by .index
	parts := strings.Split(filePath, "/")
	filename := parts[len(parts)-1]
	r := regexp.MustCompile(`\d{20}.index`)
	if valid := r.MatchString(filename); !valid {
		return fmt.Errorf("invalid index file name")
	}
	return nil
}
