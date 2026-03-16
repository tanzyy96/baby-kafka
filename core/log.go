package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"baby-kafka/internal/utils"
)

var ErrNotImplemented = errors.New("not implemented")

const lognameLength = 20

/*
Log is a single log file that stores messages. Multiple logs will build up to a partition, as they have a maxSize.
When a log is full, it will perform a rollover.
*/
type Log struct {
	file       *os.File
	index      *LogIndex
	size       int64
	baseOffset int64 // Starts from 0 in the first log only, then non-zero for subsequent logs
	nextOffset int64 // counter for number of appended messages, we use this to jump via index
}

// We use baseOffset to write the log name as 00...00.log
func NewLog(baseOffset int64, pathPrefix string) (*Log, error) {
	if pathPrefix == "" {
		pathPrefix = "./"
	}
	padded := fmt.Sprintf("%020d", baseOffset)

	filePrefix := pathPrefix + "/" + padded
	filePath := fmt.Sprintf("%s.log", filePrefix)

	// If file exists, use LoadLog
	if _, err := os.Stat(filePath); err == nil {
		return LoadLog(filePath)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}
	index, err := NewLogIndex(filePrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to create log index: %w", err)
	}

	return &Log{
		file:       file,
		index:      index,
		size:       0,
		baseOffset: baseOffset,
		nextOffset: 0,
	}, nil
}

func LoadLog(path string) (*Log, error) {
	if err := validateLogPath(path); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to load log at %s: %w", path, err)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for log at %s: %w", path, err)
	}
	size := stat.Size()

	indexPath := indexPath(path)
	index, err := LoadLogIndex(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load log index at %s: %w", indexPath, err)
	}

	return &Log{
		file:       file,
		index:      index,
		size:       size,
		baseOffset: utils.BaseOffsetFromFilename(path),
		nextOffset: index.Count(),
	}, nil
}

// Returns new file offset
func (l *Log) Append(msg Message) (offset int64, bytePos int64, err error) {
	// Serialize the message and write to the log file
	// Update the size of the log
	serialized, err := msg.Serialize()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Write length prefix, then payload
	// BigEndian is just a byte-order convention, it doesn't affect the actual data being stored, but it ensures consistency when reading the log later.
	prefix := int64(len(serialized))
	if err := binary.Write(l.file, binary.BigEndian, prefix); err != nil {
		return 0, 0, fmt.Errorf("failed to write message length prefix: %w", err)
	}

	n, err := l.file.Write(serialized)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write message to log: %w", err)
	}

	currSize := l.size
	currOffset := l.nextOffset
	l.size += int64(n) + 8 // 8 bytes for the length prefix -> 8 * 8 = 64
	l.nextOffset++

	// Write to index
	if err := l.index.Append(int32(currOffset), int32(currSize)); err != nil {
		return 0, 0, fmt.Errorf("failed to write to index: %w", err)
	}

	return currOffset, l.size, nil
}

// AppendReplicated inserts message into log with specific offset and updates index
// We need to support this because replication may not always be msg1, msg2, msg3...
//
// Duplicating from compacted logs may return msg1, msg5, msg7...
// So we need to support that too. It also helps with sanity check to ensure the replication is correct.
func (l *Log) AppendReplicated(msg MessageWithOffset) (bytePos int64, err error) {
	serialized, err := msg.Message.Serialize()
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	prefix := int64(len(serialized))
	if err := binary.Write(l.file, binary.BigEndian, prefix); err != nil {
		return 0, fmt.Errorf("failed to write message length prefix: %w", err)
	}

	n, err := l.file.Write(serialized)
	if err != nil {
		return 0, fmt.Errorf("failed to write message to log: %w", err)
	}

	currSize := l.size
	currOffset := msg.Offset // Use offset from msg instead
	l.size += int64(n) + 8   // 8 bytes for the length prefix -> 8 * 8 = 64
	l.nextOffset = msg.Offset + 1

	// Write to index
	if err := l.index.Append(int32(currOffset), int32(currSize)); err != nil {
		return 0, fmt.Errorf("failed to write to index: %w", err)
	}

	return l.size, nil
}

// ReadAt reads a message from the log based on absolute offset. This is performed via the log index.
// so ReadAt(1003) would translate to ReadAt(3) on the log with baseOffset 1000
func (l *Log) ReadAt(absoluteOffset int64) (*Message, error) {
	relativeOffset := absoluteOffset - l.baseOffset
	if relativeOffset < 0 {
		return nil, fmt.Errorf("absoluteOffset cannot be smaller than baseOffset of %d", l.baseOffset)
	}
	// We read the byte position from the index, then read the message from the log file at that byte position
	bytePos, err := l.index.Read(int32(relativeOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read from index: %w", err)
	}
	return l.readAtByte(int64(bytePos))
}

/*
readAtByte reads a message from the log file based on the offset. It should read the length prefix first, then read the message payload.
*/
func (l *Log) readAtByte(bytePos int64) (*Message, error) {
	// Seek to the offset, read the length prefix, then read the message payload
	if _, err := l.file.Seek(bytePos, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}
	var prefix int64
	if err := binary.Read(l.file, binary.BigEndian, &prefix); err != nil {
		return nil, fmt.Errorf("failed to read message length prefix: %w", err)
	}
	payload := make([]byte, prefix)
	if _, err := l.file.ReadAt(payload, bytePos+8); err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	return DeserializeMessage(payload)
}

func (l *Log) Close() error {
	return l.file.Close()
}

func indexPath(logPath string) string {
	return utils.ChangeExt(logPath, ".index")
}

// Log path should be {20 digits}.log, e.g. 00000000000000000000.log
func validateLogPath(filePath string) error {
	// filename must be 20 digit followed by .index
	parts := strings.Split(filePath, "/")
	filename := parts[len(parts)-1]
	r := regexp.MustCompile(`\d{20}.log`)
	if valid := r.MatchString(filename); !valid {
		return fmt.Errorf("invalid log file name")
	}
	return nil
}
