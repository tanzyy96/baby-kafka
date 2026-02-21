package babykafka

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"
)

var ErrNotImplemented = errors.New("not implemented")

/*
StorageEngine is the interface around the storing of message. It should support functions like:
- Append messages
- Read messages by offset
- Persist to disk

Advanced
- Recover on restart
- Maintain offset metadata
*/
type StorageEngine interface {
	Append(msg Message) error
	ReadAt(offset int64) (*Message, error)
}

/*
Partition is a logical grouping of messages. It should support functions like:
- Append messages
- Read messages by offset
*/
type Partition struct {
	currentLog Log
	nextOffset int64
}

func NewPartition(folderPath string) (*Partition, error) {
	logPath := fmt.Sprintf("%s/log-0", folderPath)
	log, err := NewLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}

	return &Partition{
		currentLog: *log,
		nextOffset: 0,
	}, nil
}

func (p *Partition) Append(msg Message) (offset int64, err error) {
	// TODO: rollover to a new log if the current log exceeds max size
	// TODO: maintain an index for faster reads
	// TODO: update nextOffset after appending the message
	return p.currentLog.Append(msg)
}

func (p *Partition) ReadAt(offset int64) (*Message, error) {
	return p.currentLog.ReadAt(offset)
}

/*
Log is a single log file that stores messages. Multiple logs will build up to a partition, as they have a maxSize.
When a log is full, it will perform a rollover.
*/
type Log struct {
	file *os.File
	// index *os.File
	size int64
}

func NewLog(filePath string) (*Log, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	return &Log{
		file: file,
		size: 0,
	}, nil
}

// Returns new file offset
func (l *Log) Append(msg Message) (offset int64, err error) {
	// Serialize the message and write to the log file
	// Update the size of the log
	serialized, err := msg.Serialize()
	if err != nil {
		return 0, fmt.Errorf("failed to append message: %w", err)
	}

	// Write length prefix, then payload
	// BigEndian is just a byte-order convention, it doesn't affect the actual data being stored, but it ensures consistency when reading the log later.
	prefix := int64(len(serialized))
	if err := binary.Write(l.file, binary.BigEndian, prefix); err != nil {
		return 0, fmt.Errorf("failed to write message length prefix: %w", err)
	}

	n, err := l.file.Write(serialized)
	if err != nil {
		return 0, fmt.Errorf("failed to write message to log: %w", err)
	}

	l.size += int64(n) + 8 // 8 bytes for the length prefix -> 8 * 8 = 64

	return l.size, nil
}

/*
ReadAt reads a message from the log file based on the offset. It should read the length prefix first, then read the message payload.
*/
func (l *Log) ReadAt(offset int64) (*Message, error) {
	// Seek to the offset, read the length prefix, then read the message payload
	if _, err := l.file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset: %w", err)
	}
	var prefix int64
	if err := binary.Read(l.file, binary.BigEndian, &prefix); err != nil {
		return nil, fmt.Errorf("failed to read message length prefix: %w", err)
	}
	payload := make([]byte, prefix)
	if _, err := l.file.ReadAt(payload, offset+8); err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	return DeserializeMessage(payload)
}

func (l *Log) Close() error {
	return l.file.Close()
}

type Message struct {
	Key       []byte
	Value     []byte
	CreatedAt time.Time
}

func NewMessage(key, value []byte) *Message {
	return &Message{
		Key:       key,
		Value:     value,
		CreatedAt: time.Now(),
	}
}

func DeserializeMessage(data []byte) (*Message, error) {
	var msg Message
	b := bytes.NewBuffer(data)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&msg); err != nil {
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}
	return &msg, nil
}

func (m *Message) Serialize() ([]byte, error) {
	// Serialize the message into bytes for storage
	var b bytes.Buffer

	enc := gob.NewEncoder(&b)
	if err := enc.Encode(m); err != nil {
		return nil, fmt.Errorf("failed to serialize message: %w", err)
	}
	return b.Bytes(), nil
}

// Return the total length of the serialized message (length prefix + payload)
func (m *Message) SerializedLength() int64 {
	serialized, err := m.Serialize()
	if err != nil {
		return 0
	}
	return int64(len(serialized) + 8) // 8 bytes for the length prefix
}
