package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"time"
)

type Message struct {
	Key       []byte
	Value     []byte
	CreatedAt time.Time
	Checksum  uint32
}

type MessageWithOffset struct {
	Message *Message
	Offset  int64
}

func NewMessage(key, value []byte) *Message {
	v := []byte(key)
	v = append(v, value...)
	return &Message{
		Key:       key,
		Value:     value,
		Checksum:  crc32.ChecksumIEEE(v),
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
	if valid := msg.ValidateChecksum(); !valid {
		return nil, fmt.Errorf("invalid checksum for message")
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

// We calculate the checksum of the message using checksumFn(key+value)
func (m *Message) ChecksumValue() uint32 {
	v := []byte(m.Key)
	v = append(v, m.Value...)
	return crc32.ChecksumIEEE(v)
}

func (m *Message) ValidateChecksum() bool {
	return m.Checksum == m.ChecksumValue()
}
