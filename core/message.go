package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

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
