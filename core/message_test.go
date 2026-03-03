package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMessage(t *testing.T) {
	m := NewMessage([]byte("k"), []byte("v"))
	if m == nil || string(m.Key) != "k" {
		t.Error("message creation failed")
	}
}

func TestNewMessage_SetsChecksum(t *testing.T) {
	m := NewMessage([]byte("hello"), []byte("world"))
	require.NotZero(t, m.Checksum, "NewMessage should compute and store a non-zero checksum")
}

func TestValidateChecksum_ValidMessage(t *testing.T) {
	m := NewMessage([]byte("hello"), []byte("world"))
	require.True(t, m.ValidateChecksum())
}

func TestValidateChecksum_TamperedKey(t *testing.T) {
	m := NewMessage([]byte("hello"), []byte("world"))
	m.Key = []byte("hXllo") // corrupt the key after checksum was computed
	require.False(t, m.ValidateChecksum())
}

func TestValidateChecksum_TamperedValue(t *testing.T) {
	m := NewMessage([]byte("hello"), []byte("world"))
	m.Value = []byte("WORLD") // corrupt the value after checksum was computed
	require.False(t, m.ValidateChecksum())
}

func TestValidateChecksum_WrongChecksum_Fails(t *testing.T) {
	m := &Message{Key: []byte("hello"), Value: []byte("world"), Checksum: 12345}
	require.False(t, m.ValidateChecksum())
}
