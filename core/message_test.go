package core

import (
	"testing"
)

func TestNewMessage(t *testing.T) {
	m := NewMessage([]byte("k"), []byte("v"))
	if m == nil || string(m.Key) != "k" {
		t.Error("message creation failed")
	}
}
