package core

import (
	"testing"
)

func TestProduceRequest(t *testing.T) {
	r := ProduceRequest{Key: []byte("k"), Value: []byte("v"), Topic: "t"}
	if r.Topic != "t" {
		t.Error("topic mismatch")
	}
}
