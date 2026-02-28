package core

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BasePath == "" {
		t.Error("expected base path")
	}
}
