package utils

import (
	"testing"
)

func TestChangeExt(t *testing.T) {
	// Test cases
	tests := []struct {
		name     string
		input    string
		newExt   string
		expected string
	}{
		{"Change .log to .index", "log-00000000.log", ".index", "log-00000000.index"},
		{"Change .txt to .md", "notes.txt", ".md", "notes.md"},
		{"Change .csv to .json", "data.csv", ".json", "data.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ChangeExt(tt.input, tt.newExt)
			if result != tt.expected {
				t.Errorf("Expected %s but got %s", tt.expected, result)
			}
		})
	}
}
