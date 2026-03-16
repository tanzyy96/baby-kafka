package client

import (
	"os"

	"github.com/charmbracelet/log"
)

func deriveLogger(parent *log.Logger, prefix string) *log.Logger {
	return log.NewWithOptions(os.Stderr, log.Options{
		Prefix: prefix,
		Level:  parent.GetLevel(),
	})
}
