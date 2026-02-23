package utils

import (
	"fmt"
	"path/filepath"
	"strings"
)

func ChangeExt(filePath, newExt string) string {
	ext := filepath.Ext(filePath)
	base := strings.TrimSuffix(filePath, ext)

	return base + newExt
}

// Assumes filename is format "baseOffset.extension", e.g. "00000000.log"
func BaseOffsetFromFilename(path string) int64 {
	parts := strings.Split(filepath.Base(path), ".")
	if len(parts) < 2 {
		return 0
	}
	var offset int64
	fmt.Sscanf(parts[0], "%d", &offset)
	return offset
}
