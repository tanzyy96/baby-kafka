package helper

import "fmt"

func GetLogname(folderPrefix string, partitionIndex int64) string {
	padded := fmt.Sprintf("%04d", partitionIndex)
	return fmt.Sprintf("%s/log-%s", folderPrefix, padded)
}
