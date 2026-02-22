package babykafka

import (
	"fmt"
	"os"
)

const MAX_SIZE int64 = 1000 // 1kb

/*
Partition is the grouping of log segments, and manages the rollover process.
The purpose of partitions is to promote concurrency, so we can default to single partition first.
*/
type Partition struct {
	Path string // Partition folder path

	logs      []*Log
	activeLog *Log
	maxSize   int64 // cap for rollover
}

// Should create a new log and then register it to this partition
func NewPartition(index int64, folderPath string, maxSize int64) (*Partition, error) {
	partitionPath := fmt.Sprintf("%s/partition-%d", folderPath, index)
	if err := os.MkdirAll(partitionPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	log, err := NewLog(0, partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}

	if maxSize == 0 {
		maxSize = MAX_SIZE
	}

	return &Partition{
		Path:      partitionPath,
		activeLog: log,
		logs:      []*Log{log},
		maxSize:   maxSize,
	}, nil
}

func (p *Partition) Append(msg Message) (offset int64, err error) {
	if p.shouldRoll() {
		if err := p.rollover(); err != nil {
			return 0, fmt.Errorf("failed to append message: %w", err)
		}
	}

	offset, _, err = p.activeLog.Append(msg)
	return offset, err
}

func (p *Partition) ReadAt(offset int64) (*Message, error) {
	// Search the logs array to find the log corresponding to the offset. We can use the baseOffset of each log to determine if the offset falls within that log's range.
	for _, log := range p.logs {
		if offset >= log.baseOffset && offset < log.baseOffset+log.nextOffset {
			fmt.Printf("Reading offset %d from log with baseOffset %d\n", offset, log.baseOffset)
			return log.Read(offset)
		}
	}
	return nil, fmt.Errorf("offset %d not found in any log", offset)
}

// Rollover creates a new log file and sets it as the active log. The old log is added to the logs array.
func (p *Partition) rollover() error {
	newLog, err := NewLog(p.activeLog.baseOffset+p.activeLog.nextOffset, p.Path)
	if err != nil {
		return fmt.Errorf("failed to perform log rollover: %w", err)
	}
	p.activeLog = newLog
	p.logs = append(p.logs, newLog)

	return nil
}

// A partition should roll to the next log when the active
// log exceeds maxSize.
func (p *Partition) shouldRoll() bool {
	return p.activeLog.size >= p.maxSize
}

// Log name is 20-padded offset, starting from 00000000000000000000.log
// We can use the nextOffset of the active log to determine the name of the next log, since it represents the offset of the next message to be appended.
func (p *Partition) getNextLogname() string {
	nextOffset := p.activeLog.nextOffset
	return fmt.Sprintf("%020d", nextOffset)
}
