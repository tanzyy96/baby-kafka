package core

import (
	"fmt"
	"os"
	"sync"
)

const MAX_SIZE int64 = 1000 // 1kb

/*
Partition is the grouping of log segments, and manages the rollover process.
The purpose of partitions is to promote concurrency, so we can default to single partition first.
Example of directory:
/partition-0

	/00000000.log
	/00000000.index
	/00000001.log
	/00000001.index
*/
type Partition struct {
	Path  string // Partition folder path
	Index int32

	mutex     sync.RWMutex // Locks read operations during writes & rollovers
	logs      []*Log
	activeLog *Log
	maxSize   int64 // cap for rollover
}

// Should create a new log and then register it to this partition
func NewPartition(index int32, folderPath string, maxSize int64) (*Partition, error) {
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
		Index:     index,
		activeLog: log,
		logs:      []*Log{log},
		maxSize:   maxSize,
	}, nil
}

// Read existing logs in the partition folder and set the active log to the last one. This is used when we restart the server and need to load existing partitions.
func LoadPartition(index int64, folderPath string, maxSize int64) (*Partition, error) {
	partitionPath := fmt.Sprintf("%s/partition-%d", folderPath, index)
	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("partition %s directory not found", partitionPath)
	}

	// Load logs
	files, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition directory: %w", err)
	}
	logs := []*Log{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if file.Name()[len(file.Name())-4:] != ".log" {
			continue
		}
		path := fmt.Sprintf("%s/%s", partitionPath, file.Name())
		log, err := LoadLog(path)
		if err != nil {
			// TODO: maybe intro a module-level logger
			fmt.Printf("failed to load log at %s: %v\n", path, err)
		}
		logs = append(logs, log)
	}

	// Determine active log based on max offset
	log := activeLog(logs)

	return &Partition{
		Path:      partitionPath,
		maxSize:   maxSize,
		logs:      logs,
		activeLog: log,
	}, nil
}

func (p *Partition) Append(msg Message) (offset int64, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.shouldRoll() {
		if err := p.rollover(); err != nil {
			return 0, fmt.Errorf("failed to append message: %w", err)
		}
	}

	offset, _, err = p.activeLog.Append(msg)
	return offset, err
}

func (p *Partition) ReadAt(offset int64) (*Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

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

func activeLog(logs []*Log) *Log {
	var active *Log
	for _, log := range logs {
		if active == nil || log.baseOffset > active.baseOffset {
			active = log
		}
	}
	return active
}
