package core

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/charmbracelet/log"
)

const MaxSize int64 = 1000 // 1kb

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

type Partition interface {
	ID() int32
	BasePath() string
	Logs() []*Log

	Append(msg Message) (offset int64, err error)
	ReadAt(offset int64) (*Message, error)
	AppendReplicated(msg MessageWithOffset) error
}

type partition struct {
	Path  string // Partition folder path
	Index int32

	// Locks read operations during writes & rollovers
	mutex     sync.RWMutex
	logs      []*Log
	activeLog *Log
	maxSize   int64 // cap for rollover
	logger    *log.Logger

	// Replication stuff
	// isLeader bool
	// leaderState *LeaderState
}

// NewPartition should create a new log and then register it to this partition. If the partition already exists, it will load it.
func NewPartition(index int32, folderPath string, maxSize int64, logger *log.Logger) (Partition, error) {
	partitionPath := fmt.Sprintf("%s/partition-%d", folderPath, index)

	if err := os.MkdirAll(partitionPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create partition directory: %w", err)
	}

	firstLog, err := NewLog(0, partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}

	if maxSize == 0 {
		maxSize = MaxSize
	}

	partLogger := deriveLogger(logger, fmt.Sprintf("partition-%d", index))
	p := &partition{
		Path:      partitionPath,
		Index:     index,
		activeLog: firstLog,
		logs:      []*Log{firstLog},
		maxSize:   maxSize,
		logger:    partLogger,
	}
	p.logger.Infof("created at %s", partitionPath)
	return p, nil
}

func LoadPartitions(basePath string, maxSize int64, logger *log.Logger) (partitions map[int32]Partition, err error) {
	f, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}
	partitions = make(map[int32]Partition)
	partLogger := deriveLogger(logger, "partition")

	// Consider chance of rubbish files
	for _, f := range f {
		if !f.IsDir() || !strings.Contains(f.Name(), "partition-") {
			partLogger.Warnf("skipping loading of %s", f.Name())
			continue
		}
		parts := strings.Split(f.Name(), "-")
		idx := parts[len(parts)-1]
		pidx, err := strconv.Atoi(idx)
		if err != nil {
			partLogger.Warn("unable to load partition file", "name", f.Name())
			continue
		}
		p, err := LoadPartition(int32(pidx), basePath, maxSize, logger)
		if err != nil {
			partLogger.Warnf("unable to load partition file %s: %s", f.Name(), err.Error())
			continue
		}
		partitions[int32(pidx)] = p
	}

	return partitions, nil
}

// LoadPartition reads existing logs in the partition folder and sets the active log to the last one. This is used when we restart the server and need to load existing partitions.
func LoadPartition(index int32, folderPath string, maxSize int64, logger *log.Logger) (Partition, error) {
	partitionPath := fmt.Sprintf("%s/partition-%d", folderPath, index)
	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		return NewPartition(index, folderPath, maxSize, logger)
	}

	// Load logs
	files, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read partition directory: %w", err)
	}
	logs := []*Log{}
	partLogger := deriveLogger(logger, fmt.Sprintf("partition-%d", index))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if file.Name()[len(file.Name())-4:] != ".log" {
			continue
		}
		path := fmt.Sprintf("%s/%s", partitionPath, file.Name())
		lg, err := LoadLog(path)
		if err != nil {
			partLogger.Warnf("failed to load log at %s: %v", path, err)
		}
		logs = append(logs, lg)
	}

	active := activeLog(logs)
	p := &partition{
		Path:      partitionPath,
		Index:     index,
		maxSize:   maxSize,
		logs:      logs,
		activeLog: active,
		logger:    partLogger,
	}
	p.logger.Debugf("loaded with %d log segment(s)", len(logs))
	return p, nil
}

func (p *partition) ID() int32 {
	return p.Index
}

func (p *partition) BasePath() string {
	return p.Path
}

func (p *partition) Logs() []*Log {
	return p.logs
}

func (p *partition) Append(msg Message) (offset int64, err error) {
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

func (p *partition) AppendReplicated(msg MessageWithOffset) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.shouldRoll() {
		if err := p.rollover(); err != nil {
			return fmt.Errorf("failed to append message: %w", err)
		}
	}

	_, err := p.activeLog.AppendReplicated(msg)
	return err
}

func (p *partition) ReadAt(offset int64) (*Message, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Search the logs array to find the log corresponding to the offset. We can use the baseOffset of each log to determine if the offset falls within that log's range.
	for _, log := range p.logs {
		if offset >= log.baseOffset && offset < log.baseOffset+log.nextOffset {
			return log.ReadAt(offset)
		}
	}

	return nil, ErrOffsetNotFound
}

// Rollover creates a new log file and sets it as the active log. The old log is added to the logs array.
func (p *partition) rollover() error {
	newBaseOffset := p.activeLog.baseOffset + p.activeLog.nextOffset
	newLog, err := NewLog(newBaseOffset, p.Path)
	if err != nil {
		return fmt.Errorf("failed to perform log rollover: %w", err)
	}
	p.logger.Infof("rolling over: new segment at offset %d (total segments: %d)", newBaseOffset, len(p.logs)+1)
	p.activeLog = newLog
	p.logs = append(p.logs, newLog)

	return nil
}

// A partition should roll to the next log when the active
// log exceeds maxSize.
func (p *partition) shouldRoll() bool {
	return p.activeLog.size >= p.maxSize
}

// Log name is 20-padded offset, starting from 00000000000000000000.log
// We can use the nextOffset of the active log to determine the name of the next log, since it represents the offset of the next message to be appended.
func (p *partition) getNextLogname() string {
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
