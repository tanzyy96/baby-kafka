package babykafka

import "fmt"

/*
Partition is a logical grouping of messages. It should support functions like:
- Append messages
- Read messages by offset
*/
type Partition struct {
	currentLog Log
}

func NewPartition(folderPath string) (*Partition, error) {
	logPath := fmt.Sprintf("%s/log-0", folderPath)
	log, err := NewLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition: %w", err)
	}

	return &Partition{
		currentLog: *log,
	}, nil
}

func (p *Partition) Append(msg Message) (offset int64, err error) {
	// TODO: rollover to a new log if the current log exceeds max size
	// TODO: maintain an index for faster reads
	// TODO: update nextOffset after appending the message
	offset, _, err = p.currentLog.Append(msg)
	return offset, err
}

func (p *Partition) ReadAt(offset int64) (*Message, error) {
	return p.currentLog.ReadAt(offset)
}
