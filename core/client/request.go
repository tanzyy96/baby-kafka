package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

// Protocol: 4 byte message length + 1 byte message type + N bytes payload
func writeRequest(w *bufio.Writer, msgType int, payload interface{}) error {
	if payload == nil {
		payload = struct{}{}
	}
	encoded, err := proto.GobEncode(payload)
	if err != nil {
		return fmt.Errorf("failed to encode produce request: %w", err)
	}
	b := bytes.NewBuffer(encoded)

	length := uint32(1 + len(b.Bytes())) // 1 byte for message type + payload length
	buffer := new(bytes.Buffer)

	log.Debug("Writing request of type", "msgType", msgType, "type+payloadLength", length)

	if err := binary.Write(buffer, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write request length: %w", err)
	}
	if err := buffer.WriteByte(byte(msgType)); err != nil {
		return fmt.Errorf("failed to write request message type: %w", err)
	}
	if _, err := buffer.Write(b.Bytes()); err != nil {
		return fmt.Errorf("failed to write request payload: %w", err)
	}

	_, err = w.Write(buffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// Protocol: 4 byte message length + N bytes payload (gob-encoded response struct)
func readResponse(r io.Reader, resp *proto.Response) error {
	// Read length of message
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("failed to read response length: %w", err)
	}
	log.Debugf("Receiving message with length: %d", length)

	// Response has no message type
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return fmt.Errorf("failed to read response payload: %w", err)
	}

	if err := proto.GobDecode(payload, resp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	log.Debug("Received response", "status", resp.Status, "error", resp.Error, "dataLength", len(resp.Data))
	return nil
}
