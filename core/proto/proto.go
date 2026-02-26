package proto

/*
Protocol package handling our custom wire protocol for communication between clients and the server. This includes functions to read and write frames, as well as a helper function to write error responses in a consistent format.
*/

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ReadFrame(r io.Reader) (msgType int, payload []byte, err error) {
	// Read length of message
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, nil, fmt.Errorf("failed to read message length: %w", err)
	}
	// Read message type + body together
	frame := make([]byte, length)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, fmt.Errorf("failed to read message frame: %w", err)
	}

	msgType = int(frame[0])
	payload = frame[1:]

	return msgType, payload, nil
}

func WriteFrame(w io.Writer, resp []byte) error {
	// Write length-prefix
	length := uint32(len(resp))

	// We use binary.Write for typed writing and conn.Write for raw bytes
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write response length: %w", err)
	}
	if _, err := w.Write(resp); err != nil {
		return fmt.Errorf("failed to write response body: %w", err)
	}
	return nil
}

func WriteError(w io.Writer, err error) error {
	// We can define a special error message format, for example: "ERROR:<error message>"
	errorResp := []byte("ERROR:" + err.Error())
	return WriteFrame(w, errorResp)
}
