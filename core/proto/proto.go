package proto

/*
Protocol package handling our custom wire protocol for communication between clients and the server. This includes functions to read and write frames, as well as a helper function to write error responses in a consistent format.
*/

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/charmbracelet/log"
)

type (
	Status int
	/*
		This acts somewhat like our HTTP protocol. So like HTTP header status and HTTP body
	*/
	Response struct {
		Status Status
		Error  string
		Data   []byte // Gob encoded of original struct
	}
)

func (r *Response) DecodeData(resp interface{}) error {
	if err := gob.NewDecoder(bytes.NewReader(r.Data)).Decode(resp); err != nil {
		return err
	}
	return nil
}

const (
	StatusOK Status = iota
	StatusBadRequest
	StatusServerError
)

/*
Request protocol:
- 4 bytes: length of the message (uint32, big-endian)
- 1 byte: message type (produce, consume, create topic, list topics, etc.)
- N bytes: serialized message (using gob encoding)

Response protocol:
- 4 bytes: length of the message (uint32, big-endian)
- N bytes: serialized Response
*/

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

	log.Info("ReadFrame", "msgType", msgType, "payloadLength", len(payload))

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

	log.Info("WriteFrame", "length", length)
	return nil
}

func WriteError(w io.Writer, status Status, err error) error {
	log.Info("Writing error response", "error", err)

	resp := Response{
		Status: status,
		Error:  err.Error(),
	}
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(&resp); err != nil {
		return fmt.Errorf("failed to encode error response: %w", err)
	}
	return WriteFrame(w, b.Bytes())
}
