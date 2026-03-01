package core

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

// Request to create message in topic
type ProduceRequest struct {
	Key   []byte
	Value []byte
	Topic string
}

type ProduceResponse struct {
	PartitionIndex int32
	Offset         int64
}

func (r *ProduceResponse) Encode() ([]byte, error) {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(&r)
	return b.Bytes(), err
}

// Request to consume message from topic and partition at a specific offset
type ConsumeRequest struct {
	Topic          string
	PartitionIndex int32
	Offset         int64
}

// For the sake of consistency
type ConsumeResponse struct {
	Message
}

func (r *ConsumeResponse) Encode() ([]byte, error) {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(&r)
	return b.Bytes(), err
}

// Request to create a new topic with a specified number of partitions
type CreateTopicRequest struct {
	Topic         string
	NumPartitions int32
}

type ListTopicsResponse struct {
	Topics []string
}

func (r *ListTopicsResponse) Encode() ([]byte, error) {
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(&r)
	return b.Bytes(), err
}

// Producer looking to produce a message will send a frame with the following structure:
func (s *Server) handleProduce(payload []byte) (resp []byte, respErr error) {
	var (
		req    ProduceRequest
		status = proto.StatusOK
	)

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		respErr = fmt.Errorf("failed to decode produce request: %w", err)
		status = proto.StatusBadRequest
	}

	// Perform produce
	partitionIndex, offset, err := s.broker.Produce(req.Topic, Message{Key: req.Key, Value: req.Value})
	if err != nil && respErr == nil {
		respErr = fmt.Errorf("failed to produce message: %w", err)
		status = proto.StatusServerError
	}

	pResp := ProduceResponse{
		PartitionIndex: partitionIndex,
		Offset:         offset,
	}
	data, encodeErr := pResp.Encode()
	if encodeErr != nil && respErr == nil {
		respErr = fmt.Errorf("failed to encode ProduceResponse: %w", encodeErr)
		status = proto.StatusServerError
	}

	errMsg := ""
	if respErr != nil {
		errMsg = respErr.Error()
	}

	response := proto.Response{
		Status: status,
		Error:  errMsg,
		Data:   data,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(response); err != nil {
		return nil, fmt.Errorf("failed to encode produce response: %w", err)
	}

	return respBuf.Bytes(), respErr
}

// Consumer looking to read message from a particular topic + partition + offset
func (s *Server) handleConsume(payload []byte) (resp []byte, respErr error) {
	var (
		req    ConsumeRequest
		status = proto.StatusOK
	)

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		respErr = fmt.Errorf("failed to decode consume request: %w", err)
		status = proto.StatusBadRequest
	}

	// Perform consume
	cResp := ConsumeResponse{}
	msg, err := s.broker.Consume(req.Topic, req.PartitionIndex, req.Offset)
	if err != nil && respErr == nil {
		respErr = fmt.Errorf("failed to consume message: %w", err)
		status = proto.StatusServerError
	} else {
		cResp.Message = *msg
	}

	data, encodeErr := cResp.Encode()
	if encodeErr != nil && respErr == nil {
		respErr = fmt.Errorf("failed to encode ConsumeResponse: %w", encodeErr)
		status = proto.StatusServerError
	}

	errMsg := ""
	if respErr != nil {
		errMsg = respErr.Error()
	}

	response := proto.Response{
		Status: status,
		Error:  errMsg,
		Data:   data,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(response); err != nil {
		return nil, fmt.Errorf("failed to encode consume response: %w", err)
	}

	return respBuf.Bytes(), respErr
}

func (s *Server) handleCreateTopic(payload []byte) (resp []byte, err error) {
	var (
		req    CreateTopicRequest
		status = proto.StatusOK
	)

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if decodeErr := dec.Decode(&req); decodeErr != nil {
		err = fmt.Errorf("failed to decode create topic request: %v", decodeErr)
	}

	// Perform consume
	if createErr := s.broker.CreateTopic(req.Topic, req.NumPartitions); createErr != nil {
		err = fmt.Errorf("failed to create topic: %v", createErr)
	}

	var errMsg string
	if err != nil {
		errMsg = err.Error()
		status = proto.StatusServerError
	}

	ctResp := proto.Response{
		Status: status,
		Error:  errMsg,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(ctResp); err != nil {
		return nil, fmt.Errorf("failed to encode consume response: %w", err)
	}

	return respBuf.Bytes(), err
}

func (s *Server) handleListTopics() (resp []byte, err error) {
	status := proto.StatusOK
	errMsg := ""

	topics := s.broker.ListTopics()
	topicsResp := ListTopicsResponse{
		Topics: topics,
	}
	data, err := topicsResp.Encode()
	if err != nil {
		log.Errorf("failed to encode topicsResp: %s", err.Error())
		status = proto.StatusServerError
		errMsg = err.Error()
	}

	response := proto.Response{
		Status: status,
		Error:  errMsg,
		Data:   data,
	}

	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(response); err != nil {
		return nil, fmt.Errorf("failed to encode list topics response: %w", err)
	}

	return respBuf.Bytes(), nil
}
