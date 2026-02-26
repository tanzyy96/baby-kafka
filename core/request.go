package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Response struct {
	Success bool
}

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

// Request to create a new topic with a specified number of partitions
type CreateTopicRequest struct {
	Topic         string
	NumPartitions int32
}

type ListTopicsResponse struct {
	Topics []string
}

// Producer looking to produce a message will send a frame with the following structure:
func (s *Server) handleProduce(payload []byte) (resp []byte, err error) {
	var req ProduceRequest

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode produce request: %w", err)
	}

	// Perform produce
	partitionIndex, offset, err := s.broker.Produce(req.Topic, Message{Key: req.Key, Value: req.Value})
	if err != nil {
		return nil, fmt.Errorf("failed to produce message: %w", err)
	}

	pResp := ProduceResponse{
		PartitionIndex: partitionIndex,
		Offset:         offset,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(pResp); err != nil {
		return nil, fmt.Errorf("failed to encode produce response: %w", err)
	}

	return respBuf.Bytes(), nil
}

// Consumer looking to read message from a particular topic + partition + offset
func (s *Server) handleConsume(payload []byte) (resp []byte, err error) {
	var req ConsumeRequest

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode produce request: %w", err)
	}

	// Perform consume
	msg, err := s.broker.Consume(req.Topic, req.PartitionIndex, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to produce message: %w", err)
	}

	cResp := ConsumeResponse{
		Message: *msg,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(cResp); err != nil {
		return nil, fmt.Errorf("failed to encode consume response: %w", err)
	}

	return respBuf.Bytes(), nil
}

func (s *Server) handleCreateTopic(payload []byte) (resp []byte, err error) {
	var req CreateTopicRequest

	// Decode the payload using gob
	b := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(b)
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode produce request: %w", err)
	}

	// Perform consume
	if err := s.broker.CreateTopic(req.Topic, req.NumPartitions); err != nil {
		return nil, fmt.Errorf("failed to produce message: %w", err)
	}

	ctResp := Response{
		Success: true,
	}

	// Encode the response using gob
	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(ctResp); err != nil {
		return nil, fmt.Errorf("failed to encode consume response: %w", err)
	}

	return respBuf.Bytes(), nil
}

func (s *Server) handleListTopics() (resp []byte, err error) {
	topics := s.broker.ListTopics()
	topicsResp := ListTopicsResponse{
		Topics: topics,
	}

	var respBuf bytes.Buffer
	enc := gob.NewEncoder(&respBuf)
	if err := enc.Encode(topicsResp); err != nil {
		return nil, fmt.Errorf("failed to encode list topics response: %w", err)
	}

	return respBuf.Bytes(), nil
}
