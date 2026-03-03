package core

import (
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

// Request to consume message from topic and partition at a specific offset
type ConsumeRequest struct {
	GroupId        string
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

type FetchOffsetRequest struct {
	GroupId        string
	Topic          string
	PartitionIndex int32
}

type FetchOffsetResponse struct {
	Offset int64
}

type CommitOffsetRequest struct {
	GroupId        string
	Topic          string
	PartitionIndex int32
	Offset         int64
}

// Producer looking to produce a message will send a frame with the following structure:
func (s *Server) handleProduce(payload []byte) (resp []byte, respErr error) {
	var (
		req    ProduceRequest
		status = proto.StatusOK
	)

	if err := proto.GobDecode(payload, &req); err != nil {
		respErr = fmt.Errorf("failed to decode produce request: %w", err)
		status = proto.StatusBadRequest
	}

	partitionIndex, offset, err := s.broker.Produce(req.Topic, *NewMessage(req.Key, req.Value))
	if err != nil && respErr == nil {
		respErr = fmt.Errorf("failed to produce message: %w", err)
		status = proto.StatusServerError
	}

	pResp := ProduceResponse{PartitionIndex: partitionIndex, Offset: offset}
	data, encodeErr := proto.GobEncode(&pResp)
	if encodeErr != nil && respErr == nil {
		respErr = fmt.Errorf("failed to encode ProduceResponse: %w", encodeErr)
		status = proto.StatusServerError
	}

	errMsg := ""
	if respErr != nil {
		errMsg = respErr.Error()
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg, Data: data})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode produce response: %w", encErr)
	}
	return respBytes, respErr
}

// Consumer looking to read message from a particular topic + partition + offset
func (s *Server) handleConsume(payload []byte) (resp []byte, respErr error) {
	var (
		req    ConsumeRequest
		status = proto.StatusOK
	)

	if err := proto.GobDecode(payload, &req); err != nil {
		respErr = fmt.Errorf("failed to decode consume request: %w", err)
		status = proto.StatusBadRequest
	}

	cResp := ConsumeResponse{}
	msg, err := s.broker.Consume(req.Topic, req.PartitionIndex, req.Offset)
	if err != nil && respErr == nil {
		respErr = fmt.Errorf("failed to consume message: %w", err)
		status = proto.StatusServerError
	} else {
		cResp.Message = *msg
	}

	data, encodeErr := proto.GobEncode(&cResp)
	if encodeErr != nil && respErr == nil {
		respErr = fmt.Errorf("failed to encode ConsumeResponse: %w", encodeErr)
		status = proto.StatusServerError
	}

	errMsg := ""
	if respErr != nil {
		errMsg = respErr.Error()
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg, Data: data})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode consume response: %w", encErr)
	}
	return respBytes, respErr
}

func (s *Server) handleCreateTopic(payload []byte) (resp []byte, err error) {
	var (
		req    CreateTopicRequest
		status = proto.StatusOK
	)

	if decodeErr := proto.GobDecode(payload, &req); decodeErr != nil {
		err = fmt.Errorf("failed to decode create topic request: %v", decodeErr)
	}

	if createErr := s.broker.CreateTopic(req.Topic, req.NumPartitions); createErr != nil && err == nil {
		err = fmt.Errorf("failed to create topic: %v", createErr)
	}

	var errMsg string
	if err != nil {
		errMsg = err.Error()
		status = proto.StatusServerError
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode createTopic response: %w", encErr)
	}
	return respBytes, err
}

func (s *Server) handleListTopics() (resp []byte, err error) {
	status := proto.StatusOK
	errMsg := ""

	topics := s.broker.ListTopics()
	data, encodeErr := proto.GobEncode(&ListTopicsResponse{Topics: topics})
	if encodeErr != nil {
		log.Errorf("failed to encode topicsResp: %s", encodeErr.Error())
		status = proto.StatusServerError
		errMsg = encodeErr.Error()
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg, Data: data})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode listTopics response: %w", encErr)
	}
	return respBytes, nil
}

func (s *Server) handleFetchOffset(payload []byte) (resp []byte, err error) {
	var (
		req    FetchOffsetRequest
		status = proto.StatusOK
	)

	if decodeErr := proto.GobDecode(payload, &req); decodeErr != nil {
		err = fmt.Errorf("failed to decode fetch offset request: %v", decodeErr)
	}

	offset, fetchErr := s.broker.FetchOffset(req.GroupId, req.Topic, req.PartitionIndex)
	if fetchErr != nil && err == nil {
		err = fmt.Errorf("failed to fetch offset: %v", fetchErr)
	}

	var errMsg string
	foResp := FetchOffsetResponse{}
	if err != nil {
		log.Warnf("handleFetchOffset error: %s", err.Error())
		errMsg = err.Error()
		status = proto.StatusServerError
	} else {
		foResp.Offset = offset
	}

	data, encodeErr := proto.GobEncode(&foResp)
	if encodeErr != nil && errMsg == "" {
		errMsg = encodeErr.Error()
		status = proto.StatusServerError
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg, Data: data})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode fetchOffset response: %w", encErr)
	}
	return respBytes, err
}

func (s *Server) handleCommitOffset(payload []byte) (resp []byte, err error) {
	var (
		req    CommitOffsetRequest
		status = proto.StatusOK
	)

	if decodeErr := proto.GobDecode(payload, &req); decodeErr != nil {
		err = fmt.Errorf("failed to decode commit offset request: %v", decodeErr)
	}

	if commitErr := s.broker.CommitOffset(req.GroupId, req.Topic, req.PartitionIndex, req.Offset); commitErr != nil && err == nil {
		err = fmt.Errorf("failed to commit offset: %v", commitErr)
	}

	var errMsg string
	if err != nil {
		errMsg = err.Error()
		status = proto.StatusServerError
	}

	respBytes, encErr := proto.GobEncode(proto.Response{Status: status, Error: errMsg})
	if encErr != nil {
		return nil, fmt.Errorf("failed to encode commitOffset response: %w", encErr)
	}
	return respBytes, err
}
