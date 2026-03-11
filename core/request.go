package core

import (
	"fmt"

	"baby-kafka/core/proto"

	"github.com/charmbracelet/log"
)

// Request to create message in topic
type ProduceRequest struct {
	Key            []byte
	Value          []byte
	Topic          string
	PartitionIndex int32
}

type ProduceResponse struct {
	Offset int64
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

type GetMetadataRequest struct {
	Topic string
}

type GetMetadataResponse struct {
	Metadata *TopicMetadata
}

type BroadcastTopicMetadataRequest struct {
	Metadata map[string]*TopicMetadata
}

// handleRequest decodes Req from payload, calls fn, encodes the Resp, and wraps it in a proto.Response frame.
func handleRequest[Req any, Resp any](payload []byte, fn func(Req) (Resp, error)) ([]byte, error) {
	var req Req
	if err := proto.GobDecode(payload, &req); err != nil {
		return buildResponse(proto.StatusServerError, err)
	}
	resp, err := fn(req)
	if err != nil {
		return buildResponse(proto.StatusServerError, err)
	}

	data, err := proto.GobEncode(&resp)
	if err != nil {
		return buildResponse(proto.StatusServerError, fmt.Errorf("failed to encode response: %w", err))
	}
	return buildResponseStatusOK(data)
}

// handleVoidRequest is like handleRequest but for handlers with no response body.
func handleVoidRequest[Req any](payload []byte, fn func(Req) error) ([]byte, error) {
	var req Req
	if err := proto.GobDecode(payload, &req); err != nil {
		return buildResponse(proto.StatusServerError, err)
	}
	if err := fn(req); err != nil {
		return buildResponse(proto.StatusServerError, err)
	}
	return buildResponseStatusOK(nil)
}

// Returns encoded proto.Response with provided status and error message
func buildResponse(status proto.Status, err error) ([]byte, error) {
	b, _ := proto.GobEncode(proto.Response{Status: status, Error: err.Error()})
	return b, err
}

// Returns encoded proto.Response with StatusOK and provided data
func buildResponseStatusOK(data []byte) ([]byte, error) {
	b, err := proto.GobEncode(proto.Response{Status: proto.StatusOK, Data: data})
	if err != nil {
		return nil, fmt.Errorf("failed to encode response: %w", err)
	}
	return b, nil
}

func (s *server) handleProduce(payload []byte) ([]byte, error) {
	return handleRequest(payload, func(req ProduceRequest) (ProduceResponse, error) {
		log.Debugf("Produce: topic=%s partition=%d", req.Topic, req.PartitionIndex)
		offset, err := s.broker.Produce(req.Topic, req.PartitionIndex, *NewMessage(req.Key, req.Value))
		return ProduceResponse{Offset: offset}, err
	})
}

func (s *server) handleConsume(payload []byte) ([]byte, error) {
	return handleRequest(payload, func(req ConsumeRequest) (ConsumeResponse, error) {
		log.Debugf("Consume: group=%s topic=%s partition=%d offset=%d", req.GroupId, req.Topic, req.PartitionIndex, req.Offset)
		msg, err := s.broker.Consume(req.Topic, req.PartitionIndex, req.Offset)
		if err != nil {
			return ConsumeResponse{}, err
		}
		return ConsumeResponse{Message: *msg}, nil
	})
}

func (s *server) handleCreateTopic(payload []byte) ([]byte, error) {
	return handleVoidRequest(payload, func(req CreateTopicRequest) error {
		log.Debugf("CreateTopic: topic=%s numPartitions=%d", req.Topic, req.NumPartitions)
		return s.broker.CreateTopic(req.Topic, req.NumPartitions)
	})
}

func (s *server) handleListTopics() ([]byte, error) {
	topics := s.broker.ListTopics()
	data, err := proto.GobEncode(&ListTopicsResponse{Topics: topics})
	if err != nil {
		return buildResponse(proto.StatusServerError, err)
	}
	return buildResponseStatusOK(data)
}

func (s *server) handleFetchOffset(payload []byte) ([]byte, error) {
	return handleRequest(payload, func(req FetchOffsetRequest) (FetchOffsetResponse, error) {
		log.Debugf("Received FetchOffset: group=%s topic=%s partition=%d", req.GroupId, req.Topic, req.PartitionIndex)
		offset, err := s.broker.FetchOffset(req.GroupId, req.Topic, req.PartitionIndex)
		return FetchOffsetResponse{Offset: offset}, err
	})
}

func (s *server) handleCommitOffset(payload []byte) ([]byte, error) {
	return handleVoidRequest(payload, func(req CommitOffsetRequest) error {
		log.Debugf("Received CommitOffset: group=%s topic=%s partition=%d offset=%d", req.GroupId, req.Topic, req.PartitionIndex, req.Offset)
		return s.broker.CommitOffset(req.GroupId, req.Topic, req.PartitionIndex, req.Offset)
	})
}

func (s *server) handleGetMetadata(payload []byte) ([]byte, error) {
	return handleRequest(payload, func(req GetMetadataRequest) (GetMetadataResponse, error) {
		log.Debug("Received GetMetadata", "topic", req.Topic)
		metadata, err := s.broker.GetTopicMetadata(req.Topic)
		return GetMetadataResponse{Metadata: metadata}, err
	})
}

func (s *server) handleBroadcastMetadata(payload []byte) ([]byte, error) {
	return handleVoidRequest(payload, func(req BroadcastTopicMetadataRequest) error {
		log.Debug("Received BroadcastMetadata", "metadata", req.Metadata)
		return s.broker.InsertMetadata(req.Metadata)
	})
}
