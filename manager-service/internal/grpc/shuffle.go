package grpc

import (
	"fmt"
	"io"
	"sync"

	pb "kubemapreduce/proto"
)

type ShuffleServer struct {
	pb.UnimplementedShuffleServiceServer
	// data maps jobID -> partitionID -> aggregated bytes
	data   map[string]map[int32][]byte
	dataMu sync.RWMutex
}

func NewShuffleServer() *ShuffleServer {
	return &ShuffleServer{
		data: make(map[string]map[int32][]byte),
	}
}

func (s *ShuffleServer) PushShuffleData(stream pb.ShuffleService_PushShuffleDataServer) error {
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.Ack{Success: true})
		}
		if err != nil {
			return err
		}

		s.dataMu.Lock()
		if _, ok := s.data[chunk.JobId]; !ok {
			s.data[chunk.JobId] = make(map[int32][]byte)
		}
		s.data[chunk.JobId][chunk.PartitionId] = append(s.data[chunk.JobId][chunk.PartitionId], chunk.Data...)
		s.dataMu.Unlock()
	}
}

func (s *ShuffleServer) GetShuffleData(req *pb.ShuffleDataRequest, stream pb.ShuffleService_GetShuffleDataServer) error {
	s.dataMu.RLock()
	jobData, ok := s.data[req.JobId]
	if !ok {
		s.dataMu.RUnlock()
		return fmt.Errorf("no shuffle data for job %s", req.JobId)
	}
	partitionData, ok := jobData[req.PartitionId]
	s.dataMu.RUnlock()

	if !ok {
		// Empty partition is fine
		return nil
	}

	// Send in 1MB chunks
	const chunkSize = 1024 * 1024
	for i := 0; i < len(partitionData); i += chunkSize {
		end := i + chunkSize
		if end > len(partitionData) {
			end = len(partitionData)
		}
		if err := stream.Send(&pb.ShuffleDataChunk{
			JobId:       req.JobId,
			PartitionId: req.PartitionId,
			Data:        partitionData[i:end],
		}); err != nil {
			return err
		}
	}
	return nil
}
