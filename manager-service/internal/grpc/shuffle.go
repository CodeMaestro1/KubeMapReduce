package grpc

import (
	"bytes"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "kubemapreduce/proto"
)

type ShuffleServer struct {
	pb.UnimplementedShuffleServiceServer
	// data maps jobID -> partitionID -> ordered list of map-output blobs.
	data      map[string]map[int32][][]byte
	jobSizes  map[string]int64
	partSizes map[string]map[int32]int64
	dataMu    sync.RWMutex
}

const (
	maxShufflePartitionBytes = int64(256 << 20) // 256 MiB
	maxShuffleJobBytes       = int64(2 << 30)   // 2 GiB
	shuffleChunkSize         = 1 << 20          // 1 MiB
)

func NewShuffleServer() *ShuffleServer {
	return &ShuffleServer{
		data:      make(map[string]map[int32][][]byte),
		jobSizes:  make(map[string]int64),
		partSizes: make(map[string]map[int32]int64),
	}
}

func (s *ShuffleServer) PushShuffleData(stream pb.ShuffleService_PushShuffleDataServer) error {
	var (
		jobID       string
		partitionID int32
		buf         bytes.Buffer
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if jobID == "" {
				return stream.SendAndClose(&pb.Ack{Success: true})
			}
			payload := append([]byte(nil), buf.Bytes()...)

			s.dataMu.Lock()
			if _, ok := s.data[jobID]; !ok {
				s.data[jobID] = make(map[int32][][]byte)
			}
			if _, ok := s.partSizes[jobID]; !ok {
				s.partSizes[jobID] = make(map[int32]int64)
			}
			newPartSize := s.partSizes[jobID][partitionID] + int64(len(payload))
			newJobSize := s.jobSizes[jobID] + int64(len(payload))
			if newPartSize > maxShufflePartitionBytes {
				s.dataMu.Unlock()
				return status.Errorf(codes.ResourceExhausted, "shuffle partition size exceeded for job %s partition %d", jobID, partitionID)
			}
			if newJobSize > maxShuffleJobBytes {
				s.dataMu.Unlock()
				return status.Errorf(codes.ResourceExhausted, "shuffle job size exceeded for job %s", jobID)
			}
			s.data[jobID][partitionID] = append(s.data[jobID][partitionID], payload)
			s.partSizes[jobID][partitionID] = newPartSize
			s.jobSizes[jobID] = newJobSize
			s.dataMu.Unlock()

			return stream.SendAndClose(&pb.Ack{Success: true})
		}
		if err != nil {
			return err
		}
		if chunk.JobId == "" {
			return status.Error(codes.InvalidArgument, "job_id is required")
		}
		if jobID == "" {
			jobID = chunk.JobId
			partitionID = chunk.PartitionId
		}
		if chunk.JobId != jobID || chunk.PartitionId != partitionID {
			return status.Error(codes.InvalidArgument, "all chunks in a stream must target the same job_id and partition_id")
		}
		if int64(buf.Len())+int64(len(chunk.Data)) > maxShufflePartitionBytes {
			return status.Errorf(codes.ResourceExhausted, "shuffle partition stream exceeds max size for job %s partition %d", jobID, partitionID)
		}

		s.dataMu.RLock()
		existingPartSize := s.partSizes[jobID][partitionID]
		existingJobSize := s.jobSizes[jobID]
		s.dataMu.RUnlock()
		incomingSize := int64(buf.Len()) + int64(len(chunk.Data))
		if existingPartSize+incomingSize > maxShufflePartitionBytes {
			return status.Errorf(codes.ResourceExhausted, "shuffle partition size exceeded for job %s partition %d", jobID, partitionID)
		}
		if existingJobSize+incomingSize > maxShuffleJobBytes {
			return status.Errorf(codes.ResourceExhausted, "shuffle job size exceeded for job %s", jobID)
		}
		if _, err := buf.Write(chunk.Data); err != nil {
			return status.Errorf(codes.Internal, "buffer shuffle chunk: %v", err)
		}
	}
}

func (s *ShuffleServer) GetShuffleData(req *pb.ShuffleDataRequest, stream pb.ShuffleService_GetShuffleDataServer) error {
	s.dataMu.RLock()
	jobData, ok := s.data[req.JobId]
	if !ok {
		s.dataMu.RUnlock()
		return status.Errorf(codes.NotFound, "no shuffle data for job %s", req.JobId)
	}
	partitionData, ok := jobData[req.PartitionId]
	s.dataMu.RUnlock()

	if !ok {
		// Empty partition is fine
		return nil
	}

	for idx, blob := range partitionData {
		for i := 0; i < len(blob); i += shuffleChunkSize {
			end := i + shuffleChunkSize
			if end > len(blob) {
				end = len(blob)
			}
			if err := stream.Send(&pb.ShuffleDataChunk{
				JobId:       req.JobId,
				PartitionId: req.PartitionId,
				Data:        blob[i:end],
			}); err != nil {
				return err
			}
		}
		if idx < len(partitionData)-1 {
			// Segment delimiter so consumers can preserve individual map-output boundaries.
			if err := stream.Send(&pb.ShuffleDataChunk{
				JobId:       req.JobId,
				PartitionId: req.PartitionId,
				Data:        nil,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
