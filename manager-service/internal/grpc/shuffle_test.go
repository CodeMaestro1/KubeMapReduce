package grpc

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "kubemapreduce/proto"
)

type fakePushShuffleStream struct {
	chunks  []*pb.ShuffleDataChunk
	recvIdx int
	ack     *pb.Ack
	sendErr error
	recvErr error
}

func (f *fakePushShuffleStream) Recv() (*pb.ShuffleDataChunk, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	if f.recvIdx >= len(f.chunks) {
		return nil, io.EOF
	}
	chunk := f.chunks[f.recvIdx]
	f.recvIdx++
	return chunk, nil
}

func (f *fakePushShuffleStream) SendAndClose(ack *pb.Ack) error {
	f.ack = ack
	return f.sendErr
}

func (f *fakePushShuffleStream) SetHeader(md metadata.MD) error  { return nil }
func (f *fakePushShuffleStream) SendHeader(md metadata.MD) error { return nil }
func (f *fakePushShuffleStream) SetTrailer(md metadata.MD)       {}
func (f *fakePushShuffleStream) Context() context.Context        { return context.Background() }
func (f *fakePushShuffleStream) SendMsg(m interface{}) error     { return nil }
func (f *fakePushShuffleStream) RecvMsg(m interface{}) error     { return nil }

func TestPushShuffleData_RejectsOversizedPartitionStream(t *testing.T) {
	s := NewShuffleServer()

	stream := &fakePushShuffleStream{
		chunks: []*pb.ShuffleDataChunk{
			{
				JobId:       "job-1",
				PartitionId: 0,
				Data:        make([]byte, maxShufflePartitionBytes),
			},
			{
				JobId:       "job-1",
				PartitionId: 0,
				Data:        []byte{1},
			},
		},
	}

	err := s.PushShuffleData(stream)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", st.Code())
	}
}

func TestPushShuffleData_StoresPayloadOnEOF(t *testing.T) {
	s := NewShuffleServer()
	stream := &fakePushShuffleStream{
		chunks: []*pb.ShuffleDataChunk{
			{JobId: "job-2", PartitionId: 2, Data: []byte("hello")},
			{JobId: "job-2", PartitionId: 2, Data: []byte(" world")},
		},
	}

	if err := s.PushShuffleData(stream); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if stream.ack == nil || !stream.ack.Success {
		t.Fatalf("expected success ack, got %+v", stream.ack)
	}

	s.dataMu.RLock()
	defer s.dataMu.RUnlock()
	got := s.data["job-2"][2]
	if len(got) != 1 {
		t.Fatalf("expected one stored payload blob, got %d", len(got))
	}
	if string(got[0]) != "hello world" {
		t.Fatalf("unexpected payload: %q", string(got[0]))
	}
}
