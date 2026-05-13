package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type fakeGetShuffleStream struct {
	sent []*pb.ShuffleDataChunk
}

func (f *fakeGetShuffleStream) Send(c *pb.ShuffleDataChunk) error {
	f.sent = append(f.sent, c)
	return nil
}
func (f *fakeGetShuffleStream) SetHeader(md metadata.MD) error  { return nil }
func (f *fakeGetShuffleStream) SendHeader(md metadata.MD) error { return nil }
func (f *fakeGetShuffleStream) SetTrailer(md metadata.MD)       {}
func (f *fakeGetShuffleStream) Context() context.Context        { return context.Background() }
func (f *fakeGetShuffleStream) SendMsg(m interface{}) error     { return nil }
func (f *fakeGetShuffleStream) RecvMsg(m interface{}) error     { return nil }

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

func TestGetShuffleData_EmitsSegmentChecksumDelimiters(t *testing.T) {
	s := NewShuffleServer()
	s.data["job-3"] = map[int32][][]byte{
		1: {
			[]byte("abc"),
			[]byte("xyz"),
		},
	}

	stream := &fakeGetShuffleStream{}
	if err := s.GetShuffleData(&pb.ShuffleDataRequest{JobId: "job-3", PartitionId: 1}, stream); err != nil {
		t.Fatalf("GetShuffleData failed: %v", err)
	}

	// Two data segments and two explicit end-of-segment delimiter chunks.
	if len(stream.sent) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(stream.sent))
	}
	if string(stream.sent[0].Data) != "abc" {
		t.Fatalf("unexpected first payload: %q", string(stream.sent[0].Data))
	}
	if len(stream.sent[1].Data) != 0 || !stream.sent[1].SegmentEnd {
		t.Fatalf("expected first delimiter chunk with segment_end=true, got %+v", stream.sent[1])
	}
	if string(stream.sent[2].Data) != "xyz" {
		t.Fatalf("unexpected second payload: %q", string(stream.sent[2].Data))
	}
	if len(stream.sent[3].Data) != 0 || !stream.sent[3].SegmentEnd {
		t.Fatalf("expected final delimiter chunk with segment_end=true, got %+v", stream.sent[3])
	}

	sumA := sha256.Sum256([]byte("abc"))
	sumB := sha256.Sum256([]byte("xyz"))
	if stream.sent[1].SegmentChecksum != hex.EncodeToString(sumA[:]) {
		t.Fatalf("unexpected first segment checksum: %s", stream.sent[1].SegmentChecksum)
	}
	if stream.sent[3].SegmentChecksum != hex.EncodeToString(sumB[:]) {
		t.Fatalf("unexpected second segment checksum: %s", stream.sent[3].SegmentChecksum)
	}
}
