package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"google.golang.org/grpc"

	pb "kubemapreduce/proto"
	"kubemapreduce/worker-service/internal/shuffle"
)

// TestWorker_MapSpillsLargeOutput drives the full runMap pipeline with a tiny
// MAP_SORT_SPILL_THRESHOLD_MB and an input large enough to exceed it, ensuring
// that the spill-to-disk path is exercised end-to-end. The test verifies that
// every input record reaches a partition and that records inside each
// partition remain sorted by key after the spill+merge pass (issue #152).
func TestWorker_MapSpillsLargeOutput(t *testing.T) {
	store := newMockStorage()

	// Build ~1.5 MiB of input so a 1 MiB threshold definitely triggers spills.
	// Using moderately large values keeps the record count low and the test fast.
	const numRecords = 60
	const valueSize = 30 * 1024 // 30 KiB per record => ~1.8 MiB total
	largeValue := strings.Repeat("v", valueSize)

	var inputBuf bytes.Buffer
	enc := json.NewEncoder(&inputBuf)
	wantKeys := make([]string, numRecords)
	for i := 0; i < numRecords; i++ {
		// Scramble keys so the input is not pre-sorted; use FNV-friendly format.
		k := fmt.Sprintf("k-%05d", (i*131+7)%(numRecords*9))
		wantKeys[i] = k
		if err := enc.Encode(map[string]string{"key": k, "value": largeValue}); err != nil {
			t.Fatalf("encode input record %d: %v", i, err)
		}
	}
	store.put("mapreduce-inputs", "data.jsonl", inputBuf.Bytes())
	store.put("code", "mapper.py", []byte("# mock"))

	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			a := mapAssignment("s3://mapreduce-inputs/data.jsonl")
			a.TotalReducers = 4
			return a, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			return &pb.Ack{Success: true}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Errorf("unexpected failure: %s", req.ErrorMessage)
			return &pb.Ack{}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	// Force a 1 MiB threshold to ensure the spill path runs for this input.
	w.cfg.MapSortSpillThresholdMB = 1

	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Walk every partition output, verifying:
	//   1. records are sorted by key within each partition,
	//   2. the union across partitions equals the original input.
	totalSeen := 0
	gotKeys := make([]string, 0, numRecords)
	for objKey, data := range store.uploaded {
		if !strings.Contains(objKey, "/partition-") {
			continue
		}
		var prevKey string
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var rec shuffle.Record
			if err := json.Unmarshal(line, &rec); err != nil {
				t.Fatalf("decode partition %s: %v", objKey, err)
			}
			if prevKey != "" && rec.Key < prevKey {
				t.Fatalf("partition %s not sorted: %q before %q", objKey, prevKey, rec.Key)
			}
			prevKey = rec.Key
			gotKeys = append(gotKeys, rec.Key)
			totalSeen++
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("scan partition %s: %v", objKey, err)
		}
	}

	if totalSeen != numRecords {
		t.Fatalf("total partitioned records = %d, want %d", totalSeen, numRecords)
	}

	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("key %d: got %q, want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

// TestWorker_MapInMemoryFastPathStillWorks ensures the no-spill fast path is
// unaffected by the spill integration: a small input with the default
// threshold should produce exactly the expected partitioned output.
func TestWorker_MapInMemoryFastPathStillWorks(t *testing.T) {
	store := newMockStorage()

	input := `{"key":"banana","value":"1"}` + "\n" +
		`{"key":"apple","value":"2"}` + "\n" +
		`{"key":"cherry","value":"3"}` + "\n"
	store.put("mapreduce-inputs", "data.jsonl", []byte(input))
	store.put("code", "mapper.py", []byte("# mock"))

	grpcClient := &mockGRPCClient{
		registerFn: func(_ context.Context, _ *pb.RegisterRequest, _ ...grpc.CallOption) (*pb.TaskAssignment, error) {
			a := mapAssignment("s3://mapreduce-inputs/data.jsonl")
			a.TotalReducers = 1
			return a, nil
		},
		heartbeatFn: func(_ context.Context, _ *pb.HeartbeatRequest, _ ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
			return &pb.HeartbeatResponse{Action: pb.HeartbeatResponse_CONTINUE}, nil
		},
		taskCompleteFn: func(_ context.Context, _ *pb.TaskCompleteRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			return &pb.Ack{Success: true}, nil
		},
		taskFailedFn: func(_ context.Context, req *pb.TaskFailedRequest, _ ...grpc.CallOption) (*pb.Ack, error) {
			t.Errorf("unexpected failure: %s", req.ErrorMessage)
			return &pb.Ack{}, nil
		},
	}

	w := newTestWorker(t, grpcClient, store)
	// Default cfg.MapSortSpillThresholdMB == 0 -> defaultSpillThresholdBytes (256 MiB).
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var keys []string
	for objKey, data := range store.uploaded {
		if !strings.Contains(objKey, "/partition-") {
			continue
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		for sc.Scan() {
			var rec shuffle.Record
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				t.Fatalf("decode: %v", err)
			}
			keys = append(keys, rec.Key)
		}
	}
	want := []string{"apple", "banana", "cherry"}
	if len(keys) != len(want) {
		t.Fatalf("got %d keys, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("key %d: got %q, want %q", i, keys[i], want[i])
		}
	}
}
