package worker

import (
	"bufio"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"testing"

	"kubemapreduce/worker-service/internal/shuffle"
)

// readAllJSONL drains an io.Reader of JSONL records and returns them as a slice.
func readAllJSONL(t *testing.T, r io.Reader) []shuffle.Record {
	t.Helper()
	var out []shuffle.Record
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec shuffle.Record
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

func TestSpillingSorter_InMemoryFastPath(t *testing.T) {
	s := newSpillingSorter(1<<20, t.TempDir())
	in := []shuffle.Record{
		{Key: "zebra", Value: "1"},
		{Key: "apple", Value: "2"},
		{Key: "mango", Value: "3"},
	}
	if err := s.AddAll(in); err != nil {
		t.Fatalf("AddAll: %v", err)
	}
	if got := s.SpillCount(); got != 0 {
		t.Fatalf("expected 0 spills with large threshold, got %d", got)
	}

	rc, err := s.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer rc.Close()

	got := readAllJSONL(t, rc)
	wantKeys := []string{"apple", "mango", "zebra"}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d records, want %d", len(got), len(wantKeys))
	}
	for i, k := range wantKeys {
		if got[i].Key != k {
			t.Errorf("record %d: key = %q, want %q", i, got[i].Key, k)
		}
	}
}

func TestSpillingSorter_SpillsWhenThresholdExceeded(t *testing.T) {
	// 32-byte threshold + 32-byte overhead per record => every record forces a spill.
	s := newSpillingSorter(32, t.TempDir())
	in := []shuffle.Record{
		{Key: "k3", Value: "v3"},
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	}
	if err := s.AddAll(in); err != nil {
		t.Fatalf("AddAll: %v", err)
	}
	if s.SpillCount() == 0 {
		t.Fatalf("expected at least one spill file with low threshold, got 0")
	}

	rc, err := s.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer rc.Close()

	got := readAllJSONL(t, rc)
	if len(got) != len(in) {
		t.Fatalf("got %d records, want %d", len(got), len(in))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Key > got[i].Key {
			t.Fatalf("output not sorted: %q before %q", got[i-1].Key, got[i].Key)
		}
	}
	wantValues := map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}
	for _, rec := range got {
		if wantValues[rec.Key] != rec.Value {
			t.Errorf("record key=%q value=%q does not match input", rec.Key, rec.Value)
		}
	}
}

func TestSpillingSorter_MergesSpillsAndTrailingBuffer(t *testing.T) {
	// Force several spill files plus a non-empty trailing in-memory buffer.
	s := newSpillingSorter(80, t.TempDir())

	const n = 50
	in := make([]shuffle.Record, n)
	for i := 0; i < n; i++ {
		in[i] = shuffle.Record{
			Key:   string(rune('a'+(i*7)%26)) + string(rune('a'+i%26)),
			Value: "v",
		}
	}
	if err := s.AddAll(in); err != nil {
		t.Fatalf("AddAll: %v", err)
	}
	if s.SpillCount() < 2 {
		t.Fatalf("expected multiple spill files, got %d", s.SpillCount())
	}

	rc, err := s.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer rc.Close()

	got := readAllJSONL(t, rc)
	if len(got) != n {
		t.Fatalf("got %d records, want %d", len(got), n)
	}

	wantKeys := make([]string, n)
	for i, rec := range in {
		wantKeys[i] = rec.Key
	}
	sort.Strings(wantKeys)
	for i, k := range wantKeys {
		if got[i].Key != k {
			t.Fatalf("record %d: key = %q, want %q", i, got[i].Key, k)
		}
	}
}

func TestSpillingSorter_EmptyInputProducesEmptyStream(t *testing.T) {
	s := newSpillingSorter(1<<20, t.TempDir())
	rc, err := s.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	defer rc.Close()
	if got := readAllJSONL(t, rc); len(got) != 0 {
		t.Fatalf("expected empty stream, got %d records", len(got))
	}
}
