package shuffle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestMerge_SingleReader(t *testing.T) {
	input := `{"key": "a", "value": "1"}
{"key": "b", "value": "2"}
`
	expected := `{"key":"a","value":"1"}
{"key":"b","value":"2"}
`
	readers := []io.Reader{strings.NewReader(input)}
	var buf bytes.Buffer
	cfg := DefaultMergeConfig()

	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	if buf.String() != expected {
		t.Errorf("Expected output %q, got %q", expected, buf.String())
	}
	if stats.TotalRecords != 2 {
		t.Errorf("Expected 2 records, got %d", stats.TotalRecords)
	}
}

func TestMerge_TwoReaders(t *testing.T) {
	r1 := `{"key": "a", "value": "1"}
{"key": "c", "value": "3"}
`
	r2 := `{"key": "b", "value": "2"}
{"key": "d", "value": "4"}
`
	expected := `{"key":"a","value":"1"}
{"key":"b","value":"2"}
{"key":"c","value":"3"}
{"key":"d","value":"4"}
`
	readers := []io.Reader{strings.NewReader(r1), strings.NewReader(r2)}
	var buf bytes.Buffer
	cfg := DefaultMergeConfig()

	_, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	if buf.String() != expected {
		t.Errorf("Expected output %q, got %q", expected, buf.String())
	}
}

func TestMerge_EmptyReaders(t *testing.T) {
	var buf bytes.Buffer
	cfg := DefaultMergeConfig()
	stats, err := MergeInputs(nil, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}
	if stats.TotalRecords != 0 {
		t.Errorf("Expected 0 records, got %d", stats.TotalRecords)
	}
}

func TestMerge_DuplicateKeys(t *testing.T) {
	r1 := `{"key": "a", "value": "1"}
`
	r2 := `{"key": "a", "value": "2"}
`
	expected := `{"key":"a","value":"1"}
{"key":"a","value":"2"}
`
	readers := []io.Reader{strings.NewReader(r1), strings.NewReader(r2)}
	var buf bytes.Buffer
	cfg := DefaultMergeConfig()

	_, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	if buf.String() != expected {
		t.Errorf("Expected output %q, got %q", expected, buf.String())
	}
}

func TestMerge_MultiPass(t *testing.T) {
	// 5 readers, batch size 2
	// Pass 1: merge(r1, r2) -> s1, merge(r3, r4) -> s2, r5 remains
	// Pass 2: merge(s1, s2, r5) -> output (since 3 <= batch size is false, actually 3 > 2)
	// Pass 1: 5 readers -> batch1(2), batch2(2), batch3(1) -> 3 spill files
	// Pass 2: 3 spill files -> batch1(2), batch2(1) -> 2 spill files
	// Pass 3: 2 spill files -> final output

	inputs := []string{
		`{"key": "e", "value": "5"}`,
		`{"key": "d", "value": "4"}`,
		`{"key": "c", "value": "3"}`,
		`{"key": "b", "value": "2"}`,
		`{"key": "a", "value": "1"}`,
	}
	// Sort them for inputs
	readers := make([]io.Reader, len(inputs))
	for i, in := range inputs {
		readers[i] = strings.NewReader(in + "\n")
	}

	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 2}

	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	expected := `{"key":"a","value":"1"}
{"key":"b","value":"2"}
{"key":"c","value":"3"}
{"key":"d","value":"4"}
{"key":"e","value":"5"}
`
	if buf.String() != expected {
		t.Errorf("Expected output %q, got %q", expected, buf.String())
	}

	if stats.TotalPasses < 2 {
		t.Errorf("Expected at least 2 passes, got %d", stats.TotalPasses)
	}
	if stats.SpillCount == 0 {
		t.Errorf("Expected at least one spill file")
	}
}

func TestMerge_StressLargeFanIn(t *testing.T) {
	numReaders := 100
	recordsPerReader := 10
	readers := make([]io.Reader, numReaders)

	for i := 0; i < numReaders; i++ {
		var b bytes.Buffer
		for j := 0; j < recordsPerReader; j++ {
			fmt.Fprintf(&b, `{"key": "%05d", "value": "%d"}`+"\n", j, i)
		}
		readers[i] = bytes.NewReader(b.Bytes())
	}

	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 10}

	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	if stats.TotalRecords != int64(numReaders*recordsPerReader) {
		t.Errorf("Expected %d records, got %d", numReaders*recordsPerReader, stats.TotalRecords)
	}

	// Verify sorting
	scanner := bufio.NewScanner(&buf)
	lastKey := ""
	count := 0
	for scanner.Scan() {
		var rec Record
		json.Unmarshal(scanner.Bytes(), &rec)
		if rec.Key < lastKey {
			t.Fatalf("Output not sorted: %s < %s at record %d", rec.Key, lastKey, count)
		}
		lastKey = rec.Key
		count++
	}
}

func TestMerge_MalformedJSON(t *testing.T) {
	r1 := `{"key": "a", "value": "1"}`
	r2 := `{"key": "b", "value": "2" INVALID`

	readers := []io.Reader{strings.NewReader(r1), strings.NewReader(r2)}
	var buf bytes.Buffer
	cfg := DefaultMergeConfig()

	_, err := MergeInputs(readers, &buf, cfg)
	if err == nil {
		t.Fatal("Expected error for malformed JSON, got nil")
	}
}

func TestMerge_StatsPopulated(t *testing.T) {
	// 5 readers, batch size 2
	inputs := []string{
		`{"key": "e", "value": "5"}`,
		`{"key": "d", "value": "4"}`,
		`{"key": "c", "value": "3"}`,
		`{"key": "b", "value": "2"}`,
		`{"key": "a", "value": "1"}`,
	}
	readers := make([]io.Reader, len(inputs))
	for i, in := range inputs {
		readers[i] = strings.NewReader(in + "\n")
	}

	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 2}

	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	// 5 readers, BatchSize 2
	// Pass 1: 5 -> batch1(2), batch2(2), batch3(1) -> 3 spills
	// Pass 2: 3 -> batch1(2), batch2(1) -> 2 spills
	// Pass 3: 2 -> final output
	if stats.TotalPasses != 3 {
		t.Errorf("Expected 3 passes, got %d", stats.TotalPasses)
	}
	if stats.SpillCount != 5 {
		t.Errorf("Expected 5 total spills (3+2), got %d", stats.SpillCount)
	}
	if stats.TotalRecords != 5 {
		t.Errorf("Expected 5 records, got %d", stats.TotalRecords)
	}
}

func TestMerge_AllEmptyReaders(t *testing.T) {
	numReaders := 10
	readers := make([]io.Reader, numReaders)
	for i := 0; i < numReaders; i++ {
		readers[i] = strings.NewReader("")
	}

	var buf bytes.Buffer
	cfg := DefaultMergeConfig()
	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}
	if stats.TotalRecords != 0 {
		t.Errorf("Expected 0 records, got %d", stats.TotalRecords)
	}
}

func TestMerge_SingleRecordHighFanIn(t *testing.T) {
	numReaders := 50
	readers := make([]io.Reader, numReaders)
	for i := 0; i < numReaders; i++ {
		if i == 25 {
			readers[i] = strings.NewReader(`{"key": "x", "value": "found"}` + "\n")
		} else {
			readers[i] = strings.NewReader("")
		}
	}

	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 10}
	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}
	if stats.TotalRecords != 1 {
		t.Errorf("Expected 1 record, got %d", stats.TotalRecords)
	}
	expected := `{"key":"x","value":"found"}` + "\n"
	if buf.String() != expected {
		t.Errorf("Expected %q, got %q", expected, buf.String())
	}
}

func TestMerge_BatchSizeOneUsesDefault(t *testing.T) {
	readers := []io.Reader{strings.NewReader(`{"key": "a", "value": "1"}` + "\n")}
	var buf bytes.Buffer
	// BatchSize 1 is invalid, should fallback to 500
	cfg := MergeConfig{BatchSize: 1}
	_, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}
}

func TestMerge_BatchSizeExceedsMax(t *testing.T) {
	readers := []io.Reader{strings.NewReader(`{"key": "a", "value": "1"}` + "\n")}
	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 10000}
	_, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}
}

func TestMerge_StressLargeFanIn_1000(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	numReaders := 1000
	recordsPerReader := 5
	readers := make([]io.Reader, numReaders)

	for i := 0; i < numReaders; i++ {
		var b bytes.Buffer
		for j := 0; j < recordsPerReader; j++ {
			fmt.Fprintf(&b, `{"key": "%05d", "value": "%d"}`+"\n", j, i)
		}
		readers[i] = bytes.NewReader(b.Bytes())
	}

	var buf bytes.Buffer
	cfg := MergeConfig{BatchSize: 100}

	stats, err := MergeInputs(readers, &buf, cfg)
	if err != nil {
		t.Fatalf("MergeInputs failed: %v", err)
	}

	expectedRecords := int64(numReaders * recordsPerReader)
	if stats.TotalRecords != expectedRecords {
		t.Errorf("Expected %d records, got %d", expectedRecords, stats.TotalRecords)
	}

	// Verify sorting
	scanner := bufio.NewScanner(&buf)
	lastKey := ""
	count := 0
	for scanner.Scan() {
		var rec Record
		json.Unmarshal(scanner.Bytes(), &rec)
		if rec.Key < lastKey {
			t.Fatalf("Output not sorted: %s < %s at record %d", rec.Key, lastKey, count)
		}
		lastKey = rec.Key
		count++
	}
}

func TestMerge_OversizedRecordRespectsLimit(t *testing.T) {
	largeValue := strings.Repeat("x", 2048)
	input := fmt.Sprintf(`{"key": "a", "value": "%s"}`+"\n", largeValue)

	readers := []io.Reader{strings.NewReader(input)}
	var buf bytes.Buffer
	// Set limit smaller than record
	cfg := MergeConfig{MaxRecordBytes: 1024}

	_, err := MergeInputs(readers, &buf, cfg)
	if err == nil {
		t.Fatal("Expected error for oversized record, got nil")
	}
	if !strings.Contains(err.Error(), "token too long") {
		t.Errorf("Expected 'token too long' error, got: %v", err)
	}
}
