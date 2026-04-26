package worker

import (
	"encoding/json"
	"sort"
	"testing"

	"kubemapreduce/pkg/shuffle"
)

// ── hashPartition ─────────────────────��───────────────────────────────────────

func TestHashPartition_Deterministic(t *testing.T) {
	got1 := hashPartition("hello", 8)
	got2 := hashPartition("hello", 8)
	if got1 != got2 {
		t.Errorf("hashPartition not deterministic: %d vs %d", got1, got2)
	}
}

func TestHashPartition_InRange(t *testing.T) {
	R := 7
	keys := []string{"", "a", "key", "longkeystring", "α", "中文"}
	for _, k := range keys {
		p := hashPartition(k, R)
		if p < 0 || p >= R {
			t.Errorf("hashPartition(%q, %d) = %d: out of [0, %d)", k, R, p, R)
		}
	}
}

func TestHashPartition_SinglePartition(t *testing.T) {
	for _, k := range []string{"a", "b", "c"} {
		if p := hashPartition(k, 1); p != 0 {
			t.Errorf("hashPartition(%q, 1) = %d, want 0", k, p)
		}
	}
}

func TestHashPartition_DistributesKeys(t *testing.T) {
	R := 4
	counts := make([]int, R)
	for i := 0; i < 1000; i++ {
		key := string(rune('a' + i%26))
		counts[hashPartition(key, R)]++
	}
	// Each bucket should get at least some keys for a typical set.
	for i, c := range counts {
		if c == 0 {
			t.Errorf("bucket %d got 0 keys out of 1000", i)
		}
	}
}

// ── parseJSONLRecords ─────────────────────────���──────────────────────────��────

func TestParseJSONLRecords_Valid(t *testing.T) {
	input := `{"key":"k1","value":"v1"}` + "\n" + `{"key":"k2","value":"v2"}` + "\n"
	records, err := parseJSONLRecords([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("want 2, got %d", len(records))
	}
	if records[0].Key != "k1" || records[0].Value != "v1" {
		t.Errorf("record[0]: %+v", records[0])
	}
	if records[1].Key != "k2" || records[1].Value != "v2" {
		t.Errorf("record[1]: %+v", records[1])
	}
}

func TestParseJSONLRecords_SkipsBlankLines(t *testing.T) {
	input := `{"key":"k","value":"v"}` + "\n\n"
	records, err := parseJSONLRecords([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("want 1 record, got %d", len(records))
	}
}

func TestParseJSONLRecords_InvalidJSON(t *testing.T) {
	_, err := parseJSONLRecords([]byte("not-json\n"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseJSONLRecords_Empty(t *testing.T) {
	records, err := parseJSONLRecords([]byte(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("want 0, got %d", len(records))
	}
}

// ── marshalRecords ────────────────────────────���──────────────────────────────��

func TestMarshalRecords_RoundTrip(t *testing.T) {
	in := []shuffle.Record{{Key: "foo", Value: "bar"}, {Key: "baz", Value: "qux"}}
	lines := marshalRecords(in)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	for i, l := range lines {
		var r shuffle.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if r != in[i] {
			t.Errorf("line %d: got %+v, want %+v", i, r, in[i])
		}
	}
}

// ── sortRecords (via sort.Slice) ──────────────────────���───────────────────────

func TestSortRecords_LexOrder(t *testing.T) {
	records := []shuffle.Record{
		{Key: "zebra", Value: "1"},
		{Key: "apple", Value: "2"},
		{Key: "mango", Value: "3"},
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	want := []string{"apple", "mango", "zebra"}
	for i, r := range records {
		if r.Key != want[i] {
			t.Errorf("records[%d].Key = %q, want %q", i, r.Key, want[i])
		}
	}
}

// ── jsonlReader ───────────────────────��───────────────────────────────────────

func TestJSONLReader(t *testing.T) {
	lines := [][]byte{
		[]byte(`{"key":"a","value":"1"}`),
		[]byte(`{"key":"b","value":"2"}`),
	}
	r := jsonlReader(lines)
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	got := string(buf[:n])
	want := `{"key":"a","value":"1"}` + "\n" + `{"key":"b","value":"2"}` + "\n"
	if got != want {
		t.Errorf("jsonlReader: got %q, want %q", got, want)
	}
}
