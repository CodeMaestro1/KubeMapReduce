package main

import "testing"

func TestParseReplicaIndexFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     int
	}{
		{name: "single digit ordinal", hostname: "manager-2", want: 2},
		{name: "multi digit ordinal", hostname: "manager-10", want: 10},
		{name: "missing suffix", hostname: "manager", want: 0},
		{name: "invalid suffix", hostname: "manager-a", want: 0},
		{name: "empty hostname", hostname: "", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseReplicaIndexFromHostname(tc.hostname); got != tc.want {
				t.Fatalf("parseReplicaIndexFromHostname(%q) = %d, want %d", tc.hostname, got, tc.want)
			}
		})
	}
}

func TestResolveReplicaIndexUsesStatefulsetOrdinalWhenValid(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "7")
	if got := resolveReplicaIndex("manager-2"); got != 7 {
		t.Fatalf("resolveReplicaIndex should use STATEFULSET_ORDINAL, got %d", got)
	}
}

func TestResolveReplicaIndexFallsBackToHostname(t *testing.T) {
	t.Setenv("STATEFULSET_ORDINAL", "invalid")
	if got := resolveReplicaIndex("manager-12"); got != 12 {
		t.Fatalf("resolveReplicaIndex should fall back to hostname, got %d", got)
	}
}
