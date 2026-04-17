package manager

import (
	"strings"
	"testing"
)

func TestBuildWorkerJobName_DNSLabelBounded(t *testing.T) {
	taskID := strings.Repeat("A", 100) + "{}"
	sanitized := sanitizeForDNSLabel(taskID)
	jobName := buildWorkerJobName(sanitized)

	if len(jobName) > 63 {
		t.Fatalf("expected job name length <= 63, got %d", len(jobName))
	}
	if strings.ToLower(jobName) != jobName {
		t.Fatalf("expected lower-case job name, got %q", jobName)
	}
}
