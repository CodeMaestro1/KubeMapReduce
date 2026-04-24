package manager

import "context"

// StagingCleaner bulk-deletes the shuffle staging prefix for a job from object
// storage. It is called by the Scheduler during the Cleaning phase, before the
// job is moved to a terminal state (Completed or Failed).
//
// Implementations must be idempotent: deleting a prefix that does not exist
// must not return an error.
type StagingCleaner interface {
	DeleteStagingPrefix(ctx context.Context, jobID string) error
}

// MockStagingCleaner is a no-op StagingCleaner for use in unit tests.
type MockStagingCleaner struct{}

// DeleteStagingPrefix is a no-op in MockStagingCleaner.
func (m *MockStagingCleaner) DeleteStagingPrefix(_ context.Context, _ string) error {
	return nil
}
