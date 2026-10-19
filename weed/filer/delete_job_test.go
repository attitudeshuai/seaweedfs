package filer

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteJobRecordPersistRoundTrip(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	rec := &DeleteJobRecord{
		JobId: "dj-test-1",
		Path:  "/data/big",
		State: DeleteJobStateRunning,
		Options: DeleteJobOptions{
			IgnoreRecursiveError: true,
			ShouldDeleteChunks:   true,
			IfNotModifiedAfter:   12345,
			KeepRemoteObject:     true,
		},
		Deleted:           42,
		InitialEstimate:   100,
		EstimateTruncated: true,
		Frontier: []DeleteJobFrontier{
			{Dir: "/data/big", LastFileName: "a"},
			{Dir: "/data/big/d", LastFileName: ""},
		},
		CreatedAtMs:  1000,
		StartedAtMs:  2000,
		FinishedAtMs: 0,
		Owner:        "filer-a:8888",
		LeaseUntilMs: 3000,
		Gen:          2,
		RetryCount:   1,
	}
	require.NoError(t, persistDeleteJob(ctx, store, rec))

	loaded, err := loadDeleteJobRecord(ctx, store, "dj-test-1")
	require.NoError(t, err)
	assert.Equal(t, rec, loaded)
}

func TestDeleteJobIndexDedupAndRemove(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	rec := &DeleteJobRecord{JobId: "dj-test-2", Path: "/x", State: DeleteJobStateQueued}
	require.NoError(t, persistDeleteJob(ctx, store, rec))
	require.NoError(t, persistDeleteJob(ctx, store, rec))

	ids, err := loadDeleteJobIndex(ctx, store)
	require.NoError(t, err)
	assert.Equal(t, []string{"dj-test-2"}, ids)

	removeDeleteJob(ctx, store, "dj-test-2")
	ids, err = loadDeleteJobIndex(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, ids)
	_, err = loadDeleteJobRecord(ctx, store, "dj-test-2")
	assert.ErrorIs(t, err, ErrKvNotFound)
}

func TestLoadAllDeleteJobsRebuildsIndex(t *testing.T) {
	store := newStubFilerStore()
	ctx := context.Background()

	require.NoError(t, persistDeleteJob(ctx, store, &DeleteJobRecord{JobId: "dj-a", Path: "/a", State: DeleteJobStateRunning}))
	require.NoError(t, persistDeleteJob(ctx, store, &DeleteJobRecord{JobId: "dj-b", Path: "/b", State: DeleteJobStateQueued}))
	// Simulate a crashed record: indexed but its job key is gone.
	require.NoError(t, storeIndexIDs(ctx, store, []string{"dj-a", "dj-ghost", "dj-b"}))

	records, err := loadAllDeleteJobs(ctx, store)
	require.NoError(t, err)
	var got []string
	for _, r := range records {
		got = append(got, r.JobId)
	}
	assert.ElementsMatch(t, []string{"dj-a", "dj-b"}, got)

	ids, err := loadDeleteJobIndex(ctx, store)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"dj-a", "dj-b"}, ids)
}

func TestDeleteJobStateLifecycle(t *testing.T) {
	assert.True(t, DeleteJobStateQueued.Active())
	assert.True(t, DeleteJobStateRunning.Active())
	assert.True(t, DeleteJobStateCancelling.Active())
	assert.True(t, DeleteJobStateFailed.Active())
	assert.False(t, DeleteJobStateCancelled.Active())
	assert.False(t, DeleteJobStateComplete.Active())

	assert.True(t, DeleteJobStateComplete.Terminal())
	assert.True(t, DeleteJobStateCancelled.Terminal())
	assert.False(t, DeleteJobStateFailed.Terminal())

	assert.True(t, DeleteJobStateFailed.Removable())
	assert.False(t, DeleteJobStateRunning.Removable())

	r := &DeleteJobRecord{InitialEstimate: 10, Deleted: 3}
	assert.Equal(t, int64(7), r.RemainingEstimate())
	r.Deleted = 99
	assert.Equal(t, int64(0), r.RemainingEstimate())
}

func TestAsyncDeleteConfigDefaults(t *testing.T) {
	v := util.GetViper()
	overrides := []string{"enabled", "threshold", "estimate_limit", "batch_size", "batch_interval",
		"conflict_policy", "queue_timeout", "retry_backoff", "retry_max", "scan_interval", "terminal_ttl"}
	for _, key := range overrides {
		v.Set(asyncDeletePrefix+key, nil)
	}
	t.Cleanup(func() {
		for _, key := range overrides {
			v.Set(asyncDeletePrefix+key, nil)
		}
	})

	cfg := loadAsyncDeleteConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, 1000, cfg.Threshold)
	assert.Equal(t, 10000, cfg.EstimateLimit)
	assert.Equal(t, 1024, cfg.BatchSize)
	assert.Equal(t, 100*time.Millisecond, cfg.BatchInterval)
	assert.Equal(t, AsyncDeletePolicyReject, cfg.ConflictPolicy)
	assert.Equal(t, 30*time.Second, cfg.QueueTimeout)
	assert.Equal(t, 5*time.Second, cfg.RetryBackoff)
	assert.Equal(t, 5, cfg.RetryMax)
	assert.Equal(t, 2*time.Second, cfg.ScanInterval)
	assert.Equal(t, 24*time.Hour, cfg.TerminalTTL)
}

func TestAsyncDeleteConfigInvalidPolicyFallsBack(t *testing.T) {
	v := util.GetViper()
	v.Set(asyncDeletePrefix+"conflict_policy", "bogus")
	t.Cleanup(func() { v.Set(asyncDeletePrefix+"conflict_policy", nil) })
	cfg := loadAsyncDeleteConfig()
	assert.Equal(t, AsyncDeletePolicyReject, cfg.ConflictPolicy)
}

func TestPathDeletingErrorClassification(t *testing.T) {
	err := pathDeletingErrorf("dj-1", "/a/b")
	assert.True(t, IsPathDeletingError(err))
	assert.True(t, IsPathDeletingError(ErrPathDeleting))
	assert.False(t, IsPathDeletingError(nil))
	assert.False(t, IsPathDeletingError(assertErr("some other failure")))
}

type staticErr string

func (e staticErr) Error() string { return string(e) }

func assertErr(msg string) error { return staticErr(msg) }
