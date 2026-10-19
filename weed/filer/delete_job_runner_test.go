package filer

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDeleteJobTestFiler(t *testing.T, store FilerStore) *Filer {
	t.Helper()
	logBuffer := log_buffer.NewLogBuffer(
		"test",
		time.Minute,
		func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
		nil,
		func() {},
	)
	t.Cleanup(logBuffer.ShutdownLogBuffer)
	f := &Filer{
		Store:               NewFilerStoreWrapper(store),
		FileIdDeletionQueue: util.NewUnboundedQueue(),
		DeletionRetryQueue:  NewDeletionRetryQueue(),
		deletionQuit:        make(chan struct{}),
		FilerConf:           NewFilerConf(),
		RemoteStorage:       NewFilerRemoteStorage(),
		remoteTombstones:    newRemoteDeletionTombstones(),
		MaxFilenameLength:   255,
		Dlm:                 lock_manager.NewDistributedLockManager("filer-test:8888"),
		LocalMetaLogBuffer:  logBuffer,
	}
	f.DeleteJobs = NewDeleteJobManager(f)
	return f
}

func testJobCfg() AsyncDeleteConfig {
	return AsyncDeleteConfig{
		Enabled:        true,
		Threshold:      1000,
		EstimateLimit:  10000,
		BatchSize:      2,
		BatchInterval:  time.Millisecond,
		ConflictPolicy: AsyncDeletePolicyReject,
		RetryBackoff:   time.Millisecond,
		RetryMax:       2,
		ScanInterval:   20 * time.Millisecond,
		TerminalTTL:    time.Hour,
	}
}

func putJobTreeEntry(t *testing.T, store FilerStore, p string, isDir bool, fids ...string) {
	t.Helper()
	e := &Entry{FullPath: util.FullPath(p)}
	if isDir {
		e.Attr.Mode = os.ModeDir | 0755
	}
	for _, id := range fids {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: id})
	}
	require.NoError(t, store.InsertEntry(context.Background(), e))
}

// buildJobTree: /root/f1, /root/f2(chunk), /root/d1/, /root/d1/f3,
// /root/d1/d2/, /root/d1/d2/f4 — six entries below root.
func buildJobTree(t *testing.T, withChunks bool) FilerStore {
	store := newStubFilerStore()
	putJobTreeEntry(t, store, "/root", true)
	putJobTreeEntry(t, store, "/root/f1", false)
	if withChunks {
		putJobTreeEntry(t, store, "/root/f2", false, "1,02")
	} else {
		putJobTreeEntry(t, store, "/root/f2", false)
	}
	putJobTreeEntry(t, store, "/root/d1", true)
	putJobTreeEntry(t, store, "/root/d1/f3", false)
	putJobTreeEntry(t, store, "/root/d1/d2", true)
	putJobTreeEntry(t, store, "/root/d1/d2/f4", false)
	return store
}

func runBatchesToEnd(t *testing.T, ctx context.Context, f *Filer, rec *DeleteJobRecord, cfg AsyncDeleteConfig) {
	t.Helper()
	for i := 0; i < 200; i++ {
		_, err := f.executeDeleteJobBatch(ctx, rec, cfg)
		if !rec.State.Active() {
			return
		}
		require.NoError(t, err, "batch %d", i)
	}
	t.Fatalf("job %s did not finish in 200 batches, state=%s frontier=%+v", rec.JobId, rec.State, rec.Frontier)
}

func TestExecuteDeleteJobBatchCompletesTreeRootLast(t *testing.T) {
	store := buildJobTree(t, true)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()

	root, err := f.FindEntry(ctx, "/root")
	require.NoError(t, err)
	require.NoError(t, f.markDeletingRoot(ctx, root, "dj-run"))
	rec := &DeleteJobRecord{
		JobId:           "dj-run",
		Path:            "/root",
		State:           DeleteJobStateQueued,
		Options:         DeleteJobOptions{ShouldDeleteChunks: true},
		CreatedAtMs:     millis(time.Now()),
	}

	// Drive one batch at a time: the root must survive while work remains.
	for i := 0; i < 200; i++ {
		_, err := f.executeDeleteJobBatch(ctx, rec, cfg)
		require.NoError(t, err)
		if !rec.State.Active() {
			break
		}
		_, findErr := f.FindEntry(ctx, "/root")
		require.NoError(t, findErr, "root must exist while job active (batch %d)", i)
	}
	require.Equal(t, DeleteJobStateComplete, rec.State)
	assert.Equal(t, int64(6), rec.Deleted)
	assert.NotZero(t, rec.StartedAtMs)
	assert.NotZero(t, rec.FinishedAtMs)
	assert.Empty(t, rec.Frontier)

	// Everything is gone, root included.
	_, err = f.FindEntry(ctx, "/root")
	assert.ErrorIs(t, err, filer_pb.ErrNotFound)
	for _, p := range []string{"/root/f1", "/root/f2", "/root/d1", "/root/d1/f3", "/root/d1/d2", "/root/d1/d2/f4"} {
		_, err := f.FindEntry(ctx, util.FullPath(p))
		assert.ErrorIs(t, err, filer_pb.ErrNotFound, p)
	}

	// The chunk-bearing file queued its fid for deletion.
	var queued []string
	f.FileIdDeletionQueue.Consume(func(ids []string) { queued = append(queued, ids...) })
	assert.Contains(t, queued, "1,02")
}

func TestExecuteDeleteJobBatchCancellationKeepsRemainder(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.BatchSize = 1

	root, err := f.FindEntry(ctx, "/root")
	require.NoError(t, err)
	require.NoError(t, f.markDeletingRoot(ctx, root, "dj-cancel"))
	rec := &DeleteJobRecord{JobId: "dj-cancel", Path: "/root", State: DeleteJobStateQueued,
		Options: DeleteJobOptions{ShouldDeleteChunks: true}, CreatedAtMs: millis(time.Now())}

	// One batch removes one entry.
	_, err = f.executeDeleteJobBatch(ctx, rec, cfg)
	require.NoError(t, err)
	require.Equal(t, DeleteJobStateRunning, rec.State)
	assert.Equal(t, int64(1), rec.Deleted)

	// Cancel at the boundary: next batch finalizes, root and survivors stay.
	rec.State = DeleteJobStateCancelling
	_, err = f.executeDeleteJobBatch(ctx, rec, cfg)
	require.NoError(t, err)
	assert.Equal(t, DeleteJobStateCancelled, rec.State)
	assert.NotZero(t, rec.FinishedAtMs)

	rootAgain, findErr := f.FindEntry(ctx, "/root")
	require.NoError(t, findErr)
	_, marked := rootAgain.Extended[ExtDeletingJobKey]
	assert.False(t, marked, "marker must be cleared on cancel")

	remaining := 0
	for _, p := range []string{"/root/f1", "/root/f2", "/root/d1", "/root/d1/f3", "/root/d1/d2", "/root/d1/d2/f4"} {
		if _, err := f.FindEntry(ctx, util.FullPath(p)); err == nil {
			remaining++
		}
	}
	assert.Equal(t, 5, remaining)
}

func TestExecuteDeleteJobBatchResumesFromDurableRecord(t *testing.T) {
	store := buildJobTree(t, false)
	f1 := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.BatchSize = 1

	root, err := f1.FindEntry(ctx, "/root")
	require.NoError(t, err)
	require.NoError(t, f1.markDeletingRoot(ctx, root, "dj-restart"))
	rec := &DeleteJobRecord{JobId: "dj-restart", Path: "/root", State: DeleteJobStateQueued,
		Options: DeleteJobOptions{ShouldDeleteChunks: true}, CreatedAtMs: millis(time.Now())}
	require.NoError(t, persistDeleteJob(ctx, store, rec))

	// First "process": three batches, then a simulated crash.
	for i := 0; i < 3; i++ {
		_, err := f1.executeDeleteJobBatch(ctx, rec, cfg)
		require.NoError(t, err)
	}
	require.Equal(t, DeleteJobStateRunning, rec.State)
	deletedSoFar := rec.Deleted

	// Restart: a fresh Filer/manager loads the durable record and continues.
	f2 := newDeleteJobTestFiler(t, store)
	loaded, err := loadDeleteJobRecord(ctx, store, "dj-restart")
	require.NoError(t, err)
	assert.Equal(t, deletedSoFar, loaded.Deleted)
	runBatchesToEnd(t, ctx, f2, loaded, cfg)

	require.Equal(t, DeleteJobStateComplete, loaded.State)
	assert.Equal(t, int64(6), loaded.Deleted)
	_, err = f2.FindEntry(ctx, "/root")
	assert.ErrorIs(t, err, filer_pb.ErrNotFound)
}

func TestExecuteDeleteJobBatchRetriesThenFails(t *testing.T) {
	store := buildJobTree(t, false)
	// Every attempt to delete f1 fails permanently.
	store.deleteErrByPath["/root/f1"] = fmt.Errorf("store unavailable")
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.BatchSize = 1
	cfg.RetryMax = 2

	root, err := f.FindEntry(ctx, "/root")
	require.NoError(t, err)
	require.NoError(t, f.markDeletingRoot(ctx, root, "dj-fail"))
	rec := &DeleteJobRecord{JobId: "dj-fail", Path: "/root", State: DeleteJobStateQueued,
		Options: DeleteJobOptions{ShouldDeleteChunks: true}, CreatedAtMs: millis(time.Now())}

	var lastErr error
	for i := 0; i < 6; i++ {
		_, lastErr = f.executeDeleteJobBatch(ctx, rec, cfg)
		if rec.State == DeleteJobStateFailed {
			break
		}
	}
	require.Error(t, lastErr)
	assert.Equal(t, DeleteJobStateFailed, rec.State)
	assert.Equal(t, 3, rec.RetryCount) // the failing attempt + retries, capped past retryMax
	assert.NotEmpty(t, rec.Error)
	// Root is still present and still marked while failed.
	rootAgain, findErr := f.FindEntry(ctx, "/root")
	require.NoError(t, findErr)
	assert.Equal(t, []byte("dj-fail"), rootAgain.Extended[ExtDeletingJobKey])

	// Store recovers: a retry re-arms and the job finishes.
	delete(store.deleteErrByPath, "/root/f1")
	resetJobForRetry(rec)
	require.NoError(t, persistDeleteJob(ctx, store, rec))
	runBatchesToEnd(t, ctx, f, rec, cfg)
	assert.Equal(t, DeleteJobStateComplete, rec.State)
}

func TestExecuteDeleteJobBatchTemporaryFailureRetriesAndSucceeds(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.BatchSize = 1
	cfg.RetryMax = 3

	root, err := f.FindEntry(ctx, "/root")
	require.NoError(t, err)
	require.NoError(t, f.markDeletingRoot(ctx, root, "dj-tmp"))
	rec := &DeleteJobRecord{JobId: "dj-tmp", Path: "/root", State: DeleteJobStateQueued,
		Options: DeleteJobOptions{ShouldDeleteChunks: true}, CreatedAtMs: millis(time.Now())}

	// Fail f1 twice, then recover.
	store.deleteErrByPath["/root/f1"] = fmt.Errorf("transient")
	_, berr := f.executeDeleteJobBatch(ctx, rec, cfg)
	require.Error(t, berr)
	assert.Equal(t, 1, rec.RetryCount)
	_, berr = f.executeDeleteJobBatch(ctx, rec, cfg)
	require.Error(t, berr)
	assert.Equal(t, 2, rec.RetryCount)
	delete(store.deleteErrByPath, "/root/f1")

	runBatchesToEnd(t, ctx, f, rec, cfg)
	assert.Equal(t, DeleteJobStateComplete, rec.State)
	assert.Equal(t, 0, rec.RetryCount, "successful batches reset the failure run")
}

func newManagerFor(t *testing.T, f *Filer, cfg AsyncDeleteConfig) *DeleteJobManager {
	t.Helper()
	m := NewDeleteJobManager(f)
	m.cfg = cfg
	// Wire the replacement manager so Filer-level dispatch and the write gate
	// observe the test configuration and scheduler.
	f.DeleteJobs = m
	return m
}

func TestRegisterOrAttachThresholdAndIdempotency(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.Threshold = 3
	cfg.EstimateLimit = 10
	m := newManagerFor(t, f, cfg)

	root, err := f.FindEntry(ctx, "/root")
	require.NoError(t, err)
	rec, async, err := m.RegisterOrAttach(ctx, root, DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	require.True(t, async)
	require.NotNil(t, rec)
	assert.Equal(t, DeleteJobStateQueued, rec.State)
	assert.Equal(t, int64(6), rec.InitialEstimate)
	assert.False(t, rec.EstimateTruncated)

	// Repeat attaches to the same job with a continuous counter.
	root2, _ := f.FindEntry(ctx, "/root")
	again, attached, err := m.RegisterOrAttach(ctx, root2, DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	assert.True(t, attached)
	assert.Equal(t, rec.JobId, again.JobId)

	// A small tree stays synchronous.
	putJobTreeEntry(t, store, "/small", true)
	putJobTreeEntry(t, store, "/small/a", false)
	small, _ := f.FindEntry(ctx, "/small")
	rec2, async2, err := m.RegisterOrAttach(ctx, small, DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	assert.False(t, async2)
	assert.Nil(t, rec2)
}

func TestRegisterEstimateTruncated(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.Threshold = 2
	cfg.EstimateLimit = 4
	m := newManagerFor(t, f, cfg)
	root, _ := f.FindEntry(ctx, "/root")
	rec, async, err := m.RegisterOrAttach(ctx, root, DeleteJobOptions{})
	require.NoError(t, err)
	require.True(t, async)
	assert.True(t, rec.EstimateTruncated)
	assert.Equal(t, int64(4), rec.InitialEstimate)
	assert.Equal(t, int64(4), rec.RemainingEstimate())
}

func TestCheckWriteRejectQueueAndBypass(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())

	// reject policy
	cfg := testJobCfg()
	cfg.Threshold = 2
	m := newManagerFor(t, f, cfg)
	root, _ := f.FindEntry(ctx, "/root")
	rec, async, err := m.RegisterOrAttach(ctx, root, DeleteJobOptions{})
	require.NoError(t, err)
	require.True(t, async)

	assert.True(t, IsPathDeletingError(m.CheckWrite(ctx, "/root/a/b")))
	assert.NotNil(t, m.ActiveJobCovering("/root"))
	assert.Nil(t, m.ActiveJobCovering("/other"))
	// The worker bypasses the gate.
	assert.NoError(t, m.CheckWrite(WithDeleteJobBypass(ctx), "/root/a"))

	// queue policy: blocks while active, proceeds once the job finalizes.
	m.cfg.ConflictPolicy = AsyncDeletePolicyQueue
	m.cfg.QueueTimeout = 3 * time.Second
	done := make(chan error, 1)
	go func() { done <- m.CheckWrite(ctx, "/root/new") }()
	select {
	case <-done:
		t.Fatal("queued write returned before the job finished")
	case <-time.After(80 * time.Millisecond):
	}
	// Cancel via the manager, then let a batch finalize it.
	_, err = m.Cancel(ctx, rec.JobId)
	require.NoError(t, err)
	loaded, err := loadDeleteJobRecord(ctx, store, rec.JobId)
	require.NoError(t, err)
	_, err = f.executeDeleteJobBatch(ctx, loaded, cfg)
	require.NoError(t, err)
	m.mergeOne(loaded)
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("queued write did not release after cancellation")
	}

	// queue timeout converts to the deleting error.
	m2store := buildJobTree(t, false)
	f2 := newDeleteJobTestFiler(t, m2store)
	cfg2 := testJobCfg()
	cfg2.Threshold = 2
	cfg2.ConflictPolicy = AsyncDeletePolicyQueue
	cfg2.QueueTimeout = 20 * time.Millisecond
	m2 := newManagerFor(t, f2, cfg2)
	root2, _ := f2.FindEntry(ctx, "/root")
	_, _, err = m2.RegisterOrAttach(ctx, root2, DeleteJobOptions{})
	require.NoError(t, err)
	assert.True(t, IsPathDeletingError(m2.CheckWrite(ctx, "/root/x")))
}

func TestManagerStartRunsJobToCompletion(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.Threshold = 2
	m := newManagerFor(t, f, cfg)
	require.NoError(t, m.Start(ctx))
	defer m.Stop()

	rec, async, err := f.MaybeStartDeleteJob(ctx, "/root", DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	require.True(t, async)

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	final, err := m.WaitTerminal(waitCtx, rec.JobId)
	require.NoError(t, err)
	assert.Equal(t, DeleteJobStateComplete, final.State)
	_, findErr := f.FindEntry(ctx, "/root")
	assert.ErrorIs(t, findErr, filer_pb.ErrNotFound)
}

func TestManagerRestartResumesRegisteredJob(t *testing.T) {
	store := buildJobTree(t, false)
	f1 := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())

	// Register on a manager that never starts: durable state only.
	cfg1 := testJobCfg()
	cfg1.Threshold = 2
	m1 := newManagerFor(t, f1, cfg1)
	root, _ := f1.FindEntry(ctx, "/root")
	rec, async, err := m1.RegisterOrAttach(ctx, root, DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	require.True(t, async)

	// A fresh process loads and completes it.
	f2 := newDeleteJobTestFiler(t, store)
	cfg2 := testJobCfg()
	cfg2.Threshold = 2
	m2 := newManagerFor(t, f2, cfg2)
	require.NoError(t, m2.Start(ctx))
	defer m2.Stop()

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	final, err := m2.WaitTerminal(waitCtx, rec.JobId)
	require.NoError(t, err)
	assert.Equal(t, DeleteJobStateComplete, final.State)

	// The restarted filer's gate observed the job while it ran and clears now.
	assert.Nil(t, m2.ActiveJobCovering("/root/anything"))
}

func TestOwnerOnlyClaimsRingWriteOwner(t *testing.T) {
	store := buildJobTree(t, false)
	f := newDeleteJobTestFiler(t, store)
	ctx := WithSuppressedMetadataEvents(context.Background())
	cfg := testJobCfg()
	cfg.Threshold = 2
	m := newManagerFor(t, f, cfg)

	// Ring owned by someone else: the registered job is gated but not claimed.
	f.Dlm.LockRing = lock_manager.NewLockRing(5 * time.Second)
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer-other:8888"}, 0)
	root, _ := f.FindEntry(ctx, "/root")
	rec, async, err := m.RegisterOrAttach(ctx, root, DeleteJobOptions{})
	require.NoError(t, err)
	require.True(t, async)
	require.NoError(t, m.scanOnce(ctx))
	loaded, err := loadDeleteJobRecord(ctx, store, rec.JobId)
	require.NoError(t, err)
	assert.Empty(t, loaded.Owner)

	// Ownership moves to this filer: scan claims it.
	f.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer-test:8888"}, 1)
	require.NoError(t, m.scanOnce(ctx))
	loaded, err = loadDeleteJobRecord(ctx, store, rec.JobId)
	require.NoError(t, err)
	assert.Equal(t, "filer-test:8888", loaded.Owner)
	assert.Equal(t, 1, loaded.Gen)
}
