package weed_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withAsyncDeleteTestConfig(t *testing.T) {
	t.Helper()
	v := util.GetViper()
	settings := map[string]interface{}{
		"enabled":         true,
		"threshold":       2,
		"estimate_limit":  5,
		"batch_size":      1,
		"batch_interval":  "1ms",
		"conflict_policy": "reject",
		"queue_timeout":   "1s",
		"retry_backoff":   "1ms",
		"retry_max":       "2",
		"scan_interval":   "20ms",
		"terminal_ttl":    "1h",
	}
	for k, val := range settings {
		v.Set("filer.options.async_delete."+k, val)
	}
	t.Cleanup(func() {
		for k := range settings {
			v.Set("filer.options.async_delete."+k, nil)
		}
	})
}

func newDeleteJobTestFiler(t *testing.T, store *renameTestStore, start bool) (*FilerServer, *filer.DeleteJobManager) {
	t.Helper()
	withAsyncDeleteTestConfig(t)
	f := newRenameTestFiler(t, store)
	f.Dlm = lock_manager.NewDistributedLockManager("localhost:8888")
	mgr := filer.NewDeleteJobManager(f)
	f.DeleteJobs = mgr
	if start {
		require.NoError(t, mgr.Start(context.Background()))
		t.Cleanup(mgr.Stop)
	}
	fs := &FilerServer{
		filer:          f,
		option:         &FilerOption{},
		entryLockTable: util.NewLockTable[util.FullPath](),
	}
	return fs, mgr
}

func seedRenameEntry(store *renameTestStore, p string, isDir bool) {
	e := &filer.Entry{FullPath: util.FullPath(p)}
	if isDir {
		e.Attr.Mode = os.ModeDir | 0755
	}
	store.entries[p] = e
}

func seedDeleteJobTree(store *renameTestStore, root string, n int) {
	seedRenameEntry(store, root, true)
	for i := 0; i < n; i++ {
		seedRenameEntry(store, root+"/f"+string(rune('a'+i%26))+strconv.Itoa(i), false)
	}
}

func TestGRPCDeleteEntryJobLifecycleAndSyncFallback(t *testing.T) {
	store := newRenameTestStore()
	seedDeleteJobTree(store, "/big", 6)
	seedRenameEntry(store, "/small", true)
	seedRenameEntry(store, "/small/a", false)
	seedRenameEntry(store, "/one", false)
	fs, _ := newDeleteJobTestFiler(t, store, true)
	ctx := context.Background()

	// Large tree: the RPC resolves only after the job completes.
	resp, err := fs.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory: "/", Name: "big", IsRecursive: true, IsDeleteData: true,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Error, "job completion resolves the RPC: %s", resp.Error)
	for p := range store.entries {
		assert.False(t, strings.HasPrefix(p, "/big"), "job should have removed %s", p)
	}

	// Small directory stays on the synchronous path.
	resp, err = fs.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory: "/", Name: "small", IsRecursive: true, IsDeleteData: true,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	_, ok := store.entries["/small"]
	assert.False(t, ok)

	// A single file never becomes a job.
	resp, err = fs.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory: "/", Name: "one", IsRecursive: false, IsDeleteData: true,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Error)
	_, ok = store.entries["/one"]
	assert.False(t, ok)
}

func TestGRPCDeleteEntryIdempotentAttach(t *testing.T) {
	store := newRenameTestStore()
	seedDeleteJobTree(store, "/big", 8)
	fs, mgr := newDeleteJobTestFiler(t, store, false) // no worker: job stays queued
	ctx := context.Background()

	start := func() string {
		job, async, err := fs.filer.MaybeStartDeleteJob(ctx, "/big", filer.DeleteJobOptions{ShouldDeleteChunks: true})
		require.NoError(t, err)
		require.True(t, async)
		return job.JobId
	}
	first := start()
	second := start()
	assert.Equal(t, first, second, "repeated delete attaches to the same job")
	_ = mgr
}

func TestGRPCDeletingPrefixLookupAndWriteGate(t *testing.T) {
	store := newRenameTestStore()
	seedRenameEntry(store, "/big", true)
	seedRenameEntry(store, "/big/a", false)
	seedRenameEntry(store, "/big/sub", true)
	seedRenameEntry(store, "/else", false)
	fs, _ := newDeleteJobTestFiler(t, store, false)
	ctx := context.Background()

	job, async, err := fs.filer.MaybeStartDeleteJob(ctx, "/big", filer.DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	require.True(t, async)

	// Simulate the worker having removed one child already.
	delete(store.entries, "/big/a")

	// Missing child under the gated prefix: distinct from not-found.
	_, lerr := fs.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/big", Name: "a"})
	require.Error(t, lerr)
	assert.True(t, filer.IsPathDeletingError(lerr))

	// A path never under the prefix keeps the plain not-found semantics.
	_, lerr = fs.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "nope"})
	assert.ErrorIs(t, lerr, filer_pb.ErrNotFound)

	// Writes into the prefix are refused with the structured marker.
	cresp, cerr := fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: "/big/sub",
		Entry:     &filer_pb.Entry{Name: "new", Attributes: &filer_pb.FuseAttributes{}},
	})
	require.NoError(t, cerr)
	assert.True(t, filer.IsPathDeletingError(stringErr(cresp.Error)), cresp.Error)

	// The job root is still lookup-able (kept until the job completes).
	rootResp, rerr := fs.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "big"})
	require.NoError(t, rerr)
	assert.Equal(t, []byte(job.JobId), rootResp.Entry.Extended[filer.ExtDeletingJobKey])
}

func TestGRPCKvCancelAndReservedKeyGuard(t *testing.T) {
	store := newRenameTestStore()
	seedDeleteJobTree(store, "/big", 4)
	fs, _ := newDeleteJobTestFiler(t, store, false)
	ctx := context.Background()

	job, async, err := fs.filer.MaybeStartDeleteJob(ctx, "/big", filer.DeleteJobOptions{ShouldDeleteChunks: true})
	require.NoError(t, err)
	require.True(t, async)

	// Raw writes to the job record namespace are refused.
	putResp, perr := fs.KvPut(ctx, &filer_pb.KvPutRequest{
		Key: []byte(filer.KvDeleteJobJobPrefix + job.JobId), Value: []byte("{}"),
	})
	require.NoError(t, perr)
	assert.NotEmpty(t, putResp.Error)

	// The cancel control key performs the cancellation instead of a raw write.
	putResp, perr = fs.KvPut(ctx, &filer_pb.KvPutRequest{
		Key: []byte(filer.KvDeleteJobCancelPrefix + job.JobId), Value: []byte("cancel"),
	})
	require.NoError(t, perr)
	assert.Empty(t, putResp.Error)
	rec, err := fs.filer.DeleteJobs.Get(ctx, job.JobId)
	require.NoError(t, err)
	assert.Equal(t, filer.DeleteJobStateCancelling, rec.State)
}

func TestHTTPDeleteJob202StatusCancelAnd409(t *testing.T) {
	store := newRenameTestStore()
	seedRenameEntry(store, "/big", true)
	seedRenameEntry(store, "/big/a", false)
	seedRenameEntry(store, "/big/b", false)
	seedRenameEntry(store, "/big/c", false)
	seedRenameEntry(store, "/big/d", false)
	fs, _ := newDeleteJobTestFiler(t, store, false) // worker stays stopped

	// Registration: 202 + descriptor + header.
	req := httptest.NewRequest(http.MethodDelete, "/big?recursive=true", nil)
	rr := httptest.NewRecorder()
	fs.DeleteHandler(rr, req)
	require.Equal(t, http.StatusAccepted, rr.Code, rr.Body.String())
	assert.NotEmpty(t, rr.Header().Get(SeaweedfsDeletingJobHeader))
	var view deleteJobView
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &view))
	assert.Equal(t, "queued", view.State)
	assert.Equal(t, "/big", view.Path)
	assert.NotEmpty(t, view.JobId)
	assert.Equal(t, 1, view.BatchSize)

	// Idempotent re-issue returns the same job id.
	req2 := httptest.NewRequest(http.MethodDelete, "/big?recursive=true", nil)
	rr2 := httptest.NewRecorder()
	fs.DeleteHandler(rr2, req2)
	require.Equal(t, http.StatusAccepted, rr2.Code)
	var view2 deleteJobView
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &view2))
	assert.Equal(t, view.JobId, view2.JobId)

	// Status query.
	req3 := httptest.NewRequest(http.MethodGet, "/big?deleteJob="+view.JobId, nil)
	rr3 := httptest.NewRecorder()
	fs.deleteJobStatusHandler(rr3, req3)
	require.Equal(t, http.StatusOK, rr3.Code)

	// Missing child reads as 409 (not 404) while the prefix is gated.
	delete(store.entries, "/big/a")
	req4 := httptest.NewRequest(http.MethodGet, "/big/a", nil)
	rr4 := httptest.NewRecorder()
	fs.GetOrHeadHandler(rr4, req4)
	assert.Equal(t, http.StatusConflict, rr4.Code, rr4.Body.String())
	assert.Equal(t, view.JobId, rr4.Header().Get(SeaweedfsDeletingJobHeader))

	// Cancel (no worker: state settles at cancelling, action accepted).
	req5 := httptest.NewRequest(http.MethodPost, "/big?deleteJob="+view.JobId+"&action=cancel", nil)
	rr5 := httptest.NewRecorder()
	fs.deleteJobActionHandler(rr5, req5)
	require.Equal(t, http.StatusOK, rr5.Code)
	var view5 deleteJobView
	require.NoError(t, json.Unmarshal(rr5.Body.Bytes(), &view5))
	assert.Equal(t, "cancelling", view5.State)

	// Small delete remains 204 synchronous.
	seedRenameEntry(store, "/tiny", true)
	req6 := httptest.NewRequest(http.MethodDelete, "/tiny?recursive=true", nil)
	rr6 := httptest.NewRecorder()
	fs.DeleteHandler(rr6, req6)
	assert.Equal(t, http.StatusNoContent, rr6.Code)
}

func stringErr(s string) error { return &staticStringErr{s: s} }

type staticStringErr struct{ s string }

func (e *staticStringErr) Error() string { return e.s }
