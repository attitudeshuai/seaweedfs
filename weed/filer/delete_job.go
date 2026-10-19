package filer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Recursive deletion of large directory trees is registered as a durable job
// and advanced in bounded batches by a background worker (see
// delete_job_manager.go / delete_job_runner.go). This file holds the job
// record, the declared configuration, and the store-KV persistence primitives.

const (
	// KvDeleteJobIndex lists every known job id so a restarting filer can load
	// the job set without scanning the KV namespace.
	KvDeleteJobIndex = "filer.deleteJob.v1.index"
	// KvDeleteJobJobPrefix scopes one JSON-encoded DeleteJobRecord per job.
	KvDeleteJobJobPrefix = "filer.deleteJob.v1.job."
	// KvDeleteJobCancelPrefix scopes cancel control requests. Writes to these
	// keys are intercepted by the filer server and never land raw in the store.
	KvDeleteJobCancelPrefix = "filer.deleteJob.v1.cancel."

	// ExtDeletingJobKey marks the root directory entry while its deletion job
	// is active. It is an internal extended attribute: store-visible across
	// restarts and filers, stripped from ordinary HTTP headers, and checked as
	// a second line of write gating. It is removed when the job leaves the
	// active states, and disappears with the entry itself when deletion
	// completes (the root is deleted last).
	ExtDeletingJobKey = "Seaweed-FS-Deleting-Job"
)

// DeleteJobState is the lifecycle state of a recursive-deletion job.
type DeleteJobState string

const (
	DeleteJobStateQueued     DeleteJobState = "queued"
	DeleteJobStateRunning    DeleteJobState = "running"
	DeleteJobStateCancelling DeleteJobState = "cancelling"
	DeleteJobStateCancelled  DeleteJobState = "cancelled"
	DeleteJobStateComplete   DeleteJobState = "complete"
	DeleteJobStateFailed     DeleteJobState = "failed"
)

// Active reports whether the job still owns its prefix. Failed jobs keep the
// prefix gated until retried or cancelled; terminal completion/cancellation
// release it.
func (s DeleteJobState) Active() bool {
	switch s {
	case DeleteJobStateQueued, DeleteJobStateRunning, DeleteJobStateCancelling, DeleteJobStateFailed:
		return true
	}
	return false
}

// Terminal reports whether no worker will advance the job without a new
// explicit action (retry) — complete and cancelled are immutable; failed is
// retryable so it is not terminal here.
func (s DeleteJobState) Terminal() bool {
	return s == DeleteJobStateComplete || s == DeleteJobStateCancelled
}

// Removable reports whether the record may be garbage-collected by the
// terminal-record TTL.
func (s DeleteJobState) Removable() bool {
	return s == DeleteJobStateComplete || s == DeleteJobStateCancelled || s == DeleteJobStateFailed
}

// DeleteJobOptions captures the request-level delete flags a job was registered
// with. IsFromOtherCluster is intentionally absent: peer-replayed deletes
// never become jobs (a replicated job on each peer would loop deletions).
type DeleteJobOptions struct {
	IgnoreRecursiveError bool  `json:"ignoreRecursiveError"`
	ShouldDeleteChunks   bool  `json:"shouldDeleteChunks"`
	IfNotModifiedAfter   int64 `json:"ifNotModifiedAfter"`
	KeepRemoteObject     bool  `json:"keepRemoteObject"`
}

// DeleteJobFrontier is one DFS position: a directory still to empty, with the
// name cursor of the last child page already processed within it.
type DeleteJobFrontier struct {
	// Dir is the directory being emptied (the job root for the bottom entry).
	Dir string `json:"dir"`
	// LastFileName is the last child name processed in Dir; "" means the
	// directory has not been paged yet.
	LastFileName string `json:"lastFileName"`
	// Exhausted marks that the last page of Dir returned no more children;
	// the directory entry itself can be removed once it reaches the stack top
	// (subdirectories pushed while listing sit above it until then).
	Exhausted bool `json:"exhausted,omitempty"`
}

// DeleteJobRecord is the durable state of one recursive-deletion job. It is
// JSON-encoded under KvDeleteJobJobPrefix and mirrored in the manager's memory.
type DeleteJobRecord struct {
	JobId  string          `json:"jobId"`
	Path   string          `json:"path"`
	State  DeleteJobState  `json:"state"`
	Options DeleteJobOptions `json:"options"`

	// Deleted counts entries (files and emptied directories) actually removed.
	Deleted int64 `json:"deleted"`
	// InitialEstimate is the bounded subtree count taken at registration;
	// EstimateTruncated marks it as a lower bound.
	InitialEstimate   int64 `json:"initialEstimate"`
	EstimateTruncated bool  `json:"estimateTruncated"`

	// Frontier is the persistent DFS stack; the root directory is deleted
	// only after it empties.
	Frontier []DeleteJobFrontier `json:"frontier"`

	CreatedAtMs  int64 `json:"createdAt"`
	StartedAtMs  int64 `json:"startedAt,omitempty"`
	FinishedAtMs int64 `json:"finishedAt,omitempty"`

	Error string `json:"error,omitempty"`

	// Owner is the filer host currently executing the job; LeaseUntilMs bounds
	// the claim so a dead owner can be fenced after it expires. Gen bumps on
	// every claim, fencing an old owner that kept running.
	Owner       string `json:"owner,omitempty"`
	LeaseUntilMs int64 `json:"leaseUntil,omitempty"`
	Gen          int    `json:"gen"`

	// RetryCount counts consecutive failures of the same batch; it resets on a
	// successful batch.
	RetryCount int `json:"retryCount"`
}

// RemainingEstimate is the declared remaining figure: a lower bound derived
// from the initial estimate. It never goes below zero.
func (r *DeleteJobRecord) RemainingEstimate() int64 {
	remaining := r.InitialEstimate - r.Deleted
	if remaining < 0 {
		return 0
	}
	return remaining
}

func deleteJobKVKey(jobId string) []byte {
	return []byte(KvDeleteJobJobPrefix + jobId)
}

func deleteJobCancelKVKey(jobId string) []byte {
	return []byte(KvDeleteJobCancelPrefix + jobId)
}

func newDeleteJobId() string {
	return fmt.Sprintf("dj-%d-%08x", time.Now().UnixNano(), uint32(util.RandomInt32()))
}

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

// persistDeleteJob writes the record and, for a previously unknown id, appends
// it to the index. The index is a read-modify-write: callers (the manager)
// serialize job writes, and startup tolerates a missing index entry by
// rebuilding it.
func persistDeleteJob(ctx context.Context, store FilerStore, rec *DeleteJobRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal delete job %s: %w", rec.JobId, err)
	}
	if err := store.KvPut(ctx, deleteJobKVKey(rec.JobId), payload); err != nil {
		return fmt.Errorf("persist delete job %s: %w", rec.JobId, err)
	}
	if ids, _ := loadDeleteJobIndex(ctx, store); !containsString(ids, rec.JobId) {
		ids = append(ids, rec.JobId)
		if err := storeIndexIDs(ctx, store, ids); err != nil {
			return fmt.Errorf("index delete job %s: %w", rec.JobId, err)
		}
	}
	return nil
}

func loadDeleteJobRecord(ctx context.Context, store FilerStore, jobId string) (*DeleteJobRecord, error) {
	payload, err := store.KvGet(ctx, deleteJobKVKey(jobId))
	if err != nil {
		return nil, err
	}
	rec := &DeleteJobRecord{}
	if err := json.Unmarshal(payload, rec); err != nil {
		return nil, fmt.Errorf("unmarshal delete job %s: %w", jobId, err)
	}
	return rec, nil
}

func removeDeleteJob(ctx context.Context, store FilerStore, jobId string) {
	if err := store.KvDelete(ctx, deleteJobKVKey(jobId)); err != nil {
		glog.V(1).InfofCtx(ctx, "delete job %s: remove record: %v", jobId, err)
	}
	ids, err := loadDeleteJobIndex(ctx, store)
	if err != nil {
		return
	}
	next := ids[:0]
	for _, id := range ids {
		if id != jobId {
			next = append(next, id)
		}
	}
	if err := storeIndexIDs(ctx, store, next); err != nil {
		glog.V(1).InfofCtx(ctx, "delete job %s: update index: %v", jobId, err)
	}
}

func storeIndexIDs(ctx context.Context, store FilerStore, ids []string) error {
	if len(ids) == 0 {
		return store.KvDelete(ctx, []byte(KvDeleteJobIndex))
	}
	payload, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return store.KvPut(ctx, []byte(KvDeleteJobIndex), payload)
}

func loadDeleteJobIndex(ctx context.Context, store FilerStore) ([]string, error) {
	payload, err := store.KvGet(ctx, []byte(KvDeleteJobIndex))
	if err == ErrKvNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	if err := json.Unmarshal(payload, &ids); err != nil {
		return nil, fmt.Errorf("unmarshal delete job index: %w", err)
	}
	return ids, nil
}

// loadAllDeleteJobs reads every indexed job record, dropping and rebuilding
// index entries whose records are gone. A record absent from the index but
// present in KV cannot be discovered (stores do not enumerate KV), so every
// persist goes through persistDeleteJob.
func loadAllDeleteJobs(ctx context.Context, store FilerStore) ([]*DeleteJobRecord, error) {
	ids, err := loadDeleteJobIndex(ctx, store)
	if err != nil {
		return nil, err
	}
	var records []*DeleteJobRecord
	var live []string
	for _, id := range ids {
		rec, err := loadDeleteJobRecord(ctx, store, id)
		if err == ErrKvNotFound {
			continue
		}
		if err != nil {
			glog.V(1).InfofCtx(ctx, "load delete job %s: %v", id, err)
			continue
		}
		records = append(records, rec)
		live = append(live, id)
	}
	if len(live) != len(ids) {
		if err := storeIndexIDs(ctx, store, live); err != nil {
			glog.V(1).InfofCtx(ctx, "rebuild delete job index: %v", err)
		}
	}
	return records, nil
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// AsyncDeleteConfig is the declared configuration for job-based recursive
// deletion. All knobs are read from viper under filer.options.async_delete.
type AsyncDeleteConfig struct {
	Enabled        bool
	Threshold      int
	EstimateLimit  int
	BatchSize      int
	BatchInterval  time.Duration
	ConflictPolicy string
	QueueTimeout   time.Duration
	RetryBackoff   time.Duration
	RetryMax       int
	ScanInterval   time.Duration
	TerminalTTL    time.Duration
}

const (
	asyncDeletePrefix = "filer.options.async_delete."

	defaultAsyncDeleteEnabled       = true
	defaultAsyncDeleteThreshold     = 1000
	defaultAsyncDeleteEstimateLimit = 10000
	defaultAsyncDeleteBatchSize     = 1024
	defaultAsyncDeleteRetryMax      = 5
)

// Conflict policies for writes landing under an active deletion prefix.
const (
	AsyncDeletePolicyReject = "reject"
	AsyncDeletePolicyQueue  = "queue"
)

func loadAsyncDeleteConfig() AsyncDeleteConfig {
	v := util.GetViper()
	v.SetDefault(asyncDeletePrefix+"enabled", defaultAsyncDeleteEnabled)
	v.SetDefault(asyncDeletePrefix+"threshold", defaultAsyncDeleteThreshold)
	v.SetDefault(asyncDeletePrefix+"estimate_limit", defaultAsyncDeleteEstimateLimit)
	v.SetDefault(asyncDeletePrefix+"batch_size", defaultAsyncDeleteBatchSize)
	v.SetDefault(asyncDeletePrefix+"batch_interval", "100ms")
	v.SetDefault(asyncDeletePrefix+"conflict_policy", AsyncDeletePolicyReject)
	v.SetDefault(asyncDeletePrefix+"queue_timeout", "30s")
	v.SetDefault(asyncDeletePrefix+"retry_backoff", "5s")
	v.SetDefault(asyncDeletePrefix+"retry_max", defaultAsyncDeleteRetryMax)
	v.SetDefault(asyncDeletePrefix+"scan_interval", "2s")
	v.SetDefault(asyncDeletePrefix+"terminal_ttl", "24h")

	policy := v.GetString(asyncDeletePrefix + "conflict_policy")
	if policy != AsyncDeletePolicyReject && policy != AsyncDeletePolicyQueue {
		glog.Warningf("unknown %sconflict_policy %q, falling back to %q", asyncDeletePrefix, policy, AsyncDeletePolicyReject)
		policy = AsyncDeletePolicyReject
	}

	return AsyncDeleteConfig{
		Enabled:        v.GetBool(asyncDeletePrefix + "enabled"),
		Threshold:      v.GetInt(asyncDeletePrefix + "threshold"),
		EstimateLimit:  v.GetInt(asyncDeletePrefix + "estimate_limit"),
		BatchSize:      v.GetInt(asyncDeletePrefix + "batch_size"),
		BatchInterval:  v.GetDuration(asyncDeletePrefix + "batch_interval"),
		ConflictPolicy: policy,
		QueueTimeout:   v.GetDuration(asyncDeletePrefix + "queue_timeout"),
		RetryBackoff:   v.GetDuration(asyncDeletePrefix + "retry_backoff"),
		RetryMax:       v.GetInt(asyncDeletePrefix + "retry_max"),
		ScanInterval:   v.GetDuration(asyncDeletePrefix + "scan_interval"),
		TerminalTTL:    v.GetDuration(asyncDeletePrefix + "terminal_ttl"),
	}
}

// MsgPathDeletingPrefix leads the error for a write refused under an active
// deletion job. Like MsgFailDelNonEmptyFolder it is never wrapped on the way
// out, so clients can classify it by prefix without parsing, and a client path
// cannot forge one.
const MsgPathDeletingPrefix = "path is being recursively deleted"

// MsgDeleteJobCancelledPrefix leads the gRPC response error when a DeleteEntry
// call waited on a job that was cancelled before completion.
const MsgDeleteJobCancelledPrefix = "recursive delete cancelled"

// ErrPathDeleting is returned when a write/create is refused (or timed out
// while queued) because an active deletion job covers the path.
var ErrPathDeleting = errors.New(MsgPathDeletingPrefix)

// IsPathDeletingError classifies a delete-prefix error that crossed the wire as
// text: the sentinel when it survived, the leading marker when it did not.
func IsPathDeletingError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrPathDeleting) || hasPrefix(err.Error(), MsgPathDeletingPrefix)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// pathDeletingErrorf names the job and prefix so a blocked writer knows what
// to poll. The leading marker stays intact.
func pathDeletingErrorf(jobId, path string) error {
	return PathDeletingJobError(jobId, path)
}

// PathDeletingJobError builds the structured deleting-prefix error for callers
// outside the filer package (HTTP/gRPC handlers). Its leading marker is
// stable and never wrapped, so clients classify it programmatically.
func PathDeletingJobError(jobId, path string) error {
	return fmt.Errorf("%s: job %s owns %s", MsgPathDeletingPrefix, jobId, path)
}
