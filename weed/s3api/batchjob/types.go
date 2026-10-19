// Package batchjob implements asynchronous, resumable batch object jobs
// (delete / copy / replace tags) for the S3 gateway.
//
// The package owns the job model, the persistent Store abstraction and the
// execution Engine. It deliberately has no dependency on the s3api package:
// per-item work is performed through the Executor interface, which the
// gateway implements by replaying requests through the real S3 handler
// chain, so single-object operation semantics are never reimplemented here.
package batchjob

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the on-disk format version of job descriptors and chunk
// files. Bump it when persisted shapes change incompatibly.
const SchemaVersion = 1

// Operation selects what a job does with every manifest item.
type Operation string

const (
	// OpDelete deletes each object (DeleteObjects semantics; VersionId honored).
	OpDelete Operation = "Delete"
	// OpPutTags replaces the tag set of each object (PutObjectTagging semantics).
	OpPutTags Operation = "PutTags"
	// OpCopy copies each object into TargetBucket, preserving the key unless
	// the item carries an explicit TargetKey.
	OpCopy Operation = "Copy"
)

// JobState is the terminal/observed state of a job.
type JobState string

const (
	StateRunning   JobState = "Running"
	StatePaused    JobState = "Paused"
	StateCompleted JobState = "Completed"
	StateCanceled  JobState = "Canceled"
)

// IsTerminal reports whether no further state transition is possible.
func (s JobState) IsTerminal() bool {
	return s == StateCompleted || s == StateCanceled
}

// ItemState is the per-item processing state recorded in result chunks.
type ItemState string

const (
	ItemPending      ItemState = "Pending"
	ItemSucceeded    ItemState = "Succeeded"
	ItemFailed       ItemState = "Failed"
	ItemNotProcessed ItemState = "NotProcessed"
)

// Tunable limits and timing constants. They are intentionally centralized:
// validation, engine and storage must all share one source of truth.
const (
	// MaxItems bounds one inline manifest submission.
	MaxItems = 50_000
	// MaxBodySize bounds a submission HTTP body (64 MiB).
	MaxBodySize = 64 * 1024 * 1024
	// ChunkSize is the number of manifest items per persisted chunk. A chunk
	// is the unit of checkpointing, leasing granularity and result export.
	ChunkSize = 100

	DefaultConcurrency = 8
	MaxConcurrency     = 128
	// MaxRate is objects/second; 0 means unlimited.
	MaxRate = 100_000

	MinRetentionSeconds     = 3_600            // 1 hour
	MaxRetentionSeconds     = 90 * 24 * 3_600  // 90 days
	DefaultRetentionSeconds = 7 * 24 * 3_600   // 7 days
	MaxTagPairs             = 10               // mirrors the S3 per-object tag limit
	// GlobalWorkers caps in-flight item executions across all jobs on one gateway.
	GlobalWorkers = 512

	LeaseTTL            = 30 * time.Second
	LeaseHeartbeat      = 5 * time.Second
	TickInterval        = 3 * time.Second
	StoreOpMaxBackoff   = 30 * time.Second
	CheckpointMaxItems  = 50
	CheckpointMaxAge    = 500 * time.Millisecond
	DefaultJobsRoot     = "/etc/s3/batch-jobs"
	defaultFailureLimit = 100
	maxFailureLimit     = 1_000
	defaultJobListLimit = 100
	maxJobListLimit     = 1_000
)

// Sentinel errors shared by Store/Engine/Service.
var (
	ErrNotFound  = errors.New("batchjob: not found")
	ErrConflict  = errors.New("batchjob: concurrent update conflict")
	ErrLeaseHeld = errors.New("batchjob: lease held by another executor")
)

// ValidationError carries an S3-style error code alongside the message so the
// protocol layer can map it onto s3err without importing s3err here.
type ValidationError struct {
	Code    string
	Message string
}

func (e *ValidationError) Error() string { return e.Code + ": " + e.Message }

func validationError(code, format string, args ...any) error {
	return &ValidationError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Creator is the authorization-attributed snapshot of the submitter. The
// executor resolves a live identity by Name first and falls back to this
// snapshot when the identity can no longer be loaded.
type Creator struct {
	Name         string         `json:"name"`
	PrincipalARN string         `json:"principal_arn,omitempty"`
	AccountID    string         `json:"account_id,omitempty"`
	PolicyNames  []string       `json:"policy_names,omitempty"`
	Actions      []string       `json:"actions,omitempty"`
	IsAdmin      bool           `json:"is_admin,omitempty"`
	Claims       map[string]any `json:"claims,omitempty"`
}

// Submission is the protocol-independent input to a job creation.
type Submission struct {
	Bucket           string
	Operation        Operation
	TargetBucket     string
	Tags             map[string]string
	Concurrency      int
	Rate             int
	RetentionSeconds int64
	Items            []Item
	Creator          Creator
}

// Item is one manifest entry. VersionId applies to Delete and PutTags.
// TargetKey applies to Copy and defaults to Key.
type Item struct {
	Index     int    `json:"i"`
	Key       string `json:"k"`
	VersionId string `json:"v,omitempty"`
	TargetKey string `json:"t,omitempty"`
}

// Job is the persisted job descriptor and aggregate counter state.
type Job struct {
	SchemaVersion int         `json:"schema_version"`
	ID            string      `json:"id"`
	Bucket        string      `json:"bucket"`
	Operation     Operation   `json:"op"`
	TargetBucket  string      `json:"target_bucket,omitempty"`
	Tags          TagSet      `json:"tags,omitempty"`
	Concurrency   int         `json:"concurrency"`
	Rate          int         `json:"rate"`
	RetentionSec  int64       `json:"retention_seconds"`
	State         JobState    `json:"state"`
	TotalItems    int         `json:"total"`
	Succeeded     int         `json:"succeeded"`
	Failed        int         `json:"failed"`
	NotProcessed  int         `json:"not_processed"`
	CreatedAt     time.Time   `json:"created_at"`
	StartedAt     time.Time   `json:"started_at,omitempty"`
	FinishedAt    time.Time   `json:"finished_at,omitempty"`
	ExpiresAt     time.Time   `json:"expires_at,omitempty"`
	Creator       Creator     `json:"creator"`
	Version       int64       `json:"-"`
}

// TagSet is a stable JSON encoding of ordered key/value tag pairs. Map
// iteration order is randomized in Go, so persisted/exported tags use an
// explicit order.
type TagSet []Tag

// Tag is one object tag pair.
type Tag struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// Map returns the tags as a lookup map.
func (t TagSet) Map() map[string]string {
	if len(t) == 0 {
		return nil
	}
	m := make(map[string]string, len(t))
	for _, tag := range t {
		m[tag.Key] = tag.Value
	}
	return m
}

// Processed returns the number of items with a terminal execution result
// (success or failure).
func (j *Job) Processed() int { return j.Succeeded + j.Failed }

// ItemResult is the recorded outcome of one item.
type ItemResult struct {
	Index      int       `json:"i"`
	State      ItemState `json:"s"`
	Code       string    `json:"c,omitempty"`
	Message    string    `json:"m,omitempty"`
	TargetKey  string    `json:"t,omitempty"`
	FinishedAt int64     `json:"f,omitempty"` // unix seconds, 0 = unset
}

// ChunkResult is the checkpoint/result file for one manifest chunk.
// Complete flips to true only after every item in the chunk is terminal.
type ChunkResult struct {
	Chunk   int          `json:"chunk"`
	Complete bool        `json:"complete"`
	Results []ItemResult `json:"results"`
}

// Lease is the exclusive execution lease for a job.
type Lease struct {
	JobID     string    `json:"job_id"`
	Owner     string    `json:"owner"`
	Fence     int64     `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Valid reports whether the lease is held by owner at time now.
func (l *Lease) Valid(owner string, now time.Time) bool {
	return l != nil && l.Owner == owner && now.Before(l.ExpiresAt)
}

// ControlOp enumerates job control operations.
type ControlOp string

const (
	ControlPause  ControlOp = "pause"
	ControlResume ControlOp = "resume"
	ControlCancel ControlOp = "cancel"
)

// ValidateTransition applies the job state machine:
//
//	pause:  Running  -> Paused
//	resume: Paused   -> Running
//	cancel: Running|Paused -> Canceled
//
// Everything else is a client conflict.
func ValidateTransition(from JobState, op ControlOp) (JobState, error) {
	if from.IsTerminal() {
		return from, validationError("InvalidJobState", "job is already in terminal state %s", from)
	}
	switch op {
	case ControlPause:
		if from != StateRunning {
			return from, validationError("InvalidJobState", "can only pause a Running job, current state %s", from)
		}
		return StatePaused, nil
	case ControlResume:
		if from != StatePaused {
			return from, validationError("InvalidJobState", "can only resume a Paused job, current state %s", from)
		}
		return StateRunning, nil
	case ControlCancel:
		if from != StateRunning && from != StatePaused {
			return from, validationError("InvalidJobState", "cannot cancel a job in state %s", from)
		}
		return StateCanceled, nil
	default:
		return from, validationError("InvalidRequest", "unknown control operation %q", op)
	}
}

// NewJobID returns a new job identifier: "bj-" + 16 lowercase hex chars.
func NewJobID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not recoverable; keep the signature error-free
		// for callers by falling back to time-based entropy.
		now := uint64(time.Now().UnixNano())
		for i := range b {
			b[i] = byte(now >> (uint(i) * 8))
		}
	}
	return "bj-" + hex.EncodeToString(b[:])
}

// NewExecutorID returns an identifier for one gateway's engine instance.
func NewExecutorID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "s3-" + hex.EncodeToString(b[:])
}

// Normalize validates the submission envelope and fills defaults. Bucket
// existence checks belong to the protocol layer (they need the filer); this
// validates every field that can be checked without it.
func (s *Submission) Normalize() error {
	if s == nil {
		return validationError("InvalidRequest", "empty submission")
	}
	switch s.Operation {
	case OpDelete, OpPutTags, OpCopy:
	default:
		return validationError("InvalidRequest", "unsupported operation %q", s.Operation)
	}
	if s.Bucket == "" {
		return validationError("InvalidRequest", "bucket is required")
	}
	if len(s.Items) == 0 {
		return validationError("MalformedXML", "object list must not be empty")
	}
	if len(s.Items) > MaxItems {
		return validationError("MalformedXML", "object list exceeds the %d items per job limit", MaxItems)
	}
	if s.Concurrency == 0 {
		s.Concurrency = DefaultConcurrency
	}
	if s.Concurrency < 1 || s.Concurrency > MaxConcurrency {
		return validationError("InvalidRequest", "Concurrency must be between 1 and %d", MaxConcurrency)
	}
	if s.Rate < 0 || s.Rate > MaxRate {
		return validationError("InvalidRequest", "Rate must be between 0 and %d", MaxRate)
	}
	if s.RetentionSeconds == 0 {
		s.RetentionSeconds = DefaultRetentionSeconds
	}
	if s.RetentionSeconds < MinRetentionSeconds || s.RetentionSeconds > MaxRetentionSeconds {
		return validationError("InvalidRequest", "ResultRetentionSeconds must be between %d and %d", MinRetentionSeconds, MaxRetentionSeconds)
	}

	switch s.Operation {
	case OpCopy:
		if s.TargetBucket == "" {
			return validationError("InvalidRequest", "TargetBucket is required for Copy jobs")
		}
		if !bucketCharsOK(s.TargetBucket) {
			return validationError("InvalidRequest", "invalid TargetBucket %q", s.TargetBucket)
		}
	case OpPutTags:
		if len(s.Tags) == 0 {
			// An empty tag set is legal per-object semantics (PutObjectTagging
			// with no tags clears them); the protocol layer decides whether to
			// send TagSet, but a missing tag payload entirely is a malformed job.
			return validationError("InvalidTag", "Tagging payload is required for PutTags jobs")
		}
		if len(s.Tags) > MaxTagPairs {
			return validationError("InvalidTag", "at most %d tags are allowed per object", MaxTagPairs)
		}
	}
	if !bucketCharsOK(s.Bucket) {
		return validationError("InvalidRequest", "invalid bucket %q", s.Bucket)
	}

	for i := range s.Items {
		item := &s.Items[i]
		if item.Key == "" {
			return validationError("MalformedXML", "object at index %d has an empty key", i)
		}
		if len(item.Key) > maxKeyLength {
			return validationError("KeyTooLongError", "object key at index %d exceeds %d bytes", i, maxKeyLength)
		}
		if item.VersionId != "" && s.Operation == OpCopy {
			return validationError("InvalidRequest", "VersionId is not supported for Copy jobs (item %q)", item.Key)
		}
		if s.Operation == OpCopy {
			if item.TargetKey != "" && len(item.TargetKey) > maxKeyLength {
				return validationError("KeyTooLongError", "target key for %q exceeds %d bytes", item.Key, maxKeyLength)
			}
		}
		item.Index = i
	}
	return nil
}

// EffectiveTargetKey returns the destination key for a copy item.
func (s *Submission) EffectiveTargetKey(item Item) string {
	if item.TargetKey != "" {
		return item.TargetKey
	}
	return item.Key
}

// ChunkIndexOf maps an item index to its manifest chunk.
func ChunkIndexOf(itemIndex int) int { return itemIndex / ChunkSize }

// ChunkRange returns the [start,end) item indices of a chunk given the total
// item count.
func ChunkRange(total, chunk int) (start, end int) {
	start = chunk * ChunkSize
	if start > total {
		return 0, 0
	}
	end = start + ChunkSize
	if end > total {
		end = total
	}
	return start, end
}

// ChunkCount returns the number of chunks for a manifest of n items.
func ChunkCount(n int) int {
	if n == 0 {
		return 0
	}
	return (n + ChunkSize - 1) / ChunkSize
}

const maxKeyLength = 1024

// bucketCharsOK mirrors the shape check needed at this layer; full S3 bucket
// validation stays in s3_constants at the protocol boundary.
func bucketCharsOK(bucket string) bool {
	if bucket == "" || len(bucket) > 255 {
		return false
	}
	for _, r := range bucket {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return !strings.Contains(bucket, "//")
}
