package batchjob

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"
)

// Store is the persistence boundary for jobs, manifests, chunk results and
// leases. filerStore implements it against a SeaweedFS filer; memStore is the
// in-memory twin used by engine tests. Both must satisfy storeContract.
type Store interface {
	// CreateJob persists a brand-new job and its immutable manifest chunks.
	// It fails with ErrConflict if a job with the same id already exists.
	CreateJob(ctx context.Context, job *Job, items []Item) error

	// GetJob loads the descriptor. Returns ErrNotFound when absent.
	GetJob(ctx context.Context, jobID string) (*Job, error)

	// SaveJob persists the descriptor with optimistic concurrency: the call
	// conflicts if another writer bumped the version since the load. On success
	// job.Version is incremented.
	SaveJob(ctx context.Context, job *Job) error

	// ListJobIDs lists all job directory ids (any state), sorted by id.
	ListJobIDs(ctx context.Context) ([]string, error)

	// ReadManifest loads one manifest chunk (chunks are 0-based).
	ReadManifest(ctx context.Context, jobID string, chunk int) ([]Item, error)

	// ReadChunkResult loads the current checkpoint/result for a chunk.
	// Returns ErrNotFound when the chunk has no checkpoint yet.
	ReadChunkResult(ctx context.Context, jobID string, chunk int) (*ChunkResult, error)

	// WriteChunkResult persists a chunk result atomically (temp entry +
	// rename), replacing any previous checkpoint for the chunk.
	WriteChunkResult(ctx context.Context, jobID string, result *ChunkResult) error

	// ListResultChunks lists the chunk indexes that have a result file and
	// reports which are marked complete.
	ListResultChunks(ctx context.Context, jobID string) (chunks []int, complete map[int]bool, err error)

	// AcquireLease creates the job execution lease (fence=1) or steals it
	// when it is released/expired (fence increments). Fails ErrLeaseHeld
	// while another owner holds a live lease.
	AcquireLease(ctx context.Context, jobID, owner string, now time.Time) (fence int64, err error)

	// RenewLease extends the lease while owner and fence still match.
	// Fails ErrLeaseHeld otherwise (lost fencing).
	RenewLease(ctx context.Context, jobID, owner string, fence int64, now time.Time) error

	// ReleaseLease marks the lease released (keeps fencing monotonic).
	ReleaseLease(ctx context.Context, jobID, owner string, fence int64) error

	// DeleteJob removes the whole job directory (retention reaping).
	DeleteJob(ctx context.Context, jobID string) error
}

const (
	extVersion = "bj-version"
	extFence   = "bj-fence"
	extOwner   = "bj-owner"
	extRelease = "bj-released"
)

// Path layout helpers (shared by both store implementations):
//
//	<root>/<jobID>/job.json
//	<root>/<jobID>/lease
//	<root>/<jobID>/manifest/m-00000
//	<root>/<jobID>/results/r-00000
func jobDirPath(root, jobID string) string   { return path.Join(root, jobID) }
func manifestDirPath(root, id string) string { return path.Join(root, id, manifestDir) }
func resultsDirPath(root, id string) string  { return path.Join(root, id, resultsDir) }

func manifestName(chunk int) string {
	return fmt.Sprintf("%s%05d", manifestPrefix, chunk)
}

func resultName(chunk int) string {
	return fmt.Sprintf("%s%05d", resultsPrefix, chunk)
}

func chunkFromName(prefix, name string) (int, bool) {
	if len(name) <= len(prefix) || name[:len(prefix)] != prefix {
		return 0, false
	}
	n, err := strconv.Atoi(name[len(prefix):])
	if err != nil {
		return 0, false
	}
	return n, true
}

func versionExt(v int64) map[string][]byte {
	return map[string][]byte{extVersion: []byte(strconv.FormatInt(v, 10))}
}
