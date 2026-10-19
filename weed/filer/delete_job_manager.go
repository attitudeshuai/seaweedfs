package filer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// DeleteJobManager owns the in-memory view of recursive-deletion jobs and the
// worker goroutines that advance them. The durable truth is the filer store
// KV (delete_job.go); the manager loads it at startup and refreshes on a
// declared scan interval, so every filer sharing a store gates writes under
// active prefixes while only the ring write owner executes a given job.
type DeleteJobManager struct {
	f    *Filer
	cfg  AsyncDeleteConfig
	host pb.ServerAddress
	now  func() time.Time

	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu      sync.Mutex
	started bool
	jobs    map[string]*managedDeleteJob
	runners map[string]int // jobId -> gen this filer is running; fenced by KV gen

	wakeCh chan struct{}
	wg     sync.WaitGroup
}

type managedDeleteJob struct {
	rec  *DeleteJobRecord
	done chan struct{} // closed when the job leaves the active states
}

// NewDeleteJobManager constructs the manager. It does not touch the store:
// call Start once the store and ring membership are ready.
func NewDeleteJobManager(f *Filer) *DeleteJobManager {
	host := pb.ServerAddress("")
	if f.Dlm != nil {
		host = f.Dlm.Host
	}
	return &DeleteJobManager{
		f:    f,
		cfg:  loadAsyncDeleteConfig(),
		host: host,
		now:  time.Now,
		jobs: make(map[string]*managedDeleteJob),
	}
}

// Start loads the durable job set (rebuilding the write gate synchronously, so
// writes after Start never see an unloaded gate) and runs the scheduler.
func (m *DeleteJobManager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	m.baseCtx, m.baseCancel = context.WithCancel(context.Background())
	m.runners = make(map[string]int)
	m.wakeCh = make(chan struct{}, 1)

	// Load before publishing started: a write that passes this point must see
	// every active prefix.
	if err := m.scanOnce(ctx); err != nil {
		m.baseCancel()
		return err
	}

	m.mu.Lock()
	m.started = true
	m.mu.Unlock()

	m.wg.Add(1)
	go m.loop()
	m.signal()
	return nil
}

// Stop ends the scheduler and every worker. In-progress batches run to their
// natural boundary; records stay durable and another filer (or the next
// process) resumes them.
func (m *DeleteJobManager) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.started = false
	baseCancel := m.baseCancel
	m.mu.Unlock()
	if baseCancel != nil {
		baseCancel()
	}
	m.wg.Wait()
}

func (m *DeleteJobManager) loop() {
	defer m.wg.Done()
	interval := m.cfg.ScanInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.baseCtx.Done():
			return
		case <-ticker.C:
		case <-m.wakeCh:
		}
		if err := m.scanOnce(m.baseCtx); err != nil {
			glog.V(1).Infof("delete job scan: %v", err)
		}
	}
}

func (m *DeleteJobManager) signal() {
	m.mu.Lock()
	ch := m.wakeCh
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// scanOnce refreshes memory from the durable records, expires old terminal
// records, and starts workers for the jobs this filer currently owns.
func (m *DeleteJobManager) scanOnce(ctx context.Context) error {
	records, err := loadAllDeleteJobs(ctx, m.f.Store)
	if err != nil {
		return err
	}

	now := m.now()
	live := make(map[string]*DeleteJobRecord, len(records))
	for _, rec := range records {
		if rec.State.Removable() && rec.FinishedAtMs > 0 &&
			now.Sub(time.UnixMilli(rec.FinishedAtMs)) > m.cfg.TerminalTTL {
			glog.V(1).InfofCtx(ctx, "delete job %s on %s expired after terminal ttl", rec.JobId, rec.Path)
			removeDeleteJob(ctx, m.f.Store, rec.JobId)
			continue
		}
		live[rec.JobId] = rec
	}
	m.dedupeActivePrefixes(ctx, live)

	m.mu.Lock()
	m.mergeLocked(live)
	// Snapshot claims under the lock; the KV writes happen without it.
	type claim struct {
		rec *DeleteJobRecord
		gen int
	}
	var claims []claim
	for _, j := range m.jobs {
		rec := j.rec
		if !rec.State.Active() || rec.State == DeleteJobStateFailed {
			continue
		}
		_, running := m.runners[rec.JobId]
		leaseValid := rec.LeaseUntilMs > now.UnixMilli()
		mine := rec.Owner == string(m.host)
		if running && mine && leaseValid {
			continue
		}
		if rec.Owner != "" && leaseValid && !mine {
			continue
		}
		if !m.isOwnerLocked(rec) {
			continue
		}
		gen := rec.Gen
		if !mine {
			gen++
		}
		claims = append(claims, claim{rec: rec, gen: gen})
	}
	m.mu.Unlock()

	for _, c := range claims {
		m.claimAndRun(ctx, c.rec.JobId, c.gen)
	}
	return nil
}

// dedupeActivePrefixes resolves concurrent registrations of the same prefix
// (possible across filers before either scan observes the other's record):
// the smallest job id wins, the others are finalized as cancelled so only one
// job ever owns a prefix. The winner's marker is (re)stamped on the root.
func (m *DeleteJobManager) dedupeActivePrefixes(ctx context.Context, live map[string]*DeleteJobRecord) {
	winners := make(map[string]string) // prefix -> winning job id
	for _, rec := range live {
		if !rec.State.Active() {
			continue
		}
		winner, seen := winners[rec.Path]
		if !seen || rec.JobId < winner {
			winners[rec.Path] = rec.JobId
		}
	}
	for _, rec := range live {
		if !rec.State.Active() {
			continue
		}
		if winners[rec.Path] == rec.JobId {
			// Re-assert the winner marker in case a losing registration
			// overwrote it concurrently.
			if rootEntry, findErr := m.f.FindEntry(ctx, util.FullPath(rec.Path)); findErr == nil && rootEntry != nil {
				if marked := string(rootEntry.Extended[ExtDeletingJobKey]); marked != rec.JobId {
					_ = m.f.markDeletingRoot(ctx, rootEntry, rec.JobId)
				}
			}
			continue
		}
		glog.V(0).InfofCtx(ctx, "delete job %s on %s superseded by %s; cancelling duplicate", rec.JobId, rec.Path, winners[rec.Path])
		rec.State = DeleteJobStateCancelling
		// Let the owner runner perform the marker cleanup/finalization; when no
		// runner claims it (another owner), finalize directly.
		_ = persistDeleteJob(ctx, m.f.Store, rec)
		if !m.isOwner(rec) {
			m.f.clearDeletingRootMarker(ctx, util.FullPath(rec.Path))
			rec.State = DeleteJobStateCancelled
			rec.FinishedAtMs = millis(m.now())
			_ = persistDeleteJob(ctx, m.f.Store, rec)
		}
	}
}

// claimAndRun writes a fresh lease for the job and starts its worker. The gen
// bump fences a previous owner that may still be finishing a batch.
func (m *DeleteJobManager) claimAndRun(ctx context.Context, jobId string, gen int) {
	rec, err := loadDeleteJobRecord(ctx, m.f.Store, jobId)
	if err != nil {
		glog.V(1).InfofCtx(ctx, "claim delete job %s: load: %v", jobId, err)
		return
	}
	if !rec.State.Active() || rec.State == DeleteJobStateFailed {
		return
	}
	// Re-check ownership against the freshest ring snapshot.
	if rec.Gen > gen {
		return
	}
	if !m.isOwner(rec) {
		return
	}
	if rec.Owner != string(m.host) && rec.LeaseUntilMs > m.now().UnixMilli() {
		return
	}
	rec.Owner = string(m.host)
	rec.LeaseUntilMs = m.now().Add(m.leaseDuration()).UnixMilli()
	rec.Gen = gen
	if err := persistDeleteJob(ctx, m.f.Store, rec); err != nil {
		glog.V(1).InfofCtx(ctx, "claim delete job %s: %v", jobId, err)
		return
	}
	m.mergeOne(rec)
	glog.V(1).InfofCtx(ctx, "delete job %s on %s claimed by %s (gen %d)", jobId, rec.Path, m.host, gen)
	m.startRunner(rec)
}

func (m *DeleteJobManager) leaseDuration() time.Duration {
	d := 3 * m.cfg.ScanInterval
	if d <= 0 {
		return 6 * time.Second
	}
	return d
}

func (m *DeleteJobManager) isOwnerLocked(rec *DeleteJobRecord) bool {
	if m.f.Dlm == nil {
		return true
	}
	owner := m.f.Dlm.LockRing.WriteOwner(s3_constants.ObjectWriteRouteKeyPrefix + rec.Path)
	return owner == "" || owner == m.host
}

func (m *DeleteJobManager) isOwner(rec *DeleteJobRecord) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOwnerLocked(rec)
}

// runJob is one worker goroutine. It reloads the record before every batch
// (so cancellation and fencing take effect), renews its lease, and exits on a
// terminal state, a failed state (retry re-arms it), a gen change, ownership
// loss, or shutdown.
func (m *DeleteJobManager) runJob(jobId string, gen int) {
	defer m.wg.Done()
	// Drop the runner slot when this generation exits so a later claim can
	// start a fresh worker.
	defer func() {
		m.mu.Lock()
		if cur, ok := m.runners[jobId]; ok && cur == gen {
			delete(m.runners, jobId)
		}
		m.mu.Unlock()
	}()
	for {
		if m.baseCtx.Err() != nil {
			return
		}
		rec, err := loadDeleteJobRecord(m.baseCtx, m.f.Store, jobId)
		if err == ErrKvNotFound {
			return
		}
		if err != nil {
			if !m.interruptibleSleep(m.cfg.ScanInterval) {
				return
			}
			continue
		}
		if rec.Gen != gen || !m.isOwner(rec) || !rec.State.Active() || rec.State == DeleteJobStateFailed {
			return
		}
		rec.Owner = string(m.host)
		rec.LeaseUntilMs = m.now().Add(m.leaseDuration()).UnixMilli()

		_, batchErr := m.f.executeDeleteJobBatch(m.baseCtx, rec, m.cfg)
		m.mergeOne(rec)
		if !rec.State.Active() {
			m.signal() // release queued writers promptly
			return
		}
		wait := m.cfg.BatchInterval
		if batchErr != nil {
			wait = m.cfg.RetryBackoff
		}
		if !m.interruptibleSleep(wait) {
			return
		}
	}
}

// interruptibleSleep waits for the delay but wakes early on shutdown or a
// broadcast wake (registration/cancel), returning false when the worker must
// exit.
func (m *DeleteJobManager) interruptibleSleep(d time.Duration) bool {
	if d <= 0 {
		return m.baseCtx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	m.mu.Lock()
	ch := m.wakeCh
	m.mu.Unlock()
	select {
	case <-m.baseCtx.Done():
		return false
	case <-timer.C:
		return true
	case <-ch:
		return true
	}
}

func (m *DeleteJobManager) startRunner(rec *DeleteJobRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runners == nil {
		return
	}
	if gen, ok := m.runners[rec.JobId]; ok && gen == rec.Gen {
		return
	}
	m.runners[rec.JobId] = rec.Gen
	m.wg.Add(1)
	go m.runJob(rec.JobId, rec.Gen)
}

// mergeLocked replaces local state with the durable records and opens/closes
// waiter channels on state transitions.
func (m *DeleteJobManager) mergeLocked(live map[string]*DeleteJobRecord) {
	if m.jobs == nil {
		m.jobs = make(map[string]*managedDeleteJob)
	}
	for id, rec := range live {
		m.mergeOneLocked(rec)
	}
	for id, j := range m.jobs {
		if _, ok := live[id]; !ok {
			if j.rec.State.Active() {
				close(j.done)
			}
			delete(m.jobs, id)
			delete(m.runners, id)
		}
	}
}

func (m *DeleteJobManager) mergeOne(rec *DeleteJobRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mergeOneLocked(rec)
}

// mergeOneLocked updates one job under the lock. Callers must hold m.mu.
func (m *DeleteJobManager) mergeOneLocked(rec *DeleteJobRecord) {
	cp := *rec
	if j, ok := m.jobs[rec.JobId]; ok {
		wasActive := j.rec.State.Active()
		j.rec = &cp
		if wasActive && !rec.State.Active() {
			close(j.done)
		} else if !wasActive && rec.State.Active() {
			j.done = make(chan struct{})
		}
		return
	}
	m.jobs[rec.JobId] = &managedDeleteJob{
		rec:  &cp,
		done: doneChanForState(rec.State),
	}
}

func doneChanForState(state DeleteJobState) chan struct{} {
	ch := make(chan struct{})
	if !state.Active() {
		close(ch)
	}
	return ch
}

// Config returns the manager's declared configuration.
func (m *DeleteJobManager) Config() AsyncDeleteConfig {
	return m.cfg
}

// ActiveJobCovering returns a snapshot of the active job whose prefix covers
// path (longest prefix wins), or nil when no job covers it.
func (m *DeleteJobManager) ActiveJobCovering(path util.FullPath) *DeleteJobRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeJobCoveringLocked(string(path))
}

func (m *DeleteJobManager) activeJobCoveringLocked(p string) *DeleteJobRecord {
	var best *DeleteJobRecord
	for _, j := range m.jobs {
		if !j.rec.State.Active() {
			continue
		}
		if !pathCoveredByPrefix(p, j.rec.Path) {
			continue
		}
		if best == nil || len(j.rec.Path) > len(best.Path) {
			cp := *j.rec
			best = &cp
		}
	}
	return best
}

func pathCoveredByPrefix(p, root string) bool {
	return p == root || strings.HasPrefix(p, root+"/")
}

// CheckWrite enforces the declared conflict policy for a write whose target
// path sits under an active deletion prefix.
func (m *DeleteJobManager) CheckWrite(ctx context.Context, path util.FullPath) error {
	if m == nil || deleteJobBypassed(ctx) {
		return nil
	}
	for {
		rec := m.ActiveJobCovering(path)
		if rec == nil {
			return nil
		}
		if m.cfg.ConflictPolicy != AsyncDeletePolicyQueue {
			return pathDeletingErrorf(rec.JobId, string(path))
		}
		if m.waitForJob(ctx, rec.JobId) {
			// Re-verify on wake: another job may have taken over the prefix
			// while this one was ending.
			continue
		}
		return pathDeletingErrorf(rec.JobId, string(path))
	}
}

// waitForJob blocks until the job leaves the active states, the request
// context ends, or the declared queue timeout elapses. It returns true only
// when the write may proceed.
func (m *DeleteJobManager) waitForJob(ctx context.Context, jobId string) (proceed bool) {
	deadline := m.now().Add(m.cfg.QueueTimeout)
	for {
		m.mu.Lock()
		j := m.jobs[jobId]
		var wait <-chan struct{}
		active := false
		if j != nil {
			wait = j.done
			active = j.rec.State.Active()
			// A failed job is gated but never advances on its own: do not
			// pin the writer for the whole queue timeout.
			if j.rec.State == DeleteJobStateFailed {
				m.mu.Unlock()
				return false
			}
		}
		m.mu.Unlock()
		if j == nil || !active {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-wait:
			return true
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
			return false
		}
	}
}

// RegisterOrAttach returns a job for the root directory: the existing active
// job when one already covers the prefix, otherwise a freshly counted and
// persisted job. When the subtree is at or below the declared threshold it
// returns (nil,false,nil), directing the caller to the synchronous path.
func (m *DeleteJobManager) RegisterOrAttach(ctx context.Context, rootEntry *Entry, opts DeleteJobOptions) (rec *DeleteJobRecord, attached bool, err error) {
	path := string(rootEntry.FullPath)

	m.mu.Lock()
	if existing := m.activeJobCoveringLocked(path); existing != nil {
		m.mu.Unlock()
		if existing.State == DeleteJobStateFailed {
			// A repeated delete request retries the remainder.
			retried, retryErr := m.Retry(ctx, existing.JobId)
			return retried, true, retryErr
		}
		return existing, true, nil
	}
	m.mu.Unlock()

	// One bounded scan decides both the threshold and the estimate.
	limit := m.cfg.Threshold
	if m.cfg.EstimateLimit > limit {
		limit = m.cfg.EstimateLimit
	}
	count, truncated, countErr := m.f.countSubtreeEntriesBounded(ctx, rootEntry.FullPath, limit)
	if countErr != nil {
		return nil, false, countErr
	}
	if !truncated && count <= m.cfg.Threshold {
		return nil, false, nil
	}

	rec = &DeleteJobRecord{
		JobId:             newDeleteJobId(),
		Path:              path,
		State:             DeleteJobStateQueued,
		Options:           opts,
		InitialEstimate:   int64(count),
		EstimateTruncated: truncated,
		CreatedAtMs:       millis(m.now()),
	}
	if truncated {
		rec.InitialEstimate = int64(limit)
	}

	if markErr := m.f.markDeletingRoot(ctx, rootEntry, rec.JobId); markErr != nil {
		return nil, false, markErr
	}
	if persistErr := persistDeleteJob(ctx, m.f.Store, rec); persistErr != nil {
		m.f.clearDeletingRootMarker(ctx, rootEntry.FullPath)
		return nil, false, persistErr
	}

	m.mu.Lock()
	// A concurrent registration (including one from another filer) may own
	// this prefix now: abandon the duplicate and attach.
	if other := m.activeJobCoveringLocked(path); other != nil && other.JobId != rec.JobId {
		m.mu.Unlock()
		removeDeleteJob(ctx, m.f.Store, rec.JobId)
		m.f.markDeletingRoot(ctx, rootEntry, other.JobId)
		return other, true, nil
	}
	m.mergeOneLocked(rec)
	m.mu.Unlock()

	glog.V(0).InfofCtx(ctx, "registered delete job %s for %s (estimate %d, truncated %v)", rec.JobId, path, rec.InitialEstimate, rec.EstimateTruncated)
	m.signal()
	return rec, false, nil
}

// Get returns the durable job descriptor.
func (m *DeleteJobManager) Get(ctx context.Context, jobId string) (*DeleteJobRecord, error) {
	return loadDeleteJobRecord(ctx, m.f.Store, jobId)
}

// Cancel requests cancellation at the next batch boundary. Terminal jobs are
// returned unchanged (idempotent). The owning worker performs the actual
// release (marker removal, state transition); if this filer is not the owner
// the durable state change is enough for the owner to observe.
func (m *DeleteJobManager) Cancel(ctx context.Context, jobId string) (*DeleteJobRecord, error) {
	rec, err := loadDeleteJobRecord(ctx, m.f.Store, jobId)
	if err != nil {
		return nil, err
	}
	if !rec.State.Active() {
		return rec, nil
	}
	rec.State = DeleteJobStateCancelling
	rec.Error = ""
	if err := persistDeleteJob(ctx, m.f.Store, rec); err != nil {
		return nil, err
	}
	glog.V(0).InfofCtx(ctx, "delete job %s on %s cancellation requested", jobId, rec.Path)
	m.mergeOne(rec)
	m.signal()
	return rec, nil
}

// RearmFailed puts a failed job back in the queue; only failed jobs re-arm.
func (m *DeleteJobManager) Retry(ctx context.Context, jobId string) (*DeleteJobRecord, error) {
	rec, err := loadDeleteJobRecord(ctx, m.f.Store, jobId)
	if err != nil {
		return nil, err
	}
	if rec.State != DeleteJobStateFailed {
		return rec, nil
	}
	rootPath := util.FullPath(rec.Path)
	if rootEntry, findErr := m.f.FindEntry(ctx, rootPath); findErr == nil && rootEntry != nil {
		if markErr := m.f.markDeletingRoot(ctx, rootEntry, rec.JobId); markErr != nil {
			return nil, markErr
		}
	}
	resetJobForRetry(rec)
	if err := persistDeleteJob(ctx, m.f.Store, rec); err != nil {
		return nil, err
	}
	m.mergeOne(rec)
	m.signal()
	glog.V(0).InfofCtx(ctx, "delete job %s on %s rearmed for retry", jobId, rec.Path)
	return rec, nil
}

// WaitTerminal blocks until the job reaches an immutable state (complete or
// cancelled) or a failed state, or the context ends, returning the last
// observed record. It wakes immediately on the local job's terminal signal
// and also refreshes the durable record once a second so a state transition
// performed by another filer sharing the store is observed here.
func (m *DeleteJobManager) WaitTerminal(ctx context.Context, jobId string) (*DeleteJobRecord, error) {
	const remoteRefresh = time.Second
	for {
		m.mu.Lock()
		j := m.jobs[jobId]
		var wait <-chan struct{}
		if j != nil {
			wait = j.done
			if j.rec.State.Terminal() || j.rec.State == DeleteJobStateFailed {
				rec := *j.rec
				m.mu.Unlock()
				return &rec, nil
			}
		}
		m.mu.Unlock()

		if j == nil {
			// Not in the local mirror: read durable state so a job owned by
			// another filer still resolves here.
			rec, err := loadDeleteJobRecord(context.WithoutCancel(ctx), m.f.Store, jobId)
			if err != nil {
				return nil, err
			}
			m.mergeOne(rec)
			if rec.State.Terminal() || rec.State == DeleteJobStateFailed {
				return rec, nil
			}
		}

		timer := time.NewTimer(remoteRefresh)
		select {
		case <-ctx.Done():
			timer.Stop()
			return m.Get(context.WithoutCancel(ctx), jobId)
		case <-wait:
			// Left the active states locally; loop reads the snapshot.
		case <-timer.C:
			// Pick up transitions another filer persisted.
			if rec, err := loadDeleteJobRecord(context.WithoutCancel(ctx), m.f.Store, jobId); err == nil {
				m.mergeOne(rec)
			}
		}
	}
}

// ActiveJobSnapshot returns the local mirror of a job without touching the
// store, or nil when the job is not known here.
func (m *DeleteJobManager) ActiveJobSnapshot(jobId string) *DeleteJobRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobId]; ok {
		cp := *j.rec
		return &cp
	}
	return nil
}
