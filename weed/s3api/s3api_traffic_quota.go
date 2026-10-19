package s3api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

// Per-scope (bucket or identity) S3 traffic quotas.
//
// A quota rule declares, independently for the read path and the write path:
//   - requests_per_minute: request rate, enforced with a lazy-refill token
//     bucket (burst capacity equals one minute of allowance),
//   - max_concurrent: number of simultaneously in-flight requests,
//   - bytes_per_minute: transferred bytes, metered post-paid from the
//     gateway's canonical sent/received byte counters (also a token bucket).
//
// Rules are delivered from the config plane as
// /etc/s3/traffic_quota.json on a filer, hot reloaded through the metadata
// event subscription, and account independently per bucket: one bucket
// spending its budget can never consume another bucket's. Identity rules
// apply in addition to bucket rules; a request is admitted only when both
// pass.
//
// Over-limit behavior follows the rule's policy:
//   - reject (default): the request immediately fails with an S3 SlowDown
//     error (HTTP 503) carrying a Retry-After hint;
//   - queue: the request waits, bounded by queue_timeout_ms (falling back to
//     a SlowDown error when the wait budget expires). A request that actually
//     waited answers with an X-Seaweedfs-Quota-Queue-Wait-Ms header.
//
// The two policies are deliberately distinguishable: immediate rejections
// count as traffic_quota_throttled_total{result="rejected"}, queue timeouts
// as result="timeout", and waits surface through the queued counter, the
// queue-wait histogram and the per-request wait header.
const (
	trafficQuotaScopeBucket   = "bucket"
	trafficQuotaScopeIdentity = "identity"

	trafficQuotaPolicyReject = "reject"
	trafficQuotaPolicyQueue  = "queue"

	// quotaReason* double as the prometheus "reason" label values.
	quotaReasonRate       = "requests_per_minute"
	quotaReasonConcurrent = "max_concurrent"
	quotaReasonBytes      = "bytes_per_minute"

	// defaultQueueTimeout bounds queueing when a rule omits queue_timeout_ms.
	defaultQueueTimeout = 5 * time.Second
	// trafficQuotaTickInterval wakes queued requests so that time-based token
	// refill, queue timeouts and client cancellations are picked up promptly.
	trafficQuotaTickInterval = 50 * time.Millisecond
)

// TrafficQuotaConfig is the JSON document stored at
// /etc/s3/traffic_quota.json.
type TrafficQuotaConfig struct {
	// Enabled is the master switch. With it false (or with no file) the
	// request path takes a single atomic-load fast path and behaves exactly
	// as without traffic quotas.
	Enabled    bool                         `json:"enabled"`
	Buckets    map[string]*TrafficQuotaRule `json:"buckets,omitempty"`
	Identities map[string]*TrafficQuotaRule `json:"identities,omitempty"`
}

// TrafficQuotaRule declares quotas for one bucket or one identity. Read/write
// limits are metered independently. Every dimension is optional; 0 means
// unlimited for that dimension.
type TrafficQuotaRule struct {
	Enabled bool `json:"enabled"`
	// Policy is "reject" (default) or "queue".
	Policy string `json:"policy,omitempty"`
	// QueueTimeoutMs bounds how long a request may wait under the queue
	// policy. 0 falls back to defaultQueueTimeout; -1 waits until the client
	// gives up.
	QueueTimeoutMs int64               `json:"queue_timeout_ms,omitempty"`
	Read           *TrafficQuotaLimits `json:"read,omitempty"`
	Write          *TrafficQuotaLimits `json:"write,omitempty"`
}

// TrafficQuotaLimits holds the three quota dimensions for one direction.
type TrafficQuotaLimits struct {
	RequestsPerMinute int64 `json:"requests_per_minute,omitempty"`
	MaxConcurrent     int64 `json:"max_concurrent,omitempty"`
	BytesPerMinute    int64 `json:"bytes_per_minute,omitempty"`
}

func (l *TrafficQuotaLimits) hasAnyLimit() bool {
	return l != nil && (l.RequestsPerMinute > 0 || l.MaxConcurrent > 0 || l.BytesPerMinute > 0)
}

type trafficDirection string

const (
	trafficDirRead  trafficDirection = "read"
	trafficDirWrite trafficDirection = "write"
)

// quotaDirectionByMethod maps HTTP methods to the metered direction.
// GET/HEAD never mutate data; PUT/POST/DELETE/PATCH are writes.
func quotaDirectionByMethod(method string) trafficDirection {
	if method == http.MethodGet || method == http.MethodHead {
		return trafficDirRead
	}
	return trafficDirWrite
}

// trafficTokenBucket is a lazy, time-driven token bucket. All access is
// serialized by the owning limiter's mutex.
type trafficTokenBucket struct {
	capacity     float64 // 0 disables the bucket (unlimited)
	tokens       float64
	refillPerSec float64
	lastRefill   time.Time
}

func (tb *trafficTokenBucket) refill(now time.Time) {
	if tb.capacity <= 0 || tb.refillPerSec <= 0 {
		return
	}
	if tb.lastRefill.IsZero() {
		tb.tokens = tb.capacity
		tb.lastRefill = now
		return
	}
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.refillPerSec)
	tb.lastRefill = now
}

// resetTo reconfigures the bucket for a per-minute allowance. A previously
// disabled bucket starts full; an existing bucket keeps (and refills) its
// balance and clamps to the new capacity so shrinking the quota takes effect.
func (tb *trafficTokenBucket) resetTo(perMinute float64, now time.Time) {
	wasActive := tb.capacity > 0
	tb.capacity = perMinute
	if perMinute <= 0 {
		tb.tokens = 0
		tb.refillPerSec = 0
		tb.lastRefill = time.Time{}
		return
	}
	tb.refillPerSec = perMinute / 60.0
	if !wasActive {
		tb.tokens = perMinute
		tb.lastRefill = now
		return
	}
	tb.refill(now)
	if tb.tokens > perMinute {
		tb.tokens = perMinute
	}
}

// trafficDirectionState keeps one direction's bookkeeping.
type trafficDirectionState struct {
	rate  trafficTokenBucket
	bytes trafficTokenBucket

	maxConcurrent int64
	inFlight      int64

	admitted           int64
	rejectedRate       int64
	rejectedConcurrent int64
	rejectedBytes      int64
	queued             int64
	queueTimeouts      int64
	queueWaitNs        int64
	bytesMetered       int64
}

type trafficQuotaLimiter struct {
	scope string
	name  string

	mu   sync.Mutex
	cond *sync.Cond
	// waiting is the number of requests currently blocked in cond.Wait on
	// this limiter; the manager's janitor only broadcasts while it is nonzero.
	waiting int64

	policy       string
	queueTimeout time.Duration

	read  trafficDirectionState
	write trafficDirectionState
}

func newTrafficQuotaLimiter(scope, name string, rule *TrafficQuotaRule, now time.Time) *trafficQuotaLimiter {
	l := &trafficQuotaLimiter{scope: scope, name: name, policy: trafficQuotaPolicyReject, queueTimeout: defaultQueueTimeout}
	l.cond = sync.NewCond(&l.mu)
	l.applyRule(rule, now)
	return l
}

func (l *trafficQuotaLimiter) state(d trafficDirection) *trafficDirectionState {
	if d == trafficDirRead {
		return &l.read
	}
	return &l.write
}

// applyRule reconfigures a limiter in place so runtime adjustments preserve
// in-flight counts, token balances and rejection counters.
func (l *trafficQuotaLimiter) applyRule(rule *TrafficQuotaRule, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.policy = trafficQuotaPolicyReject
	if rule != nil && rule.Policy == trafficQuotaPolicyQueue {
		l.policy = trafficQuotaPolicyQueue
	}
	l.queueTimeout = defaultQueueTimeout
	if rule != nil {
		switch {
		case rule.QueueTimeoutMs < 0:
			l.queueTimeout = 0 // unbounded, limited only by the request context
		case rule.QueueTimeoutMs > 0:
			l.queueTimeout = time.Duration(rule.QueueTimeoutMs) * time.Millisecond
		}
	}

	var readLimits, writeLimits *TrafficQuotaLimits
	if rule != nil {
		readLimits = rule.Read
		writeLimits = rule.Write
	}
	applyDirectionLimits(&l.read, readLimits, now)
	applyDirectionLimits(&l.write, writeLimits, now)
}

func applyDirectionLimits(st *trafficDirectionState, limits *TrafficQuotaLimits, now time.Time) {
	if limits == nil {
		st.maxConcurrent = 0
		st.rate.resetTo(0, now)
		st.bytes.resetTo(0, now)
		return
	}
	st.maxConcurrent = limits.MaxConcurrent
	st.rate.resetTo(float64(limits.RequestsPerMinute), now)
	st.bytes.resetTo(float64(limits.BytesPerMinute), now)
}

// tryAcquire attempts a non-blocking admission for one direction. It either
// commits against all three dimensions (rate token, concurrency slot, byte
// budget) and returns "", or changes nothing and returns the deficit reason.
func (l *trafficQuotaLimiter) tryAcquire(d trafficDirection) string {
	l.mu.Lock()
	defer l.mu.Unlock()

	st := l.state(d)
	now := time.Now()
	st.rate.refill(now)
	st.bytes.refill(now)

	if st.rate.capacity > 0 && st.rate.tokens < 1 {
		return quotaReasonRate
	}
	if st.maxConcurrent > 0 && st.inFlight >= st.maxConcurrent {
		return quotaReasonConcurrent
	}
	// Transfer bytes are metered post-paid as they actually flow; admission
	// only blocks once the current minute's byte budget is already spent.
	// The concurrency dimension bounds how far one in-flight burst can
	// overshoot.
	if st.bytes.capacity > 0 && st.bytes.tokens <= 0 {
		return quotaReasonBytes
	}

	if st.rate.capacity > 0 {
		st.rate.tokens -= 1
	}
	st.inFlight++
	l.publishGaugesLocked(d, st)
	return ""
}

// release hands back a concurrency slot acquired by tryAcquire. Rate tokens
// are never returned (a served request is a served request).
func (l *trafficQuotaLimiter) release(d trafficDirection) {
	l.mu.Lock()
	st := l.state(d)
	if st.inFlight > 0 {
		st.inFlight--
	}
	l.publishGaugesLocked(d, st)
	waiters := l.waiting
	l.mu.Unlock()
	if waiters > 0 {
		l.cond.Broadcast()
	}
}

// addBytes meters bytes actually transferred on a direction, deducting them
// from the per-minute byte budget of this scope.
func (l *trafficQuotaLimiter) addBytes(d trafficDirection, n int64) {
	if n <= 0 {
		return
	}
	l.mu.Lock()
	st := l.state(d)
	st.bytes.refill(time.Now())
	if st.bytes.capacity > 0 {
		st.bytes.tokens -= float64(n)
	}
	st.bytesMetered += n
	l.publishGaugesLocked(d, st)
	l.mu.Unlock()
}

func (l *trafficQuotaLimiter) markAdmitted(d trafficDirection) {
	l.mu.Lock()
	l.state(d).admitted++
	l.mu.Unlock()
}

// markQueued records one request entering the queue on this limiter.
func (l *trafficQuotaLimiter) markQueued(d trafficDirection) {
	l.mu.Lock()
	l.state(d).queued++
	l.mu.Unlock()
	stats_collect.S3TrafficQuotaQueuedCounter.WithLabelValues(l.scope, l.name, string(d)).Inc()
}

// markQueueWait settles the wait time of a request that queued: observed on
// the wait histogram whether it was eventually admitted or timed out.
func (l *trafficQuotaLimiter) markQueueWait(d trafficDirection, waited time.Duration) {
	l.mu.Lock()
	l.state(d).queueWaitNs += int64(waited)
	l.mu.Unlock()
	stats_collect.S3TrafficQuotaQueueWaitHistogram.WithLabelValues(l.scope, l.name, string(d)).Observe(waited.Seconds())
}

// markRejected records an immediate policy=reject refusal.
func (l *trafficQuotaLimiter) markRejected(d trafficDirection, reason string) {
	l.mu.Lock()
	st := l.state(d)
	switch reason {
	case quotaReasonRate:
		st.rejectedRate++
	case quotaReasonConcurrent:
		st.rejectedConcurrent++
	default:
		st.rejectedBytes++
	}
	l.mu.Unlock()
	stats_collect.S3TrafficQuotaThrottledCounter.WithLabelValues(l.scope, l.name, string(d), reason, "rejected").Inc()
}

// markTimeout records a queued request whose wait budget expired.
func (l *trafficQuotaLimiter) markTimeout(d trafficDirection, reason string) {
	l.mu.Lock()
	l.state(d).queueTimeouts++
	l.mu.Unlock()
	stats_collect.S3TrafficQuotaThrottledCounter.WithLabelValues(l.scope, l.name, string(d), reason, "timeout").Inc()
}

func (l *trafficQuotaLimiter) publishGaugesLocked(d trafficDirection, st *trafficDirectionState) {
	stats_collect.S3TrafficQuotaInFlightGauge.WithLabelValues(l.scope, l.name, string(d)).Set(float64(st.inFlight))
	if st.rate.capacity > 0 {
		stats_collect.S3TrafficQuotaRateTokensGauge.WithLabelValues(l.scope, l.name, string(d)).Set(math.Max(0, st.rate.tokens))
	}
	if st.bytes.capacity > 0 {
		stats_collect.S3TrafficQuotaBytesTokensGauge.WithLabelValues(l.scope, l.name, string(d)).Set(math.Max(0, st.bytes.tokens))
	}
}

func (l *trafficQuotaLimiter) policyMode() string {
	l.mu.Lock()
	p := l.policy
	l.mu.Unlock()
	return p
}

// retryAfterHint suggests how long the client should wait before retrying, in
// whole seconds (at least 1). For a rate deficit it reflects how long the
// slowest configured rate needs to mint one token.
func (l *trafficQuotaLimiter) retryAfterHint(d trafficDirection, reason string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if reason == quotaReasonRate {
		if perSec := l.state(d).rate.refillPerSec; perSec > 0 {
			secs := int(math.Ceil(1 / perSec))
			if secs > 60 {
				secs = 60
			}
			if secs < 1 {
				secs = 1
			}
			return secs
		}
	}
	return 1
}

// quotaVerdict is the admission outcome.
type quotaVerdict struct {
	admitted bool
	waited   time.Duration
	timedOut bool

	scope     string
	name      string
	direction trafficDirection
	reason    string
}

// TrafficQuotaManager owns the per-bucket and per-identity quota limiters.
type TrafficQuotaManager struct {
	enabled atomic.Bool

	mu         sync.RWMutex
	buckets    map[string]*trafficQuotaLimiter
	identities map[string]*trafficQuotaLimiter

	stopOnce sync.Once
	stopCh   chan struct{}
}

// activeTrafficQuota is the package-level hook used by the byte accounting
// points in stats.go. At most one S3 gateway runs per process.
var activeTrafficQuota atomic.Pointer[TrafficQuotaManager]

// NewTrafficQuotaManager creates an empty (disabled) manager and starts its
// queue janitor.
func NewTrafficQuotaManager() *TrafficQuotaManager {
	m := &TrafficQuotaManager{
		buckets:    make(map[string]*trafficQuotaLimiter),
		identities: make(map[string]*trafficQuotaLimiter),
		stopCh:     make(chan struct{}),
	}
	go m.runJanitor()
	return m
}

func (m *TrafficQuotaManager) Close() {
	m.stopOnce.Do(func() { close(m.stopCh) })
}

// activate installs the manager as the process-wide traffic quota hook used
// by the byte accounting points.
func (m *TrafficQuotaManager) activate() {
	activeTrafficQuota.Store(m)
}

func (m *TrafficQuotaManager) get(scope, name string) *trafficQuotaLimiter {
	if name == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if scope == trafficQuotaScopeIdentity {
		return m.identities[name]
	}
	return m.buckets[name]
}

// LoadFromBytes replaces the runtime quota configuration. Existing limiters
// are reused in place so runtime adjustments preserve counters, balances and
// in-flight reservations; removed entities are dropped and their metrics
// deleted. An empty document disables the layer entirely.
func (m *TrafficQuotaManager) LoadFromBytes(content []byte) error {
	cfg := &TrafficQuotaConfig{}
	if len(bytes.TrimSpace(content)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(content))
		dec.DisallowUnknownFields()
		if err := dec.Decode(cfg); err != nil {
			return fmt.Errorf("parse traffic quota config: %w", err)
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			return errors.New("parse traffic quota config: unexpected trailing data after JSON object")
		}
	}
	return m.applyConfig(cfg)
}

func (m *TrafficQuotaManager) applyConfig(cfg *TrafficQuotaConfig) error {
	if cfg == nil {
		cfg = &TrafficQuotaConfig{}
	}
	if err := validateTrafficQuotaConfig(cfg); err != nil {
		return err
	}

	now := time.Now()

	m.mu.Lock()
	oldBuckets := m.buckets
	oldIdentities := m.identities

	newBuckets := make(map[string]*trafficQuotaLimiter, len(cfg.Buckets))
	for name, rule := range cfg.Buckets {
		if rule == nil || !rule.Enabled {
			continue
		}
		if l := oldBuckets[name]; l != nil {
			l.applyRule(rule, now)
			newBuckets[name] = l
		} else {
			newBuckets[name] = newTrafficQuotaLimiter(trafficQuotaScopeBucket, name, rule, now)
		}
	}
	newIdentities := make(map[string]*trafficQuotaLimiter, len(cfg.Identities))
	for name, rule := range cfg.Identities {
		if rule == nil || !rule.Enabled {
			continue
		}
		if l := oldIdentities[name]; l != nil {
			l.applyRule(rule, now)
			newIdentities[name] = l
		} else {
			newIdentities[name] = newTrafficQuotaLimiter(trafficQuotaScopeIdentity, name, rule, now)
		}
	}

	m.buckets = newBuckets
	m.identities = newIdentities
	m.mu.Unlock()

	// Drop prometheus series for entities the new config no longer carries.
	for name, l := range oldBuckets {
		if _, ok := newBuckets[name]; !ok {
			stats_collect.DeleteTrafficQuotaScope(l.scope, name)
		}
	}
	for name, l := range oldIdentities {
		if _, ok := newIdentities[name]; !ok {
			stats_collect.DeleteTrafficQuotaScope(l.scope, name)
		}
	}

	m.enabled.Store(cfg.Enabled && (len(newBuckets)+len(newIdentities) > 0))
	glog.V(1).Infof("traffic quota config applied: enabled=%v buckets=%d identities=%d", cfg.Enabled, len(newBuckets), len(newIdentities))
	return nil
}

func validateTrafficQuotaConfig(cfg *TrafficQuotaConfig) error {
	validate := func(scope string, rules map[string]*TrafficQuotaRule) error {
		for name, rule := range rules {
			if name == "" {
				return fmt.Errorf("%s quota rule has an empty name", scope)
			}
			if rule == nil {
				return fmt.Errorf("%s quota %q is empty", scope, name)
			}
			if rule.Policy != "" && rule.Policy != trafficQuotaPolicyReject && rule.Policy != trafficQuotaPolicyQueue {
				return fmt.Errorf("%s quota %q has invalid policy %q (allowed: reject, queue)", scope, name, rule.Policy)
			}
			if rule.QueueTimeoutMs < -1 {
				return fmt.Errorf("%s quota %q queue_timeout_ms must be >= -1", scope, name)
			}
			if rule.Read != nil {
				if err := validateLimits(scope, name, "read", rule.Read); err != nil {
					return err
				}
			}
			if rule.Write != nil {
				if err := validateLimits(scope, name, "write", rule.Write); err != nil {
					return err
				}
			}
			if rule.Enabled && !rule.Read.hasAnyLimit() && !rule.Write.hasAnyLimit() {
				return fmt.Errorf("%s quota %q is enabled but declares no limit", scope, name)
			}
		}
		return nil
	}
	if err := validate("bucket", cfg.Buckets); err != nil {
		return err
	}
	return validate("identity", cfg.Identities)
}

func validateLimits(scope, name, direction string, l *TrafficQuotaLimits) error {
	if l.RequestsPerMinute < 0 || l.MaxConcurrent < 0 || l.BytesPerMinute < 0 {
		return fmt.Errorf("%s quota %q %s limits must be non-negative", scope, name, direction)
	}
	return nil
}

// loadFromFiler loads the config document from the filer, tolerating its
// absence; errors against every filer only disable the feature.
func (m *TrafficQuotaManager) loadFromFiler(filers []pb.ServerAddress, grpcDialOption grpc.DialOption) {
	err := pb.WithOneOfGrpcFilerClients(false, filers, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(context.Background(), client, s3_constants.CircuitBreakerConfigDir, s3_constants.TrafficQuotaConfigFile)
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read S3 traffic quota config: %w", err)
		}
		return m.LoadFromBytes(content)
	})
	if err != nil {
		glog.Warningf("S3 traffic quota disabled; failed to load config from any filer: %v", err)
	}
}

// intercept is installed as the circuit breaker per-request interceptor, so
// it runs after authentication (identity is known) but ahead of every other
// admission gate (upload limiting, breaker checks, handlers). A queued
// request therefore holds no global slot while waiting, and a rejected
// request never reaches the gateway's shared resources.
func (m *TrafficQuotaManager) intercept(next http.HandlerFunc, _ string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !m.enabled.Load() {
			next(w, r)
			return
		}

		bucket, _ := s3_constants.GetBucketAndObject(r)
		identity := s3_constants.GetIdentityNameFromContext(r)
		direction := quotaDirectionByMethod(r.Method)

		bucketLimiter := m.get(trafficQuotaScopeBucket, bucket)
		identityLimiter := m.get(trafficQuotaScopeIdentity, identity)
		if bucketLimiter == nil && identityLimiter == nil {
			next(w, r)
			return
		}

		// Bucket first, identity second: a single global acquisition order
		// across all requests prevents cross-scope wait deadlocks.
		limiters := make([]*trafficQuotaLimiter, 0, 2)
		if bucketLimiter != nil {
			limiters = append(limiters, bucketLimiter)
		}
		if identityLimiter != nil {
			limiters = append(limiters, identityLimiter)
		}

		verdict, acquired := m.admit(limiters, direction, r.Context())
		if !verdict.admitted {
			m.writeSlowDown(w, r, verdict)
			return
		}
		if verdict.waited > 0 {
			w.Header().Set("X-Seaweedfs-Quota-Queue-Wait-Ms", strconv.FormatInt(verdict.waited.Milliseconds(), 10))
		}
		defer func() {
			for _, l := range acquired {
				l.release(direction)
			}
		}()
		next(w, r)
	}
}

// admit runs the acquire/retry loop across all applicable limiters. On
// success it returns the committed handles; on failure every partial
// acquisition has already been rolled back. Queue bookkeeping is attributed
// to the limiter whose deficit actually caused each wait.
func (m *TrafficQuotaManager) admit(limiters []*trafficQuotaLimiter, direction trafficDirection, ctx context.Context) (quotaVerdict, []*trafficQuotaLimiter) {
	var (
		waitedTotal time.Duration
		deadline    time.Time
		// blamed records limiters already counted as "queued" for this
		// request, so a limiter is blamed at most once even if the deficit
		// oscillates between scopes across retries.
		blamed = make(map[*trafficQuotaLimiter]bool, len(limiters))
		// waitByLimiter accumulates wait time per blamed limiter; the wait
		// histogram is observed once per limiter at the final outcome
		// instead of on every janitor wake.
		waitByLimiter = make(map[*trafficQuotaLimiter]time.Duration, len(limiters))
	)
	flushWaits := func() {
		for l, waited := range waitByLimiter {
			l.markQueueWait(direction, waited)
		}
	}

	for {
		acquired := make([]*trafficQuotaLimiter, 0, len(limiters))
		var deficit *trafficQuotaLimiter
		reason := ""
		for _, l := range limiters {
			if r := l.tryAcquire(direction); r != "" {
				deficit = l
				reason = r
				break
			}
			acquired = append(acquired, l)
		}

		if deficit == nil {
			for _, l := range acquired {
				l.markAdmitted(direction)
			}
			flushWaits()
			return quotaVerdict{admitted: true, waited: waitedTotal}, acquired
		}

		// Roll back anything acquired earlier in this attempt.
		for _, l := range acquired {
			l.release(direction)
		}

		if deficit.policyMode() == trafficQuotaPolicyReject {
			flushWaits()
			deficit.markRejected(direction, reason)
			return quotaVerdict{
				scope:     deficit.scope,
				name:      deficit.name,
				direction: direction,
				reason:    reason,
			}, nil
		}

		// Queue policy: block on the deficit limiter until capacity frees,
		// tokens refill, the deadline expires or the client disconnects.
		if !blamed[deficit] {
			blamed[deficit] = true
			deficit.markQueued(direction)
		}
		if ctx.Err() != nil || m.deadlineExceeded(limiters, &deadline) {
			flushWaits()
			deficit.markTimeout(direction, reason)
			return timeoutVerdict(deficit, direction, reason, waitedTotal), nil
		}

		waitStart := time.Now()
		deficit.waitUntil()
		waited := time.Since(waitStart)
		waitedTotal += waited
		waitByLimiter[deficit] += waited
	}
}

// deadlineExceeded lazily computes the earliest queue deadline across all
// applicable limiters and reports whether it has passed.
func (m *TrafficQuotaManager) deadlineExceeded(limiters []*trafficQuotaLimiter, deadline *time.Time) bool {
	if deadline.IsZero() {
		now := time.Now()
		for _, l := range limiters {
			t := l.queueTimeoutDuration()
			if t > 0 {
				candidate := now.Add(t)
				if deadline.IsZero() || candidate.Before(*deadline) {
					*deadline = candidate
				}
			}
		}
	}
	return !deadline.IsZero() && !time.Now().Before(*deadline)
}

func timeoutVerdict(l *trafficQuotaLimiter, direction trafficDirection, reason string, waited time.Duration) quotaVerdict {
	return quotaVerdict{
		admitted:  false,
		timedOut:  true,
		waited:    waited,
		scope:     l.scope,
		name:      l.name,
		direction: direction,
		reason:    reason,
	}
}

func (l *trafficQuotaLimiter) queueTimeoutDuration() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queueTimeout
}

// waitUntil blocks until the limiter is broadcast (slot release or janitor
// tick). Context cancellation and queue deadlines are evaluated by the caller
// after every wake; the janitor ticks every trafficQuotaTickInterval, so
// time-based wakeups have bounded latency without a timer goroutine per
// waiter.
func (l *trafficQuotaLimiter) waitUntil() {
	l.mu.Lock()
	l.waiting++
	trafficQuotaGlobalWaiters.Add(1)
	l.cond.Wait()
	l.waiting--
	trafficQuotaGlobalWaiters.Add(-1)
	l.mu.Unlock()
}

// trafficQuotaGlobalWaiters aggregates waiters across managers (normally one).
var trafficQuotaGlobalWaiters atomic.Int64

func (m *TrafficQuotaManager) runJanitor() {
	ticker := time.NewTicker(trafficQuotaTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if trafficQuotaGlobalWaiters.Load() == 0 {
				continue
			}
			m.broadcastWaiting()
		}
	}
}

func (m *TrafficQuotaManager) broadcastWaiting() {
	m.mu.RLock()
	ls := make([]*trafficQuotaLimiter, 0, len(m.buckets)+len(m.identities))
	for _, l := range m.buckets {
		ls = append(ls, l)
	}
	for _, l := range m.identities {
		ls = append(ls, l)
	}
	m.mu.RUnlock()
	for _, l := range ls {
		l.mu.Lock()
		waiting := l.waiting
		l.mu.Unlock()
		if waiting > 0 {
			l.cond.Broadcast()
		}
	}
}

// recordBytes meters actually transferred bytes against the bucket and the
// identity scopes independently.
func (m *TrafficQuotaManager) recordBytes(bucket, identity string, direction trafficDirection, n int64) {
	if !m.enabled.Load() || n <= 0 {
		return
	}
	if l := m.get(trafficQuotaScopeBucket, bucket); l != nil {
		l.addBytes(direction, n)
	}
	if l := m.get(trafficQuotaScopeIdentity, identity); l != nil {
		l.addBytes(direction, n)
	}
}

// writeSlowDown answers an over-quota request with the S3 SlowDown envelope,
// a Retry-After hint and quota headers so reject vs. queue-timeout outcomes
// are distinguishable on the wire as well as in metrics.
func (m *TrafficQuotaManager) writeSlowDown(w http.ResponseWriter, r *http.Request, v quotaVerdict) {
	retryAfter := 1
	if l := m.get(v.scope, v.name); l != nil {
		retryAfter = l.retryAfterHint(v.direction, v.reason)
	}
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	w.Header().Set("X-Seaweedfs-Quota-Scope", v.scope)
	w.Header().Set("X-Seaweedfs-Quota-Direction", string(v.direction))
	w.Header().Set("X-Seaweedfs-Quota-Reason", v.reason)
	result := "rejected"
	if v.timedOut {
		result = "timeout"
	}
	w.Header().Set("X-Seaweedfs-Quota-Result", result)

	var message string
	if v.timedOut {
		message = fmt.Sprintf("traffic quota queue timeout for %s %q on %s path: waited %s for %s budget; retry after %d second(s)",
			v.scope, v.name, v.direction, v.waited.Round(time.Millisecond), v.reason, retryAfter)
	} else {
		message = fmt.Sprintf("traffic quota exceeded for %s %q on %s path: %s limit reached; slow down and retry after %d second(s)",
			v.scope, v.name, v.direction, v.reason, retryAfter)
	}
	s3err.WriteErrorResponseWithMessage(w, r, s3err.ErrTooManyRequest, message)
}

// TrafficQuotaDirectionView is the JSON/observable projection of one direction.
type TrafficQuotaDirectionView struct {
	RequestsPerMinute         int64   `json:"requests_per_minute"`
	MaxConcurrent             int64   `json:"max_concurrent"`
	BytesPerMinute            int64   `json:"bytes_per_minute"`
	InFlight                  int64   `json:"in_flight"`
	RateTokensAvailable       float64 `json:"rate_tokens_available"`
	BytesTokensAvailable      float64 `json:"bytes_tokens_available"`
	AdmittedTotal             int64   `json:"admitted_total"`
	RejectedRequestsPerMinute int64   `json:"rejected_requests_per_minute_total"`
	RejectedMaxConcurrent     int64   `json:"rejected_max_concurrent_total"`
	RejectedBytesPerMinute    int64   `json:"rejected_bytes_per_minute_total"`
	QueuedTotal               int64   `json:"queued_total"`
	QueueTimeoutsTotal        int64   `json:"queue_timeouts_total"`
	QueueWaitMsTotal          int64   `json:"queue_wait_ms_total"`
	BytesMeteredTotal         int64   `json:"bytes_metered_total"`
}

// TrafficQuotaView is the observable projection of one quota rule.
type TrafficQuotaView struct {
	Scope          string                    `json:"scope"`
	Name           string                    `json:"name"`
	Policy         string                    `json:"policy"`
	QueueTimeoutMs int64                     `json:"queue_timeout_ms"`
	Read           TrafficQuotaDirectionView `json:"read"`
	Write          TrafficQuotaDirectionView `json:"write"`
}

// TrafficQuotaListResponse reports every active rule with live consumption.
type TrafficQuotaListResponse struct {
	Enabled    bool                        `json:"enabled"`
	Buckets    map[string]TrafficQuotaView `json:"buckets"`
	Identities map[string]TrafficQuotaView `json:"identities"`
}

func (m *TrafficQuotaManager) snapshotLimiter(l *trafficQuotaLimiter) TrafficQuotaView {
	l.mu.Lock()
	defer l.mu.Unlock()
	queueTimeoutMs := l.queueTimeout.Milliseconds()
	if l.queueTimeout == 0 {
		queueTimeoutMs = -1
	}
	view := TrafficQuotaView{
		Scope:          l.scope,
		Name:           l.name,
		Policy:         l.policy,
		QueueTimeoutMs: queueTimeoutMs,
		Read:           snapshotDirection(l, trafficDirRead),
		Write:          snapshotDirection(l, trafficDirWrite),
	}
	return view
}

func snapshotDirection(l *trafficQuotaLimiter, d trafficDirection) TrafficQuotaDirectionView {
	st := l.state(d)
	return TrafficQuotaDirectionView{
		RequestsPerMinute:         int64(st.rate.capacity),
		MaxConcurrent:             st.maxConcurrent,
		BytesPerMinute:            int64(st.bytes.capacity),
		InFlight:                  st.inFlight,
		RateTokensAvailable:       math.Max(0, st.rate.tokens),
		BytesTokensAvailable:      math.Max(0, st.bytes.tokens),
		AdmittedTotal:             st.admitted,
		RejectedRequestsPerMinute: st.rejectedRate,
		RejectedMaxConcurrent:     st.rejectedConcurrent,
		RejectedBytesPerMinute:    st.rejectedBytes,
		QueuedTotal:               st.queued,
		QueueTimeoutsTotal:        st.queueTimeouts,
		QueueWaitMsTotal:          st.queueWaitNs / int64(time.Millisecond),
		BytesMeteredTotal:         st.bytesMetered,
	}
}

// Snapshot returns the live view of one scope, or nil if unconfigured.
func (m *TrafficQuotaManager) Snapshot(scope, name string) *TrafficQuotaView {
	l := m.get(scope, name)
	if l == nil {
		return nil
	}
	view := m.snapshotLimiter(l)
	return &view
}

// SnapshotAll returns the live view of the whole configuration.
func (m *TrafficQuotaManager) SnapshotAll() TrafficQuotaListResponse {
	m.mu.RLock()
	resp := TrafficQuotaListResponse{
		Enabled:    m.enabled.Load(),
		Buckets:    make(map[string]TrafficQuotaView, len(m.buckets)),
		Identities: make(map[string]TrafficQuotaView, len(m.identities)),
	}
	for name, l := range m.buckets {
		resp.Buckets[name] = m.snapshotLimiter(l)
	}
	for name, l := range m.identities {
		resp.Identities[name] = m.snapshotLimiter(l)
	}
	m.mu.RUnlock()
	return resp
}

// trafficQuotaMeterBytes is the post-paid byte accounting hook invoked from
// the gateway's canonical sent/received byte counters in stats.go.
func trafficQuotaMeterBytes(r *http.Request, write bool, n int64) {
	m := activeTrafficQuota.Load()
	if m == nil || n <= 0 {
		return
	}
	bucket, _ := s3_constants.GetBucketAndObject(r)
	identity := s3_constants.GetIdentityNameFromContext(r)
	direction := trafficDirRead
	if write {
		direction = trafficDirWrite
	}
	m.recordBytes(bucket, identity, direction, n)
}
