package s3api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func newQuotaTestRequest(method, target, bucket, identity string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	if bucket != "" {
		r = mux.SetURLVars(r, map[string]string{"bucket": bucket})
	}
	if identity != "" {
		r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), identity))
	}
	return r
}

func mustLoadQuotaConfig(t *testing.T, m *TrafficQuotaManager, cfg *TrafficQuotaConfig) {
	t.Helper()
	content, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := m.LoadFromBytes(content); err != nil {
		t.Fatalf("load config: %v", err)
	}
}

func quotaRule(policy string, read, write *TrafficQuotaLimits) *TrafficQuotaRule {
	rule := &TrafficQuotaRule{Enabled: true, Read: read, Write: write}
	if policy != "" {
		rule.Policy = policy
	}
	return rule
}

// serveThroughQuota runs one request through the interceptor with a handler
// that always answers 200, returning the recorder.
func serveThroughQuota(m *TrafficQuotaManager, r *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)
	h(rr, r)
	return rr
}

// With no configuration at all the layer is a no-op: every request runs and
// no SlowDown is ever produced.
func TestTrafficQuotaDisabledByDefault(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()

	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "alice"))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 with no quota configured, got %d", rr.Code)
	}

	// An explicit empty document keeps the layer disabled.
	if err := m.LoadFromBytes(nil); err != nil {
		t.Fatalf("empty config should be accepted: %v", err)
	}
	rr = serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "alice"))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 after empty config, got %d", rr.Code)
	}
}

// Rate quota: exactly requests_per_minute requests pass in a window, the next
// one is rejected immediately with SlowDown and a Retry-After hint.
func TestTrafficQuotaRateReject(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 2}),
		},
	})

	for i := 0; i < 2; i++ {
		if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d", i+1, rr.Code)
		}
	}
	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on 3rd request, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "<Code>SlowDown</Code>") {
		t.Fatalf("expected SlowDown envelope, got %q", rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("rejected response must carry a Retry-After hint")
	}
	if rr.Header().Get("X-Seaweedfs-Quota-Scope") != "bucket" ||
		rr.Header().Get("X-Seaweedfs-Quota-Direction") != "write" ||
		rr.Header().Get("X-Seaweedfs-Quota-Reason") != quotaReasonRate ||
		rr.Header().Get("X-Seaweedfs-Quota-Result") != "rejected" {
		t.Fatalf("missing quota headers: %v", rr.Header())
	}

	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view == nil || view.Write.RejectedRequestsPerMinute != 1 || view.Write.AdmittedTotal != 2 {
		t.Fatalf("unexpected snapshot: %+v", view)
	}
}

// Concurrency quota: a second in-flight request is refused while one is held.
func TestTrafficQuotaConcurrentReject(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{MaxConcurrent: 1}),
		},
	})

	started := make(chan struct{})
	release := make(chan struct{})
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)

	go h(httptest.NewRecorder(), newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	<-started

	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("X-Seaweedfs-Quota-Reason") != quotaReasonConcurrent {
		t.Fatalf("expected 503 reason %s, got %d (%v)", quotaReasonConcurrent, rr.Code, rr.Header())
	}
	close(release)

	// After the holder finishes, the slot is available again.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code == http.StatusOK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected concurrency slot to be released")
}

// Read and write accounting is independent: exhausting one direction leaves
// the other fully available.
func TestTrafficQuotaReadWriteSeparation(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("",
				&TrafficQuotaLimits{RequestsPerMinute: 100},
				&TrafficQuotaLimits{RequestsPerMinute: 1}),
		},
	})

	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusOK {
		t.Fatalf("first write expected 200, got %d", rr.Code)
	}
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("second write expected 503, got %d", rr.Code)
	}
	// GET reads are metered separately and remain unaffected.
	for i := 0; i < 5; i++ {
		if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodGet, "/b/o", "b", "")); rr.Code != http.StatusOK {
			t.Fatalf("read %d expected 200, got %d", i+1, rr.Code)
		}
	}
}

// Bucket isolation: one bucket over quota never consumes another bucket's
// budget.
func TestTrafficQuotaBucketIsolation(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"noisy": quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 1}),
			"calm":  quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 1}),
		},
	})

	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/noisy/o", "noisy", "")); rr.Code != http.StatusOK {
		t.Fatalf("noisy first request expected 200, got %d", rr.Code)
	}
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/noisy/o", "noisy", "")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("noisy second request expected 503, got %d", rr.Code)
	}
	// calm has its own independent rate budget and is admitted normally.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/calm/o", "calm", "")); rr.Code != http.StatusOK {
		t.Fatalf("calm must not be affected by noisy, got %d", rr.Code)
	}
}

// Identity quotas apply regardless of the bucket the identity targets, and
// compose with bucket quotas.
func TestTrafficQuotaIdentityScope(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b1": quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 100}),
		},
		Identities: map[string]*TrafficQuotaRule{
			"alice": quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 1}),
		},
	})

	// alice: one admit, next rejected against the identity scope even though
	// the bucket budget is wide open.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b1/o", "b1", "alice")); rr.Code != http.StatusOK {
		t.Fatalf("alice first request expected 200, got %d", rr.Code)
	}
	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b1/o", "b1", "alice"))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("alice second request expected 503, got %d", rr.Code)
	}
	if rr.Header().Get("X-Seaweedfs-Quota-Scope") != "identity" || rr.Header().Get("X-Seaweedfs-Quota-Reason") != quotaReasonRate {
		t.Fatalf("expected identity/rate headers, got %v", rr.Header())
	}
	// Identity quota follows her to another bucket (no bucket rule there).
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b2/o", "b2", "alice")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("identity quota must apply on unconfigured bucket too, got %d", rr.Code)
	}
	// Anonymous traffic is not subject to an identity quota.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b1/o", "b1", "")); rr.Code != http.StatusOK {
		t.Fatalf("anonymous request expected 200 under bucket budget, got %d", rr.Code)
	}
}

// Byte quotas are metered post-paid from the actual transferred bytes and,
// once spent, block subsequent requests on that direction.
func TestTrafficQuotaBytesPostPaid(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("",
				&TrafficQuotaLimits{BytesPerMinute: 1000},
				&TrafficQuotaLimits{BytesPerMinute: 100}),
		},
	})

	// In-flight traffic starts; bytes are accounted as the upload proceeds.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusOK {
		t.Fatalf("first write expected 200, got %d", rr.Code)
	}
	// The write path fully spends its minute budget.
	m.recordBytes("b", "", trafficDirWrite, 100)

	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("X-Seaweedfs-Quota-Reason") != quotaReasonBytes {
		t.Fatalf("expected 503 reason bytes on write, got %d (%v)", rr.Code, rr.Header())
	}
	// The read byte budget is untouched: reads pass, and read metering does
	// not spend write budget.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodGet, "/b/o", "b", "")); rr.Code != http.StatusOK {
		t.Fatalf("read expected 200 with separate byte budget, got %d", rr.Code)
	}
	m.recordBytes("b", "", trafficDirRead, 1000)
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodGet, "/b/o", "b", "")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("read expected 503 after spending read byte budget, got %d", rr.Code)
	}

	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view.Write.BytesMeteredTotal != 100 || view.Read.BytesMeteredTotal != 1000 {
		t.Fatalf("unexpected byte counters: %+v", view)
	}
}

// Queue policy: an over-limit request waits for a slot instead of failing,
// answers with the wait header and is distinguished from immediate rejects
// in counters.
func TestTrafficQuotaQueueAdmitsAfterWait(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule(trafficQuotaPolicyQueue, nil, &TrafficQuotaLimits{MaxConcurrent: 1}),
		},
	})

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)
	go h(httptest.NewRecorder(), newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	<-started

	status := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rr := httptest.NewRecorder()
		h(rr, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
		status <- rr
	}()

	// Wait until the second request is actually parked in the queue.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if view := m.Snapshot(trafficQuotaScopeBucket, "b"); view != nil && view.Write.QueuedTotal == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view == nil || view.Write.QueuedTotal != 1 || view.Write.RejectedMaxConcurrent != 0 {
		t.Fatalf("request should be queued, not rejected: %+v", view)
	}

	close(release)
	select {
	case rr := <-status:
		if rr.Code != http.StatusOK {
			t.Fatalf("queued request expected 200 after release, got %d", rr.Code)
		}
		if rr.Header().Get("X-Seaweedfs-Quota-Queue-Wait-Ms") == "" {
			t.Fatal("admitted queued request must expose its queue wait")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued request was not admitted after release")
	}
}

// Queue policy with a short timeout: the request waits but fails SlowDown
// with a timeout result once the wait budget expires.
func TestTrafficQuotaQueueTimeout(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	rule := quotaRule(trafficQuotaPolicyQueue, nil, &TrafficQuotaLimits{MaxConcurrent: 1})
	rule.QueueTimeoutMs = 80
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{"b": rule},
	})

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)
	go h(httptest.NewRecorder(), newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	<-started

	rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after queue timeout, got %d", rr.Code)
	}
	if rr.Header().Get("X-Seaweedfs-Quota-Result") != "timeout" ||
		rr.Header().Get("X-Seaweedfs-Quota-Reason") != quotaReasonConcurrent {
		t.Fatalf("expected timeout/concurrent headers, got %v", rr.Header())
	}
	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view == nil || view.Write.QueuedTotal != 1 || view.Write.QueueTimeoutsTotal != 1 || view.Write.RejectedMaxConcurrent != 0 {
		t.Fatalf("timeout must count as queued+timeout, not immediate rejection: %+v", view)
	}
}

// A canceled request context unblocks a queued request as a timeout outcome.
func TestTrafficQuotaQueueContextCancel(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule(trafficQuotaPolicyQueue, nil, &TrafficQuotaLimits{MaxConcurrent: 1}),
		},
	})

	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)
	go h(httptest.NewRecorder(), newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	r := newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")
	r = r.WithContext(ctx)
	done := make(chan struct{})
	go func() {
		serveThroughQuota(m, r)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled queued request should have returned")
	}
}

// Runtime config adjustment takes effect without restart: raising the
// concurrency limit admits more in-flight requests, and counters survive the
// reload.
func TestTrafficQuotaRuntimeAdjust(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{MaxConcurrent: 1}),
		},
	})

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)
	go h(httptest.NewRecorder(), newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
	<-started
	// The first slot is held, so the second request must be rejected.
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("second concurrent request expected 503, got %d", rr.Code)
	}

	// Widen the limit at runtime on the same limiter instance; now a second
	// in-flight request is admitted while the first is still running.
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{MaxConcurrent: 5}),
		},
	})
	admitted := make(chan struct{}, 1)
	go func() {
		serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
		admitted <- struct{}{}
	}()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("second request should be admitted after widening the limit at runtime")
	}

	// Counters carried over the in-place update; the first request is still
	// holding a slot, so in-flight stays 1.
	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view == nil || view.Write.MaxConcurrent != 5 || view.Write.RejectedMaxConcurrent != 1 || view.Write.InFlight != 1 {
		t.Fatalf("runtime update should preserve counters and replace limits: %+v", view)
	}

	close(release)

	// Deleting the rule disables enforcement for the bucket.
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{Enabled: true})
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusOK {
		t.Fatalf("removed rule should disable enforcement, got %d", rr.Code)
	}
	if view := m.Snapshot(trafficQuotaScopeBucket, "b"); view != nil {
		t.Fatalf("removed rule should not appear in snapshots: %+v", view)
	}
}

// Invalid documents are rejected rather than silently misapplied.
func TestTrafficQuotaInvalidConfig(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()

	cases := []string{
		`{"enabled":true,"buckets":{"b":{"enabled":true,"policy":"wait","write":{"max_concurrent":1}}}}`,
		`{"enabled":true,"buckets":{"b":{"enabled":true,"write":{"max_concurrent":-1}}}}`,
		`{"enabled":true,"buckets":{"b":{"enabled":true}}}`,
		`{"enabled":true,"buckets":{"b":{"enabled":true,"write":{}}},"bogus":1}`,
	}
	for i, doc := range cases {
		if err := m.LoadFromBytes([]byte(doc)); err == nil {
			t.Fatalf("case %d expected validation error", i)
		}
	}

	// A failed reload must leave the previously active configuration in place.
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{RequestsPerMinute: 1}),
		},
	})
	if err := m.LoadFromBytes([]byte(cases[0])); err == nil {
		t.Fatal("expected invalid reload to fail")
	}
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusOK {
		t.Fatalf("old config should remain effective after failed reload, got %d", rr.Code)
	}
	if rr := serveThroughQuota(m, newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("old rate limit should still be enforced, got %d", rr.Code)
	}
}

// SnapshotAll enumerates both scopes with live counters; Snapshot on an
// unknown entity reports nil.
func TestTrafficQuotaSnapshot(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", &TrafficQuotaLimits{RequestsPerMinute: 10}, &TrafficQuotaLimits{BytesPerMinute: 2048}),
		},
		Identities: map[string]*TrafficQuotaRule{
			"alice": quotaRule("", nil, &TrafficQuotaLimits{MaxConcurrent: 3}),
		},
	})

	serveThroughQuota(m, newQuotaTestRequest(http.MethodGet, "/b/o", "b", "alice"))

	all := m.SnapshotAll()
	if !all.Enabled || len(all.Buckets) != 1 || len(all.Identities) != 1 {
		t.Fatalf("unexpected snapshot list: %+v", all)
	}
	b := all.Buckets["b"]
	if b.Read.RequestsPerMinute != 10 || b.Write.BytesPerMinute != 2048 || b.Policy != trafficQuotaPolicyReject {
		t.Fatalf("unexpected bucket view: %+v", b)
	}
	if b.Read.AdmittedTotal != 1 || all.Identities["alice"].Write.MaxConcurrent != 3 {
		t.Fatalf("counters not attributed correctly: bucket=%+v identity=%+v", b, all.Identities["alice"])
	}
	if m.Snapshot(trafficQuotaScopeBucket, "missing") != nil {
		t.Fatal("unknown entity should snapshot to nil")
	}
}

// The package-level byte hook (used by BucketTrafficSent/Received) meters the
// active manager.
func TestTrafficQuotaByteHook(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	m.activate()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{BytesPerMinute: 10}),
		},
	})

	r := newQuotaTestRequest(http.MethodPut, "/b/o", "b", "")
	trafficQuotaMeterBytes(r, true, 10)
	trafficQuotaMeterBytes(r, true, 0) // no-op

	view := m.Snapshot(trafficQuotaScopeBucket, "b")
	if view == nil || view.Write.BytesMeteredTotal != 10 {
		t.Fatalf("byte hook should meter writes: %+v", view)
	}
	if rr := serveThroughQuota(m, r); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("spent write byte budget should reject, got %d", rr.Code)
	}
}

// Concurrent admissions never exceed the configured concurrency limit.
func TestTrafficQuotaConcurrentContention(t *testing.T) {
	m := NewTrafficQuotaManager()
	defer m.Close()
	mustLoadQuotaConfig(t, m, &TrafficQuotaConfig{
		Enabled: true,
		Buckets: map[string]*TrafficQuotaRule{
			"b": quotaRule("", nil, &TrafficQuotaLimits{MaxConcurrent: 4}),
		},
	})

	var inFlight, maxObserved int64
	var mu sync.Mutex
	gate := make(chan struct{})
	h := m.intercept(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight > maxObserved {
			maxObserved = inFlight
		}
		mu.Unlock()
		<-gate
		mu.Lock()
		inFlight--
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}, s3_constants.ACTION_WRITE)

	var wg sync.WaitGroup
	var rejected int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := httptest.NewRecorder()
			h(rr, newQuotaTestRequest(http.MethodPut, "/b/o", "b", ""))
			if rr.Code == http.StatusServiceUnavailable {
				// Rejected requests do not enter the gate; count them.
				mu.Lock()
				rejected++
				mu.Unlock()
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(gate)
	wg.Wait()

	if maxObserved > 4 {
		t.Fatalf("concurrency exceeded limit: observed %d", maxObserved)
	}
	if maxObserved != 4 {
		t.Fatalf("expected to fill all 4 slots, observed max %d", maxObserved)
	}
	if rejected == 0 {
		t.Fatal("expected some requests to be rejected under contention")
	}
}
