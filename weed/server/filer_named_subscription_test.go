package weed_server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// ---- in-memory KV for manager-level tests ----

type memCursorKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemCursorKV() *memCursorKV {
	return &memCursorKV{m: make(map[string][]byte)}
}

func (k *memCursorKV) KvPut(_ context.Context, key, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[string(key)] = append([]byte(nil), value...)
	return nil
}

func (k *memCursorKV) KvGet(_ context.Context, key []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[string(key)]
	if !ok {
		return nil, filer.ErrKvNotFound
	}
	return append([]byte(nil), v...), nil
}

func (k *memCursorKV) KvDelete(_ context.Context, key []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, string(key))
	return nil
}

func fastCursorTiming(t *testing.T) {
	t.Helper()
	lease, poll, settle, scan, grace := cursorLeaseTTL, cursorFencePoll, cursorDrainSettle, cursorReaperScan, cursorDefaultTakeoverGrace
	cursorLeaseTTL = time.Second
	cursorFencePoll = 10 * time.Millisecond
	cursorDrainSettle = 5 * time.Millisecond
	cursorReaperScan = 15 * time.Millisecond
	cursorDefaultTakeoverGrace = 2 * time.Second
	t.Cleanup(func() {
		cursorLeaseTTL, cursorFencePoll, cursorDrainSettle, cursorReaperScan, cursorDefaultTakeoverGrace = lease, poll, settle, scan, grace
	})
}

func newTestCursorManager(t *testing.T, host string, kv cursorKV) *namedCursorManager {
	m := newNamedCursorManager(pb.ServerAddress(host), func() cursorKV { return kv })
	m.Start()
	t.Cleanup(m.Stop)
	return m
}

func cursorReq(name string, since int64, mutate func(*filer_pb.SubscribeMetadataRequest)) *filer_pb.SubscribeMetadataRequest {
	req := &filer_pb.SubscribeMetadataRequest{
		ClientName:       "cursor-test",
		SubscriptionName: name,
		SinceNs:          since,
	}
	if mutate != nil {
		mutate(req)
	}
	return req
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestCursorNameValidation(t *testing.T) {
	valid := []string{"a", "sync-job_01.v2", "a.leading-dot-ok"}
	for _, name := range valid {
		if err := validateCursorName(name); err != nil {
			t.Fatalf("%q should be valid: %v", name, err)
		}
	}
	invalid := []string{"", "bad/name", "bad name", "中国", "-bad-start", ".leading-dot"}
	for _, name := range invalid {
		if err := validateCursorName(name); err == nil {
			t.Fatalf("%q should be rejected", name)
		}
	}
	longName := ""
	for i := 0; i < namedCursorMaxNameLength+1; i++ {
		longName += "a"
	}
	if err := validateCursorName(longName); err == nil {
		t.Fatal("overlong name should be rejected")
	}
}

// TestCursorPersistenceAcrossManagerInstances: a second manager (another filer
// process / machine) sharing the store resumes exactly at the confirmed
// position the first one left.
func TestCursorPersistenceAcrossManagerInstances(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()

	m1 := newTestCursorManager(t, "filer-a:8888", kv)
	a1, start, err := m1.begin(ctx, cursorReq("jobs-sync", 100, nil), "10.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	if start != 100 {
		t.Fatalf("first registration start = %d, want 100", start)
	}
	// Simulate delivered and confirmed events, then a clean disconnect.
	m1.markDelivered(a1, &filer_pb.SubscribeMetadataResponse{
		TsNs: 150, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "x"}},
	})
	if c, err := m1.ack(ctx, "jobs-sync", []int64{150}); err != nil || c != 150 {
		t.Fatalf("ack = %d, %v, want 150", c, err)
	}
	m1.release(ctx, a1)

	m2 := newTestCursorManager(t, "filer-b:8888", kv)
	a2, start, err := m2.begin(ctx, cursorReq("jobs-sync", 100, nil), "10.0.0.2:9")
	if err != nil {
		t.Fatal(err)
	}
	defer m2.release(ctx, a2)
	if start != 150 {
		t.Fatalf("resume on another instance = %d, want confirmed 150", start)
	}
}

// TestCursorAckNeverMovesBack: a stale ack below confirmed is rejected and the
// position stays put.
func TestCursorAckNeverMovesBack(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m := newTestCursorManager(t, "filer-a:8888", kv)

	a, _, err := m.begin(ctx, cursorReq("w", 0, nil), "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.release(ctx, a)
	if _, err := m.ack(ctx, "w", []int64{200}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ack(ctx, "w", []int64{199}); err == nil {
		t.Fatal("ack behind confirmed must be rejected")
	}
	// Equal is an idempotent no-op.
	if c, err := m.ack(ctx, "w", []int64{200}); err != nil || c != 200 {
		t.Fatalf("equal ack = %d, %v, want 200", c, err)
	}
	st, err := m.loadStateLocked(ctx, "w")
	if err != nil || st.ConfirmedTsNs != 200 {
		t.Fatalf("confirmed moved: %+v err=%v", st, err)
	}
}

// TestCursorImmediateTakeoverAcrossInstances: a new instance takes the name at
// once; the old owner fences itself off its persisted record.
func TestCursorImmediateTakeoverAcrossInstances(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m1 := newTestCursorManager(t, "filer-a:8888", kv)
	m2 := newTestCursorManager(t, "filer-b:8888", kv)

	a1, start, err := m1.begin(ctx, cursorReq("failover", 100, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionTakeoverPolicy = filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DISCONNECT_OLD
	}), "c1")
	if err != nil || start != 100 {
		t.Fatalf("begin old: start=%d err=%v", start, err)
	}

	// New instance on another filer takes immediately; its begin must not block.
	done := make(chan struct{})
	var a2 *activeNamedStream
	go func() {
		a2, _, err = m2.begin(ctx, cursorReq("failover", 0, nil), "c2")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("immediate takeover begin blocked")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer m2.release(ctx, a2)

	waitFor(t, time.Second, a1.isLost, "old stream fenced after immediate takeover")
}

// TestCursorDrainTakeoverAcrossInstances: with DRAIN the old stream stays
// owner until the client acks what it already holds; the new instance starts
// exactly at that confirmed position.
func TestCursorDrainTakeoverAcrossInstances(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m1 := newTestCursorManager(t, "filer-a:8888", kv)
	m2 := newTestCursorManager(t, "filer-b:8888", kv)

	a1, _, err := m1.begin(ctx, cursorReq("drain", 100, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionTakeoverPolicy = filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DRAIN_OLD
		r.SubscriptionTakeoverGraceNs = int64(5 * time.Second)
	}), "c1")
	if err != nil {
		t.Fatal(err)
	}
	for _, ts := range []int64{110, 120, 130} {
		m1.markDelivered(a1, &filer_pb.SubscribeMetadataResponse{
			TsNs: ts, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: fmt.Sprintf("f%d", ts)}},
		})
	}

	beginDone := make(chan struct{})
	var a2 *activeNamedStream
	var start2 int64
	go func() {
		a2, start2, err = m2.begin(ctx, cursorReq("drain", 0, nil), "c2")
		close(beginDone)
	}()

	// The new instance must wait while the old stream has unacked events.
	select {
	case <-beginDone:
		t.Fatal("new instance started before the old stream drained")
	case <-time.After(100 * time.Millisecond):
	}
	waitFor(t, time.Second, a1.isDraining, "old stream entering drain")

	// The old consumer finishes its in-flight events and confirms them.
	if c, err := m1.ack(ctx, "drain", []int64{130}); err != nil || c != 130 {
		t.Fatalf("drain ack = %d, %v", c, err)
	}

	select {
	case <-beginDone:
	case <-time.After(2 * time.Second):
		t.Fatal("new instance did not start after drain ack")
	}
	defer m2.release(ctx, a2)
	if start2 != 130 {
		t.Fatalf("new instance resumes at %d, want 130", start2)
	}
}

// TestCursorDrainGraceExpires: an unresponsive old consumer is taken over once
// its grace runs out even without acks.
func TestCursorDrainGraceExpires(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m1 := newTestCursorManager(t, "filer-a:8888", kv)
	m2 := newTestCursorManager(t, "filer-b:8888", kv)

	a1, _, err := m1.begin(ctx, cursorReq("grace", 100, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionTakeoverPolicy = filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DRAIN_OLD
		r.SubscriptionTakeoverGraceNs = int64(200 * time.Millisecond)
	}), "c1")
	if err != nil {
		t.Fatal(err)
	}
	m1.markDelivered(a1, &filer_pb.SubscribeMetadataResponse{
		TsNs: 140, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "x"}},
	})

	beginDone := make(chan struct{})
	go func() {
		_, _, err = m2.begin(ctx, cursorReq("grace", 0, nil), "c2")
		close(beginDone)
	}()
	select {
	case <-beginDone:
	case <-time.After(2 * time.Second):
		t.Fatal("takeover did not complete after the drain grace")
	}
	waitFor(t, time.Second, a1.isLost, "old stream leaves after grace")
}

// TestCursorIndividualAckOrdering: out-of-order acks only advance the
// confirmed position across the acked prefix.
func TestCursorIndividualAckOrdering(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m := newTestCursorManager(t, "filer-a:8888", kv)
	a, _, err := m.begin(ctx, cursorReq("ind", 0, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionAckGranularity = filer_pb.SubscriptionAckGranularity_ACK_INDIVIDUAL
	}), "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer m.release(ctx, a)
	for _, ts := range []int64{101, 102, 103} {
		m.markDelivered(a, &filer_pb.SubscribeMetadataResponse{
			TsNs: ts, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "x"}},
		})
	}
	// Ack the middle first: the head is still outstanding, so the position
	// cannot move.
	if c, err := m.ack(ctx, "ind", []int64{102}); err != nil || c != 0 {
		t.Fatalf("ack middle = %d, %v, want 0", c, err)
	}
	// Acking the head releases the already-acked middle too: the prefix is
	// contiguous through 102.
	if c, err := m.ack(ctx, "ind", []int64{101}); err != nil || c != 102 {
		t.Fatalf("ack head = %d, %v, want 102", c, err)
	}
	// Ack the tail: prefix empties and jumps to it.
	if c, err := m.ack(ctx, "ind", []int64{103}); err != nil || c != 103 {
		t.Fatalf("ack tail = %d, %v, want 103", c, err)
	}
}

// TestCursorReaperReclaimsAndRecords: a subscription quiet beyond the declared
// retention is reclaimed, the active stream is fenced, a disposition is kept,
// and re-registration starts fresh.
func TestCursorReaperReclaimsAndRecords(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m := newTestCursorManager(t, "filer-a:8888", kv)

	a, start, err := m.begin(ctx, cursorReq("stale", 100, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionAckTimeoutNs = int64(100 * time.Millisecond)
	}), "c1")
	if err != nil || start != 100 {
		t.Fatalf("begin = %d, %v", start, err)
	}
	waitFor(t, 2*time.Second, a.isLost, "active stream fenced after ack timeout")

	records, err := m.dispositions(ctx, 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("dispositions = %d, %v, want 1", len(records), err)
	}
	if records[0].Name != "stale" || records[0].Reason != namedCursorReasonAckTimeout {
		t.Fatalf("unexpected disposition: %+v", records[0])
	}
	subs, err := m.list(ctx)
	if err != nil || len(subs) != 0 {
		t.Fatalf("reclaimed cursor still listed: %v %+v", err, subs)
	}

	// Re-registration starts a fresh lifecycle from the self-reported point.
	a2, start, err := m.begin(ctx, cursorReq("stale", 500, nil), "c2")
	if err != nil || start != 500 {
		t.Fatalf("re-register = %d, %v, want 500", start, err)
	}
	defer m.release(ctx, a2)
}

// TestCursorDispositionRingBounds: the disposition log keeps a bounded number
// of newest records.
func TestCursorDispositionRingBounds(t *testing.T) {
	fastCursorTiming(t)
	kv := newMemCursorKV()
	ctx := context.Background()
	m := newTestCursorManager(t, "filer-a:8888", kv)

	// Records are prepended newest first; hand over a batch in that order.
	var disps []*filer_pb.SubscriptionDisposition
	for i := namedCursorMaxDispositions + 4; i >= 0; i-- {
		disps = append(disps, &filer_pb.SubscriptionDisposition{
			Name: fmt.Sprintf("n%d", i), Reason: namedCursorReasonAckTimeout, AtNs: int64(i),
		})
	}
	m.mu.Lock()
	if err := m.appendDispositionsLocked(ctx, disps); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.mu.Unlock()
	got, err := m.dispositions(ctx, 0)
	if err != nil || len(got) != namedCursorMaxDispositions {
		t.Fatalf("bounded dispositions = %d, %v", len(got), err)
	}
	// Newest first: the last inserted batch survives in front.
	if got[0].Name != fmt.Sprintf("n%d", namedCursorMaxDispositions+4) {
		t.Fatalf("newest disposition not first: %s", got[0].Name)
	}
}

func namedReq(name string, epoch int32, mutate func(*filer_pb.SubscribeMetadataRequest)) func(*filer_pb.SubscribeMetadataRequest) {
	return func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionName = name
		r.ClientEpoch = epoch
		if mutate != nil {
			mutate(r)
		}
	}
}

// TestE2ENamedSubscriptionResumeAndAck: full SubscribeLocalMetadata loop with
// a name; acked events are not redelivered after reconnect, and the stream
// starts with a start marker carrying the confirmed position.
func TestE2ENamedSubscriptionResumeAndAck(t *testing.T) {
	fastCursorTiming(t)
	h := newSubscribeHarness(t)
	mgr, err := h.fs.getNamedCursorManager()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Stop)

	base := h.base
	ts1, ts2, ts3 := base+1, base+2, base+3
	h.append(ts1)
	h.append(ts2)

	r1 := h.subscribe(base, namedReq("e2e-sync", 1, nil))
	got := waitForEvents(t, r1, []int64{ts1, ts2}, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	// Start marker must be the first response and name the owner.
	firstMsgs := r1.stream.snapshot()
	if len(firstMsgs) == 0 || firstMsgs[0].SubscriptionOwnerInstance == "" {
		t.Fatalf("missing owner start marker: %+v", firstMsgs)
	}
	if firstMsgs[0].SubscriptionConfirmedTsNs != base {
		t.Fatalf("marker confirmed = %d, want %d", firstMsgs[0].SubscriptionConfirmedTsNs, base)
	}

	if _, err := h.fs.AckMetadataSubscription(context.Background(), &filer_pb.AckMetadataSubscriptionRequest{
		SubscriptionName: "e2e-sync", TsNs: []int64{ts2},
	}); err != nil {
		t.Fatal(err)
	}
	r1.cancel()
	select {
	case <-r1.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("old stream did not finish")
	}

	// A fresh instance reports a wildly different sinceNs; the server ignores it.
	h.append(ts3)
	r2 := h.subscribe(0, namedReq("e2e-sync", 2, nil))
	got = waitForEvents(t, r2, []int64{ts3}, 3*time.Second)
	if len(got) != 1 || got[0] != ts3 {
		t.Fatalf("redelivery after ack: %v, want only %d", got, ts3)
	}
	r2.cancel()
}

// TestE2ENamedSubscriptionImmediateTakeover: two live streams, one name; the
// second connection closes the first immediately.
func TestE2ENamedSubscriptionImmediateTakeover(t *testing.T) {
	fastCursorTiming(t)
	h := newSubscribeHarness(t)
	mgr, err := h.fs.getNamedCursorManager()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Stop)

	base := h.base
	r1 := h.subscribe(base, namedReq("e2e-takeover", 1, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionTakeoverPolicy = filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DISCONNECT_OLD
	}))
	waitForEvents(t, r1, nil, time.Second) // let it park as the active owner

	r2 := h.subscribe(base, namedReq("e2e-takeover", 2, nil))
	t.Cleanup(r2.cancel)

	select {
	case <-r1.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("old stream was not closed by immediate takeover")
	}

	// Only the new stream receives subsequent events.
	ts := base + 10
	h.append(ts)
	got := waitForEvents(t, r2, []int64{ts}, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("new owner got %v", got)
	}
	oldGot := eventTimestamps(r1.stream.snapshot())
	for _, g := range oldGot {
		if g == ts {
			t.Fatal("old owner received an event after takeover")
		}
	}
}

// TestE2ENamedSubscriptionDrainTakeover: the new instance waits for the old
// one to ack its in-flight events, then resumes after them without duplicates.
func TestE2ENamedSubscriptionDrainTakeover(t *testing.T) {
	fastCursorTiming(t)
	h := newSubscribeHarness(t)
	mgr, err := h.fs.getNamedCursorManager()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Stop)

	base := h.base
	ts1, ts2 := base+20, base+21
	h.append(ts1)
	h.append(ts2)

	r1 := h.subscribe(base, namedReq("e2e-drain", 1, func(r *filer_pb.SubscribeMetadataRequest) {
		r.SubscriptionTakeoverPolicy = filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DRAIN_OLD
		r.SubscriptionTakeoverGraceNs = int64(5 * time.Second)
	}))
	got := waitForEvents(t, r1, []int64{ts1, ts2}, 3*time.Second)
	if len(got) != 2 {
		t.Fatalf("old owner events: %v", got)
	}

	// The second connection's handler blocks in begin() until the old stream
	// drains; h.subscribe returns once the goroutine is running, so "started"
	// means its start marker has actually been sent.
	r2 := h.subscribe(base, namedReq("e2e-drain", 2, nil))
	streamStarted := func(r *runningSubscribe) bool {
		msgs := r.stream.snapshot()
		return len(msgs) > 0 && msgs[0].SubscriptionOwnerInstance != ""
	}

	// While unacked, the old stream must still be the owner.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-r1.finished:
		t.Fatal("old stream left before the ack")
	default:
	}
	if streamStarted(r2) {
		t.Fatal("second stream started before the old stream drained")
	}

	if _, err := h.fs.AckMetadataSubscription(context.Background(), &filer_pb.AckMetadataSubscriptionRequest{
		SubscriptionName: "e2e-drain", TsNs: []int64{ts2},
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-r1.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("old stream did not leave after draining")
	}
	waitFor(t, 3*time.Second, func() bool { return streamStarted(r2) }, "new stream starting after drain")

	// The new owner continues after the confirmed events without duplicates.
	ts3 := base + 22
	h.append(ts3)
	got = waitForEvents(t, r2, []int64{ts3}, 3*time.Second)
	if len(got) != 1 || got[0] != ts3 {
		t.Fatalf("new owner delivery: %v, want only %d", got, ts3)
	}
	r2.cancel()
}

// TestE2ENamedSubscriptionAckRewindRejected end-to-end.
func TestE2ENamedSubscriptionAckRewindRejected(t *testing.T) {
	fastCursorTiming(t)
	h := newSubscribeHarness(t)
	mgr, err := h.fs.getNamedCursorManager()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mgr.Stop)

	base := h.base
	ts := base + 30
	h.append(ts)
	r := h.subscribe(base, namedReq("e2e-rewind", 1, nil))
	waitForEvents(t, r, []int64{ts}, 3*time.Second)
	if _, err := h.fs.AckMetadataSubscription(context.Background(), &filer_pb.AckMetadataSubscriptionRequest{
		SubscriptionName: "e2e-rewind", TsNs: []int64{ts},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.fs.AckMetadataSubscription(context.Background(), &filer_pb.AckMetadataSubscriptionRequest{
		SubscriptionName: "e2e-rewind", TsNs: []int64{ts - 1},
	}); err == nil {
		t.Fatal("rewind ack was accepted")
	}
	r.cancel()
}

// TestE2EAnonymousSubscriptionUnchanged: without a name the start marker is
// absent and self-reported SinceNs governs.
func TestE2EAnonymousSubscriptionUnchanged(t *testing.T) {
	h := newSubscribeHarness(t)
	base := h.base
	ts := base + 40
	h.append(ts)
	r := h.subscribe(base, nil)
	got := waitForEvents(t, r, []int64{ts}, 3*time.Second)
	if len(got) != 1 {
		t.Fatalf("anonymous events: %v", got)
	}
	for _, m := range r.stream.snapshot() {
		if m.SubscriptionOwnerInstance != "" || m.SubscriptionConfirmedTsNs != 0 {
			t.Fatalf("anonymous stream carried a cursor marker: %+v", m)
		}
	}
	r.cancel()
}
