package weed_server

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Server-managed named cursors for metadata subscriptions.
//
// A consumer registers a subscription under a name; the filer persists the
// confirmed consumption position of that name in its own metadata store. Any
// client instance connecting with the name resumes after the confirmed
// position, only one stream may be active for a name at any time, and a new
// instance either closes the old stream immediately or takes over once the
// old one has drained the events it already holds.
//
// Authority model: the persisted cursor record itself carries an ownership
// lease (owner instance + fencing epoch + wall-clock deadline). The owner
// renews the lease while its stream is alive and continuously fences itself
// against the record; a takeover bumps the epoch and, when possible, signals
// the old owner in-process. A filer that cannot reach the store or loses its
// fence stops serving. Events may be redelivered until acked (at-least-once);
// a confirmed position never moves backwards.

var (
	// cursorLeaseTTL bounds how long a dead owner can block a name after it
	// stops renewing. Vars (not consts) so tests shorten the waits.
	cursorLeaseTTL = 5 * time.Second

	// cursorFencePoll paces ownership-fence and takeover-state polling: how
	// quickly a remote old stream notices that its name was taken.
	cursorFencePoll = 200 * time.Millisecond

	// cursorDrainSettle lets events already inside the delivery pipeline
	// finish before a draining stream snapshots what it has handed out.
	cursorDrainSettle = 50 * time.Millisecond

	// cursorReaperScan paces the ack-timeout reaper.
	cursorReaperScan = 5 * time.Second

	// cursorDefaultTakeoverGrace bounds a DRAIN takeover when the client does
	// not declare its own grace.
	cursorDefaultTakeoverGrace = 10 * time.Second
)

const (
	namedCursorKeyPrefix        = "sw:namedcursor:v1:"
	namedCursorIndexKey         = namedCursorKeyPrefix + "__index__"
	namedCursorDispositionsKey  = namedCursorKeyPrefix + "__dispositions__"
	namedCursorMaxNameLength    = 128
	namedCursorMaxDispositions  = 200
	namedCursorReasonAckTimeout = "ack_timeout"
)

var namedCursorNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// cursorKV is the tiny persistence surface the manager needs. FilerStore
// satisfies it.
type cursorKV interface {
	KvPut(ctx context.Context, key []byte, value []byte) error
	KvGet(ctx context.Context, key []byte) (value []byte, err error)
	KvDelete(ctx context.Context, key []byte) error
}

type namedCursorManager struct {
	host        pb.ServerAddress
	kv          func() cursorKV
	instanceSeq atomic.Uint64

	mu     sync.Mutex
	active map[string]*activeNamedStream

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newNamedCursorManager(host pb.ServerAddress, kv func() cursorKV) *namedCursorManager {
	return &namedCursorManager{
		host:   host,
		kv:     kv,
		active: make(map[string]*activeNamedStream),
		stopCh: make(chan struct{}),
	}
}

func (m *namedCursorManager) Start() {
	m.wg.Add(1)
	go m.reapLoop()
}

func (m *namedCursorManager) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

func (m *namedCursorManager) newInstanceID() string {
	seq := m.instanceSeq.Add(1)
	return fmt.Sprintf("%s#%d-%d", m.host, time.Now().UnixNano(), seq)
}

// activeStream returns this manager's live ownership of a name, if any.
func (m *namedCursorManager) activeStream(name string) *activeNamedStream {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[name]
}

// activeNamedStream is one filer process's live ownership of a named cursor.
type activeNamedStream struct {
	name     string
	instance string
	epoch    int64
	cancel   context.CancelFunc

	// deliveredTsNs is the newest non-heartbeat position handed to the client
	// (real events and filtered-range markers), i.e. the in-flight frontier.
	deliveredTsNs atomic.Int64

	// INDIVIDUAL granularity: real event timestamps handed out but not yet
	// confirmed (sorted), plus acked points blocked behind an unacked head
	// (sorted). Owner-process memory only; its loss only means redelivery,
	// which the contract allows.
	pendingMu      sync.Mutex
	pendingUnacked []int64
	pendingAcked   []int64

	draining    atomic.Bool
	lost        atomic.Bool
	demandCh    chan struct{} // signaled when a takeover demand lands locally
	drainKickCh chan struct{} // signaled when an ack may have completed the drain
	drainDoneCh chan struct{} // closed when this stream must leave (drain finished, fenced, lost)
	doneCh      chan struct{} // closed when the fencing watcher has exited
}

func (a *activeNamedStream) isDraining() bool { return a.draining.Load() }
func (a *activeNamedStream) isLost() bool     { return a.lost.Load() }

// waitDrainCompletion blocks until the drain (or ownership loss) lets this
// stream leave. Returns immediately for non-drain endings.
func (a *activeNamedStream) waitDrainCompletion(ctx context.Context) {
	if !a.draining.Load() || a.lost.Load() {
		return
	}
	select {
	case <-a.drainDoneCh:
	case <-ctx.Done():
	}
}

// begin registers (or re-registers) a named subscription and acquires active
// ownership of it, blocking for a DRAIN takeover while the old stream finishes.
// It returns the live ownership and the exclusive start position the stream
// must read from.
func (m *namedCursorManager) begin(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, clientAddress string) (*activeNamedStream, int64, error) {
	name := req.SubscriptionName
	if err := validateCursorName(name); err != nil {
		return nil, 0, err
	}

	// Registration/create/load.
	m.mu.Lock()
	st, err := m.loadStateLocked(ctx, name)
	if err != nil {
		m.mu.Unlock()
		return nil, 0, err
	}
	if st == nil {
		st = newCursorStateFromRequest(name, req, clientAddress)
		if err := m.saveStateLocked(ctx, st); err != nil {
			m.mu.Unlock()
			return nil, 0, err
		}
		if err := m.addIndexLocked(ctx, name); err != nil {
			m.mu.Unlock()
			return nil, 0, err
		}
		glog.V(0).Infof("named subscription %s registered by %s, start %d (grace=%v ackTimeout=%v granularity=%v takeover=%v)",
			name, clientAddress, st.ConfirmedTsNs, st.TakeoverGraceNs, st.AckTimeoutNs, st.AckGranularity, st.TakeoverPolicy)
	} else if st.Reclaimed {
		// A previous lifecycle was reclaimed; its disposition stays on
		// record. Re-registration starts a fresh lifecycle from the position
		// this instance reports.
		st = newCursorStateFromRequest(name, req, clientAddress)
		if err := m.saveStateLocked(ctx, st); err != nil {
			m.mu.Unlock()
			return nil, 0, err
		}
		if err := m.addIndexLocked(ctx, name); err != nil {
			m.mu.Unlock()
			return nil, 0, err
		}
		glog.V(0).Infof("named subscription %s re-registered by %s after reclaim, start %d", name, clientAddress, st.ConfirmedTsNs)
	}
	startTsNs := st.ConfirmedTsNs
	policy := st.TakeoverPolicy
	m.mu.Unlock()

	// Takeover of a live owner. None of the waiting may hold m.mu: the drain
	// completes only when the old consumer's acks land, and those acks take
	// the same lock.
	if ownerStream := m.activeStream(name); ownerStream != nil {
		if policy == filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DISCONNECT_OLD {
			glog.V(0).Infof("named subscription %s: immediate takeover by %s, closing local stream %s", name, clientAddress, ownerStream.instance)
			ownerStream.lost.Store(true)
			closeIfOpen(ownerStream.drainDoneCh)
			ownerStream.cancel()
			select {
			case <-ownerStream.doneCh:
			case <-time.After(cursorLeaseTTL):
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-m.stopCh:
				return nil, 0, fmt.Errorf("filer is shutting down")
			}
		} else {
			grace := int64(cursorDefaultTakeoverGrace)
			m.mu.Lock()
			current, lerr := m.loadStateLocked(ctx, name)
			if lerr != nil {
				m.mu.Unlock()
				return nil, 0, lerr
			}
			if current != nil {
				grace = takeoverGrace(current)
				current.TakeoverByInstance = "pending-local:" + name
				current.TakeoverAtNs = time.Now().UnixNano()
				current.DrainGraceUntilNs = time.Now().UnixNano() + grace
				lerr = m.saveStateLocked(ctx, current)
			}
			m.mu.Unlock()
			if lerr != nil {
				return nil, 0, lerr
			}
			glog.V(0).Infof("named subscription %s: drain takeover requested by %s, waiting for local stream %s", name, clientAddress, ownerStream.instance)
			select {
			case ownerStream.demandCh <- struct{}{}:
			default:
			}
			deadline := time.After(time.Duration(grace))
			select {
			case <-ownerStream.drainDoneCh:
			case <-deadline:
				glog.V(0).Infof("named subscription %s: local drain grace elapsed, taking over", name)
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-m.stopCh:
				return nil, 0, fmt.Errorf("filer is shutting down")
			}
		}
	} else {
		m.mu.Lock()
		current, lerr := m.loadStateLocked(ctx, name)
		m.mu.Unlock()
		if lerr != nil {
			return nil, 0, lerr
		}
		if current != nil && current.OwnerInstance != "" && current.LeaseUntilNs > time.Now().UnixNano() {
			// Owned by a stream on another filer (or one this process forgot).
			st, err = m.acquireRemote(ctx, req, clientAddress, current)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	// Final acquisition under the lock, from the freshest record: a DRAIN
	// takeover waited here while the old owner acked its in-flight events, so
	// the confirmed position moved while begin blocked.
	m.mu.Lock()
	defer m.mu.Unlock()

	current, err := m.loadStateLocked(ctx, name)
	if err != nil {
		return nil, 0, err
	}
	if current != nil && !current.Reclaimed {
		st = current
	} else if current == nil {
		st = newCursorStateFromRequest(name, req, clientAddress)
	}
	startTsNs = st.ConfirmedTsNs
	now := time.Now()
	instance := m.newInstanceID()
	st.OwnerInstance = instance
	st.OwnerAddress = string(m.host)
	st.OwnerEpoch++
	st.LeaseUntilNs = now.Add(cursorLeaseTTL).UnixNano()
	st.LastRenewAtNs = now.UnixNano()
	st.TakeoverByInstance = ""
	st.TakeoverByAddress = ""
	st.TakeoverAtNs = 0
	st.DrainGraceUntilNs = 0
	st.LastDeliveredTsNs = 0
	st.Reclaimed = false
	if err := m.saveStateLocked(ctx, st); err != nil {
		return nil, 0, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	active := &activeNamedStream{
		name:        name,
		instance:    instance,
		epoch:       st.OwnerEpoch,
		cancel:      cancel,
		demandCh:    make(chan struct{}, 1),
		drainKickCh: make(chan struct{}, 1),
		drainDoneCh: make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	m.active[name] = active
	go m.watchOwnership(streamCtx, active)

	glog.V(0).Infof("named subscription %s active as %s epoch %d, resume after %d", name, instance, st.OwnerEpoch, startTsNs)
	return active, startTsNs, nil
}

// acquireRemote drives a takeover against an owner in another process through
// the persisted record: IMMEDIATE bumps the epoch at once, DRAIN waits for the
// old owner to release after acking what it holds, bounded by the grace.
func (m *namedCursorManager) acquireRemote(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, clientAddress string, st *filer_pb.NamedSubscriptionCursorState) (*filer_pb.NamedSubscriptionCursorState, error) {
	name := req.SubscriptionName
	now := time.Now().UnixNano()
	grace := takeoverGrace(st)
	st.TakeoverByInstance = m.newInstanceID()
	st.TakeoverByAddress = string(m.host)
	st.TakeoverAtNs = now
	st.DrainGraceUntilNs = now + grace
	immediate := st.TakeoverPolicy == filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DISCONNECT_OLD
	m.mu.Lock()
	if err := m.saveStateLocked(ctx, st); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Unlock()
	if immediate {
		glog.V(0).Infof("named subscription %s: immediate takeover by %s of remote stream %s", name, clientAddress, st.OwnerInstance)
		return st, nil
	}
	glog.V(0).Infof("named subscription %s: drain takeover by %s of remote stream %s, grace %v", name, clientAddress, st.OwnerInstance, time.Duration(grace))
	timer := time.NewTimer(cursorFencePoll)
	defer timer.Stop()
	graceDeadline := time.Now().Add(time.Duration(grace))
	for {
		// Reprogram the poll timer.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(cursorFencePoll)
		select {
		case <-m.stopCh:
			return nil, fmt.Errorf("filer is shutting down")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		m.mu.Lock()
		current, err := m.loadStateLocked(ctx, name)
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if current == nil {
			return newCursorStateFromRequest(name, req, clientAddress), nil
		}
		if current.OwnerInstance == "" || current.OwnerEpoch != st.OwnerEpoch {
			// The old owner released after draining (or someone else fenced
			// it).
			return current, nil
		}
		if time.Now().After(graceDeadline) {
			glog.V(0).Infof("named subscription %s: drain grace elapsed, taking over from %s", name, current.OwnerInstance)
			return current, nil
		}
	}
}

// release ends this process's ownership of a name. It never clears an owner
// record that a newer epoch already replaced, so a taken-over stream cannot
// strip the new owner.
func (m *namedCursorManager) release(ctx context.Context, a *activeNamedStream) {
	a.cancel()
	<-a.doneCh

	m.mu.Lock()
	defer m.mu.Unlock()
	if cur := m.active[a.name]; cur == a {
		delete(m.active, a.name)
	}
	st, err := m.loadStateLocked(ctx, a.name)
	if err != nil {
		glog.V(0).Infof("named subscription %s: reload on release failed: %v", a.name, err)
		return
	}
	if st == nil {
		return
	}
	if st.OwnerInstance != a.instance || st.OwnerEpoch != a.epoch {
		// Already taken over (and possibly re-owned); leave the record alone.
		glog.V(0).Infof("named subscription %s: stream %s epoch %d released while owner is %s epoch %d",
			a.name, a.instance, a.epoch, st.OwnerInstance, st.OwnerEpoch)
		return
	}
	st.OwnerInstance = ""
	st.OwnerAddress = ""
	st.OwnerEpoch++
	st.LeaseUntilNs = 0
	st.TakeoverByInstance = ""
	st.TakeoverByAddress = ""
	st.TakeoverAtNs = 0
	st.DrainGraceUntilNs = 0
	if err := m.saveStateLocked(ctx, st); err != nil {
		glog.Errorf("named subscription %s: persist release failed: %v", a.name, err)
		return
	}
	glog.V(0).Infof("named subscription %s: stream %s released at confirmed %d", a.name, a.instance, st.ConfirmedTsNs)
}

// watchOwnership fences the stream against the persisted record, renews its
// lease, and drives a remote-or-local takeover demand.
func (m *namedCursorManager) watchOwnership(ctx context.Context, a *activeNamedStream) {
	defer close(a.doneCh)
	ticker := time.NewTicker(cursorFencePoll)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			a.lost.Store(true)
			closeIfOpen(a.drainDoneCh)
			a.cancel()
			return
		case <-ctx.Done():
			return
		case <-a.demandCh:
		case <-ticker.C:
		}

		m.mu.Lock()
		st, err := m.loadStateLocked(ctx, a.name)
		if err != nil {
			glog.V(0).Infof("named subscription %s: fence reload failed: %v", a.name, err)
			m.mu.Unlock()
			continue
		}
		if st == nil || st.Reclaimed || st.OwnerInstance != a.instance || st.OwnerEpoch != a.epoch {
			m.mu.Unlock()
			glog.V(0).Infof("named subscription %s: stream %s fenced (owner now %v epoch %v, reclaimed=%v)",
				a.name, a.instance, stOwner(st), stEpoch(st), st != nil && st.Reclaimed)
			a.lost.Store(true)
			closeIfOpen(a.drainDoneCh)
			a.cancel()
			return
		}

		if !a.draining.Load() && st.TakeoverByInstance != "" {
			if st.TakeoverPolicy == filer_pb.SubscriptionTakeoverPolicy_TAKEOVER_DISCONNECT_OLD {
				m.mu.Unlock()
				glog.V(0).Infof("named subscription %s: stream %s closing for immediate takeover by %s",
					a.name, a.instance, st.TakeoverByInstance)
				a.lost.Store(true)
				closeIfOpen(a.drainDoneCh)
				a.cancel()
				return
			}
			a.draining.Store(true)
			m.mu.Unlock()
			glog.V(0).Infof("named subscription %s: stream %s draining for takeover by %s",
				a.name, a.instance, st.TakeoverByInstance)
			go m.drainOwnership(ctx, a, st.DrainGraceUntilNs)
			continue
		}

		// Lease renewal.
		now := time.Now()
		if st.LeaseUntilNs-now.UnixNano() < int64(cursorLeaseTTL)/2 {
			st.LeaseUntilNs = now.Add(cursorLeaseTTL).UnixNano()
			st.LastRenewAtNs = now.UnixNano()
			if err := m.saveStateLocked(ctx, st); err != nil {
				glog.V(0).Infof("named subscription %s: lease renewal failed: %v", a.name, err)
			}
		}
		m.mu.Unlock()
	}
}

// drainOwnership stops new delivery, waits (bounded by the grace) for the
// client to ack everything the stream has handed out, then releases the name.
func (m *namedCursorManager) drainOwnership(ctx context.Context, a *activeNamedStream, graceUntilNs int64) {
	// Let sends already inside the pipeline land before freezing the target.
	select {
	case <-time.After(cursorDrainSettle):
	case <-ctx.Done():
	}
	target := a.deliveredTsNs.Load()

	m.mu.Lock()
	st, err := m.loadStateLocked(ctx, a.name)
	if err == nil && st != nil && st.OwnerInstance == a.instance && st.OwnerEpoch == a.epoch {
		st.LastDeliveredTsNs = target
		if graceUntilNs == 0 {
			graceUntilNs = time.Now().UnixNano() + int64(cursorDefaultTakeoverGrace)
		}
		st.DrainGraceUntilNs = graceUntilNs
		err = m.saveStateLocked(ctx, st)
	}
	m.mu.Unlock()
	if err != nil {
		glog.V(0).Infof("named subscription %s: persist drain target failed: %v", a.name, err)
	}
	glog.V(0).Infof("named subscription %s: draining, target %d, confirmed will be caught up to it", a.name, target)

	waitUntil := time.Unix(0, graceUntilNs)
	for {
		remaining := time.Until(waitUntil)
		var waitCh <-chan time.Time
		if remaining <= 0 {
			// Grace already over; check once more below and leave.
			waitCh = time.After(0)
		} else {
			t := time.NewTimer(remaining)
			waitCh = t.C
			defer t.Stop()
		}
		select {
		case <-ctx.Done():
			// The client went away; the deferred release handles ownership.
			return
		case <-a.drainKickCh:
		case <-waitCh:
		}

		m.mu.Lock()
		st, err := m.loadStateLocked(ctx, a.name)
		caughtUp := err == nil && st != nil && st.ConfirmedTsNs >= target
		graceOver := time.Now().UnixNano() >= graceUntilNs
		if err == nil && st != nil && st.OwnerInstance == a.instance && st.OwnerEpoch == a.epoch && (caughtUp || graceOver) {
			st.OwnerInstance = ""
			st.OwnerAddress = ""
			st.OwnerEpoch++
			st.LeaseUntilNs = 0
			st.TakeoverByInstance = ""
			st.TakeoverByAddress = ""
			st.TakeoverAtNs = 0
			st.DrainGraceUntilNs = 0
			st.LastDeliveredTsNs = st.ConfirmedTsNs
			err = m.saveStateLocked(ctx, st)
			if err == nil {
				if cur := m.active[a.name]; cur == a {
					delete(m.active, a.name)
				}
			}
		}
		m.mu.Unlock()
		if err != nil {
			glog.V(0).Infof("named subscription %s: drain completion persist failed: %v", a.name, err)
		}
		if caughtUp {
			glog.V(0).Infof("named subscription %s: drained cleanly through %d, handing over", a.name, target)
		} else if graceOver {
			glog.Warningf("named subscription %s: drain grace elapsed with confirmed below %d, handing over anyway; events will be redelivered", a.name, target)
		} else {
			continue
		}
		// The name has a new owner from this stream's point of view in both
		// endings; mark lost before cancelling so gates and tests observe it
		// even when the watcher exits through the cancelled context.
		a.lost.Store(true)
		closeIfOpen(a.drainDoneCh)
		a.cancel()
		return
	}
}

// markDelivered records the newest position handed to the client and, under
// INDIVIDUAL granularity, the outstanding real-event timestamps.
func (m *namedCursorManager) markDelivered(a *activeNamedStream, msg *filer_pb.SubscribeMetadataResponse) {
	if a == nil || msg == nil || msg.EventNotification == nil || msg.TsNs <= 0 {
		return
	}
	for {
		cur := a.deliveredTsNs.Load()
		if msg.TsNs <= cur || a.deliveredTsNs.CompareAndSwap(cur, msg.TsNs) {
			break
		}
	}
	if filer_pb.IsEmpty(msg) {
		// Filtered-range marker, not a real event the consumer must ack one by
		// one; the watermark frontier may still pass over it.
		return
	}
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	idx := sort.Search(len(a.pendingUnacked), func(i int) bool { return a.pendingUnacked[i] >= msg.TsNs })
	if idx < len(a.pendingUnacked) && a.pendingUnacked[idx] == msg.TsNs {
		return
	}
	a.pendingUnacked = append(a.pendingUnacked, 0)
	copy(a.pendingUnacked[idx+1:], a.pendingUnacked[idx:])
	a.pendingUnacked[idx] = msg.TsNs
}

// errAckAtOtherOwner asks the RPC handler to forward an ack to the filer
// currently owning the name.
type errAckAtOtherOwner struct{ ownerAddress string }

func (e errAckAtOtherOwner) Error() string {
	return fmt.Sprintf("named subscription owned by %s", e.ownerAddress)
}

// ack applies a client confirmation. WATERMARK: every event at or below the
// largest timestamp is confirmed. INDIVIDUAL: the listed events are marked
// processed and the confirmed position advances across the acked prefix in
// delivery order. It never moves the confirmed position backwards.
func (m *namedCursorManager) ack(ctx context.Context, name string, timestamps []int64) (int64, error) {
	if err := validateCursorName(name); err != nil {
		return 0, err
	}
	if len(timestamps) == 0 {
		return 0, fmt.Errorf("ack for %s carries no timestamp", name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.loadStateLocked(ctx, name)
	if err != nil {
		return 0, err
	}
	if st == nil {
		return 0, fmt.Errorf("named subscription %q is not registered", name)
	}

	maxTs := timestamps[0]
	for _, ts := range timestamps {
		if ts < st.ConfirmedTsNs {
			return st.ConfirmedTsNs, fmt.Errorf("named subscription %s rejects ack %d behind confirmed %d: position cannot move backwards", name, ts, st.ConfirmedTsNs)
		}
		if ts > maxTs {
			maxTs = ts
		}
	}

	newConfirmed := st.ConfirmedTsNs
	if st.AckGranularity == filer_pb.SubscriptionAckGranularity_ACK_INDIVIDUAL {
		a := m.active[name]
		if a != nil && a.instance == st.OwnerInstance {
			a.pendingMu.Lock()
			// Register every acked point; dedup while inserting sorted.
			for _, ts := range timestamps {
				idx := sort.Search(len(a.pendingAcked), func(i int) bool { return a.pendingAcked[i] >= ts })
				if idx < len(a.pendingAcked) && a.pendingAcked[idx] == ts {
					continue
				}
				a.pendingAcked = append(a.pendingAcked, 0)
				copy(a.pendingAcked[idx+1:], a.pendingAcked[idx:])
				a.pendingAcked[idx] = ts
			}
			// Pop the acked prefix of outstanding events, in delivery order.
			for len(a.pendingUnacked) > 0 {
				head := a.pendingUnacked[0]
				idx := sort.Search(len(a.pendingAcked), func(i int) bool { return a.pendingAcked[i] >= head })
				if idx >= len(a.pendingAcked) || a.pendingAcked[idx] != head {
					break
				}
				if head > newConfirmed {
					newConfirmed = head
				}
				a.pendingUnacked = a.pendingUnacked[1:]
			}
			// Every acked point below the new unacked head is confirmed too
			// (filtered markers, or events acked out of order); absorb them.
			cut := len(a.pendingAcked)
			if len(a.pendingUnacked) > 0 {
				cut = sort.Search(len(a.pendingAcked), func(i int) bool { return a.pendingAcked[i] >= a.pendingUnacked[0] })
			}
			if cut > 0 && a.pendingAcked[cut-1] > newConfirmed {
				newConfirmed = a.pendingAcked[cut-1]
			}
			a.pendingAcked = a.pendingAcked[cut:]
			a.pendingMu.Unlock()
		} else if st.OwnerInstance != "" && st.OwnerAddress != "" && st.OwnerAddress != string(m.host) {
			// Outstanding events are tracked in the owner process's memory.
			return st.ConfirmedTsNs, errAckAtOtherOwner{ownerAddress: st.OwnerAddress}
		} else {
			// No owner on record here: apply the ack monotonically.
			if maxTs > newConfirmed {
				newConfirmed = maxTs
			}
		}
	} else if maxTs > newConfirmed {
		newConfirmed = maxTs
	}

	if newConfirmed == st.ConfirmedTsNs {
		return newConfirmed, nil
	}
	st.ConfirmedTsNs = newConfirmed
	st.LastAckAtNs = time.Now().UnixNano()
	if err := m.saveStateLocked(ctx, st); err != nil {
		return st.ConfirmedTsNs, err
	}
	if a := m.active[name]; a != nil && a.draining.Load() {
		select {
		case a.drainKickCh <- struct{}{}:
		default:
		}
	}
	return newConfirmed, nil
}

// reapLoop reclaims cursors whose owner stopped confirming for the declared
// retention, and leaves a queryable disposition for each reclaim.
func (m *namedCursorManager) reapLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(cursorReaperScan)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.reapOnce(context.Background())
		}
	}
}

func (m *namedCursorManager) reapOnce(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	names, err := m.indexLocked(ctx)
	if err != nil {
		glog.V(1).Infof("named subscriptions: read index failed: %v", err)
		return
	}
	now := time.Now().UnixNano()
	var removed []string
	var dispositions []*filer_pb.SubscriptionDisposition
	for _, name := range names {
		st, err := m.loadStateLocked(ctx, name)
		if err != nil {
			glog.V(1).Infof("named subscriptions: load %s failed: %v", name, err)
			continue
		}
		if st == nil || st.Reclaimed || st.AckTimeoutNs <= 0 {
			continue
		}
		if now-st.LastAckAtNs < st.AckTimeoutNs {
			continue
		}
		dispositions = append(dispositions, &filer_pb.SubscriptionDisposition{
			Name:              name,
			Reason:            namedCursorReasonAckTimeout,
			AtNs:              now,
			ConfirmedTsNs:     st.ConfirmedTsNs,
			LastDeliveredTsNs: st.LastDeliveredTsNs,
			OwnerInstance:     st.OwnerInstance,
			OwnerAddress:      st.OwnerAddress,
			Detail:            fmt.Sprintf("no ack for %v (declared retention %v)", time.Duration(now-st.LastAckAtNs), time.Duration(st.AckTimeoutNs)),
		})
		st.Reclaimed = true
		st.OwnerInstance = ""
		st.OwnerAddress = ""
		st.OwnerEpoch++
		st.LeaseUntilNs = 0
		st.TakeoverByInstance = ""
		st.TakeoverByAddress = ""
		st.TakeoverAtNs = 0
		st.DrainGraceUntilNs = 0
		if err := m.saveStateLocked(ctx, st); err != nil {
			dispositions = dispositions[:len(dispositions)-1]
			glog.Errorf("named subscription %s: persist reclaim failed: %v", name, err)
			continue
		}
		removed = append(removed, name)
		if a := m.active[name]; a != nil {
			a.lost.Store(true)
			closeIfOpen(a.drainDoneCh)
			a.cancel()
		}
		glog.Warningf("named subscription %s reclaimed: no ack for %v, last confirmed %d", name, time.Duration(now-st.LastAckAtNs), st.ConfirmedTsNs)
	}
	if len(removed) > 0 {
		if err := m.removeIndexLocked(ctx, removed); err != nil {
			glog.Errorf("named subscriptions: update index failed: %v", err)
		}
	}
	if len(dispositions) > 0 {
		if err := m.appendDispositionsLocked(ctx, dispositions); err != nil {
			glog.Errorf("named subscriptions: persist dispositions failed: %v", err)
		}
	}
}

func (m *namedCursorManager) list(ctx context.Context) ([]*filer_pb.NamedMetadataSubscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	names, err := m.indexLocked(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*filer_pb.NamedMetadataSubscription, 0, len(names))
	now := time.Now().UnixNano()
	for _, name := range names {
		st, err := m.loadStateLocked(ctx, name)
		if err != nil {
			return nil, err
		}
		if st == nil {
			continue
		}
		out = append(out, &filer_pb.NamedMetadataSubscription{
			Name:            st.Name,
			ConfirmedTsNs:   st.ConfirmedTsNs,
			LastAckAtNs:     st.LastAckAtNs,
			CreatedAtNs:     st.CreatedAtNs,
			AckTimeoutNs:    st.AckTimeoutNs,
			TakeoverPolicy:  st.TakeoverPolicy,
			AckGranularity:  st.AckGranularity,
			TakeoverGraceNs: st.TakeoverGraceNs,
			Active:          st.OwnerInstance != "" && st.LeaseUntilNs > now,
			OwnerInstance:   st.OwnerInstance,
			OwnerAddress:    st.OwnerAddress,
			PathPrefix:      st.PathPrefix,
			Reclaimed:       st.Reclaimed,
		})
	}
	return out, nil
}

func (m *namedCursorManager) dispositions(ctx context.Context, limit int) ([]*filer_pb.SubscriptionDisposition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := m.kv().KvGet(ctx, []byte(namedCursorDispositionsKey))
	if err != nil {
		if isKvNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	list := &filer_pb.ListMetadataSubscriptionDispositionsResponse{}
	if err := proto.Unmarshal(data, list); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > len(list.Dispositions) {
		return list.Dispositions, nil
	}
	return list.Dispositions[:limit], nil
}

func (m *namedCursorManager) appendDispositionsLocked(ctx context.Context, extra []*filer_pb.SubscriptionDisposition) error {
	data, err := m.kv().KvGet(ctx, []byte(namedCursorDispositionsKey))
	list := &filer_pb.ListMetadataSubscriptionDispositionsResponse{}
	if err == nil {
		_ = proto.Unmarshal(data, list)
	} else if !isKvNotFound(err) {
		return err
	}
	merged := append(extra, list.Dispositions...)
	if len(merged) > namedCursorMaxDispositions {
		merged = merged[:namedCursorMaxDispositions]
	}
	out, err := proto.Marshal(&filer_pb.ListMetadataSubscriptionDispositionsResponse{Dispositions: merged})
	if err != nil {
		return err
	}
	return m.kv().KvPut(ctx, []byte(namedCursorDispositionsKey), out)
}

// ---- persistence helpers (callers hold m.mu) ----

func (m *namedCursorManager) loadStateLocked(ctx context.Context, name string) (*filer_pb.NamedSubscriptionCursorState, error) {
	data, err := m.kv().KvGet(ctx, []byte(namedCursorKeyPrefix+name))
	if err != nil {
		if isKvNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	st := &filer_pb.NamedSubscriptionCursorState{}
	if err := proto.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("unmarshal named cursor %s: %w", name, err)
	}
	return st, nil
}

func (m *namedCursorManager) saveStateLocked(ctx context.Context, st *filer_pb.NamedSubscriptionCursorState) error {
	data, err := proto.Marshal(st)
	if err != nil {
		return err
	}
	return m.kv().KvPut(ctx, []byte(namedCursorKeyPrefix+st.Name), data)
}

func (m *namedCursorManager) indexLocked(ctx context.Context) ([]string, error) {
	data, err := m.kv().KvGet(ctx, []byte(namedCursorIndexKey))
	if err != nil {
		if isKvNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n"), nil
}

func (m *namedCursorManager) writeIndexLocked(ctx context.Context, names []string) error {
	sort.Strings(names)
	value := strings.Join(names, "\n")
	if value == "" {
		return m.kv().KvDelete(ctx, []byte(namedCursorIndexKey))
	}
	return m.kv().KvPut(ctx, []byte(namedCursorIndexKey), []byte(value))
}

func (m *namedCursorManager) addIndexLocked(ctx context.Context, name string) error {
	names, err := m.indexLocked(ctx)
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == name {
			return nil
		}
	}
	return m.writeIndexLocked(ctx, append(names, name))
}

func (m *namedCursorManager) removeIndexLocked(ctx context.Context, gone []string) error {
	names, err := m.indexLocked(ctx)
	if err != nil {
		return err
	}
	drop := make(map[string]struct{}, len(gone))
	for _, n := range gone {
		drop[n] = struct{}{}
	}
	kept := names[:0]
	for _, n := range names {
		if _, ok := drop[n]; !ok {
			kept = append(kept, n)
		}
	}
	return m.writeIndexLocked(ctx, kept)
}

func newCursorStateFromRequest(name string, req *filer_pb.SubscribeMetadataRequest, clientAddress string) *filer_pb.NamedSubscriptionCursorState {
	now := time.Now().UnixNano()
	grace := req.SubscriptionTakeoverGraceNs
	if grace <= 0 {
		grace = int64(cursorDefaultTakeoverGrace)
	}
	return &filer_pb.NamedSubscriptionCursorState{
		Name:            name,
		ConfirmedTsNs:   req.SinceNs,
		CreatedAtNs:     now,
		LastAckAtNs:     now,
		AckTimeoutNs:    req.SubscriptionAckTimeoutNs,
		TakeoverPolicy:  req.SubscriptionTakeoverPolicy,
		AckGranularity:  req.SubscriptionAckGranularity,
		TakeoverGraceNs: grace,
		PathPrefix:      req.PathPrefix,
		PathPrefixes:    req.PathPrefixes,
		Directories:     req.Directories,
		Signature:       req.Signature,
	}
}

func takeoverGrace(st *filer_pb.NamedSubscriptionCursorState) int64 {
	if st.TakeoverGraceNs > 0 {
		return st.TakeoverGraceNs
	}
	return int64(cursorDefaultTakeoverGrace)
}

func validateCursorName(name string) error {
	if name == "" {
		return fmt.Errorf("subscription name is empty")
	}
	if len(name) > namedCursorMaxNameLength {
		return fmt.Errorf("subscription name %q is longer than %d bytes", name, namedCursorMaxNameLength)
	}
	if !namedCursorNamePattern.MatchString(name) {
		return fmt.Errorf("subscription name %q must match %s", name, namedCursorNamePattern.String())
	}
	return nil
}

func closeIfOpen(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}

func isKvNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func stOwner(st *filer_pb.NamedSubscriptionCursorState) string {
	if st == nil {
		return ""
	}
	return st.OwnerInstance
}

func stEpoch(st *filer_pb.NamedSubscriptionCursorState) int64 {
	if st == nil {
		return 0
	}
	return st.OwnerEpoch
}
