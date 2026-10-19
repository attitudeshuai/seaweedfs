package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// getNamedCursorManager lazily starts the named-cursor manager once a named
// subscription reaches this filer.
func (fs *FilerServer) getNamedCursorManager() (*namedCursorManager, error) {
	fs.namedCursorsLock.Lock()
	defer fs.namedCursorsLock.Unlock()
	if fs.namedCursors != nil {
		return fs.namedCursors, nil
	}
	if fs.filer == nil || fs.filer.Store == nil {
		return nil, fmt.Errorf("metadata store is not available for named subscriptions")
	}
	var host pb.ServerAddress
	if fs.option != nil {
		host = fs.option.Host
	}
	store := fs.filer.Store
	m := newNamedCursorManager(host, func() cursorKV { return store })
	m.Start()
	fs.namedCursors = m
	return m, nil
}

// namedActiveFor returns the local ownership record of a request's named
// subscription, or nil for anonymous subscriptions / names owned elsewhere.
func (fs *FilerServer) namedActiveFor(req *filer_pb.SubscribeMetadataRequest) *activeNamedStream {
	if req == nil || req.SubscriptionName == "" || fs.namedCursors == nil {
		return nil
	}
	return fs.namedCursors.activeStream(req.SubscriptionName)
}

// subscriptionProceed is the shared liveness gate of both subscribe loops.
// For anonymous subscriptions it is exactly the previous hasClient check.
// For a named one it also fences a stream that lost ownership, and blocks a
// draining stream until its drain completes so the old consumer acks what it
// already holds before the stream leaves.
func (fs *FilerServer) subscriptionProceed(ctx context.Context, req *filer_pb.SubscribeMetadataRequest) bool {
	if !fs.hasClient(req.ClientId, req.ClientEpoch) {
		return false
	}
	a := fs.namedActiveFor(req)
	if a == nil {
		return true
	}
	if a.isLost() {
		return false
	}
	if a.isDraining() {
		a.waitDrainCompletion(ctx)
		return false
	}
	return true
}

// cursorEntryGate is the per-log-entry gate for disk and memory passes, where
// the wait predicates are not consulted between entries. A fenced stream ends
// at once; a draining stream finishes handing out nothing new and blocks here
// until its drain completes, so the in-flight events it already buffered can
// be acked before the stream leaves.
func (fs *FilerServer) cursorEntryGate(ctx context.Context, req *filer_pb.SubscribeMetadataRequest) bool {
	a := fs.namedActiveFor(req)
	if a == nil {
		return false
	}
	if a.isLost() {
		return true
	}
	if a.isDraining() {
		a.waitDrainCompletion(ctx)
		return true
	}
	return false
}

// beginNamedSubscription acquires a named cursor for a new stream and returns
// the ownership record and the start position. The returned release function
// must be deferred by the handler.
func (fs *FilerServer) beginNamedSubscription(ctx context.Context, req *filer_pb.SubscribeMetadataRequest, peerAddress string) (*namedCursorManager, *activeNamedStream, int64, error) {
	if req.SubscriptionName == "" {
		return nil, nil, req.SinceNs, nil
	}
	mgr, err := fs.getNamedCursorManager()
	if err != nil {
		return nil, nil, 0, err
	}
	active, startTsNs, err := mgr.begin(ctx, req, peerAddress)
	if err != nil {
		return nil, nil, 0, err
	}
	return mgr, active, startTsNs, nil
}

// namedDeliveryTracker wraps a subscribe stream so every successfully sent
// non-heartbeat message advances the named cursor's delivery frontier. Heart
// beats, flush reports and chunk refs carry no EventNotification/TsNs and are
// intentionally ignored, exactly as the client ignores them for checkpoints.
type namedDeliveryTracker struct {
	inner  metadataLocalStream
	mgr    *namedCursorManager
	active *activeNamedStream
}

func (t *namedDeliveryTracker) Send(msg *filer_pb.SubscribeMetadataResponse) error {
	if err := t.inner.Send(msg); err != nil {
		return err
	}
	t.mgr.markDelivered(t.active, msg)
	return nil
}

func (t *namedDeliveryTracker) Context() context.Context {
	return t.inner.Context()
}

// namedDeliveryStream wraps a stream with named-cursor delivery tracking when
// the subscription is named, and returns the bare stream otherwise.
func (fs *FilerServer) namedDeliveryStream(mgr *namedCursorManager, active *activeNamedStream, stream metadataLocalStream) metadataStreamSender {
	if active == nil {
		return stream
	}
	return &namedDeliveryTracker{inner: stream, mgr: mgr, active: active}
}
