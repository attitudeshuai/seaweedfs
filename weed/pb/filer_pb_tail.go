package pb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type EventErrorType int

const (
	TrivialOnError EventErrorType = iota
	FatalOnError
	RetryForeverOnError
	DontLogError
)

// MetadataFollowOption is used to control the behavior of the metadata following
// process. Part of it is used as a cursor to resume the following process.
type MetadataFollowOption struct {
	ClientName             string
	ClientId               int32
	ClientEpoch            int32
	SelfSignature          int32
	PathPrefix             string
	AdditionalPathPrefixes []string
	DirectoriesToWatch     []string
	StartTsNs              int64
	StopTsNs               int64
	EventErrorType         EventErrorType
	// LogFileReaderFn, when non-nil, enables metadata chunks mode:
	// the server sends log file chunk fids instead of streaming events,
	// and the client reads directly from volume servers.
	LogFileReaderFn LogFileReaderFn
	// OnIdleHeartbeat, when non-nil, opts in to idle heartbeats: while the
	// subscriber is caught up the server periodically sends an empty response
	// carrying the current time, and this is called with that timestamp. It is
	// a freshness signal only and does not advance StartTsNs, so the resume
	// checkpoint stays on the last real event.
	OnIdleHeartbeat func(tsNs int64)
	// GetResumeTsNs, when non-nil, supplies the reconnect position instead of
	// StartTsNs. It is read on every subscribe, so a callback can return the
	// durably processed watermark while StartTsNs keeps tracking positions the
	// stream has merely seen.
	GetResumeTsNs func() int64
	// Resubscribe, when closed, drops the stream with ErrResubscribe so the
	// caller's reconnect loop resubscribes from GetResumeTsNs and replays the
	// events still pinning the processed watermark. Target-side job failures
	// never surface on this stream, so without it a pinned event waits for an
	// unrelated source-stream reconnect (or a restart) to be replayed.
	Resubscribe <-chan struct{}

	// NamedSubscription, when non-empty, switches to the server-managed named
	// cursor: the filer persists the confirmed position under this name, every
	// connection carrying it resumes after the confirmed position, and only
	// one stream is active for the name at a time. StartTsNs is then only used
	// when the name is registered (or re-registered) for the first time.
	NamedSubscription string
	// Named cursor declarations, applied only when the name is first
	// registered. Zero values take the server defaults.
	SubscriptionTakeoverPolicy filer_pb.SubscriptionTakeoverPolicy
	SubscriptionAckGranularity filer_pb.SubscriptionAckGranularity
	SubscriptionAckTimeout     time.Duration
	SubscriptionTakeoverGrace  time.Duration
	// OnSubscriptionStart, when set, is called on every (re)connect with the
	// owner token and the confirmed position the stream resumed after.
	OnSubscriptionStart func(ownerInstance string, confirmedTsNs int64)
}

// ErrResubscribe ends a metadata follow when the consumer asked for the
// stream to drop so the reconnect replays what the resume watermark pins.
var ErrResubscribe = errors.New("resubscribe to replay events behind the failed offset")

type ProcessMetadataFunc func(resp *filer_pb.SubscribeMetadataResponse) error

func FollowMetadata(filerAddress ServerAddress, grpcDialOption grpc.DialOption, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := WithFilerClient(true, option.SelfSignature, filerAddress, grpcDialOption, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %w", err)
	}
	return err
}

func WithFilerClientFollowMetadata(filerClient filer_pb.FilerClient, option *MetadataFollowOption, processEventFn ProcessMetadataFunc) error {

	err := filerClient.WithFilerClient(true, makeSubscribeMetadataFunc(option, processEventFn))
	if err != nil {
		return fmt.Errorf("subscribing filer meta change: %w", err)
	}

	return nil
}

func makeSubscribeMetadataFunc(option *MetadataFollowOption, processEventFn ProcessMetadataFunc) func(client filer_pb.SeaweedFilerClient) error {
	return func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sinceNs := option.StartTsNs
		if option.GetResumeTsNs != nil {
			sinceNs = option.GetResumeTsNs()
		}
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName:                   option.ClientName,
			PathPrefix:                   option.PathPrefix,
			PathPrefixes:                 option.AdditionalPathPrefixes,
			Directories:                  option.DirectoriesToWatch,
			SinceNs:                      sinceNs,
			Signature:                    option.SelfSignature,
			ClientId:                     option.ClientId,
			ClientEpoch:                  option.ClientEpoch,
			UntilNs:                      option.StopTsNs,
			ClientSupportsBatching:       true,
			ClientSupportsMetadataChunks: option.LogFileReaderFn != nil,
			ClientSupportsIdleHeartbeat:  option.OnIdleHeartbeat != nil,
			SubscriptionName:             option.NamedSubscription,
			SubscriptionTakeoverPolicy:   option.SubscriptionTakeoverPolicy,
			SubscriptionAckGranularity:   option.SubscriptionAckGranularity,
			SubscriptionAckTimeoutNs:     int64(option.SubscriptionAckTimeout),
			SubscriptionTakeoverGraceNs:  int64(option.SubscriptionTakeoverGrace),
		})
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		// ackCursor confirms processed events of a named subscription. It is a
		// no-op for anonymous subscriptions. A failed ack is logged but never
		// fails processing: the unconfirmed range is simply redelivered later.
		var ownerInstance string
		ackCursor := func(timestamps []int64) {
			if option.NamedSubscription == "" || len(timestamps) == 0 {
				return
			}
			ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			resp, ackErr := client.AckMetadataSubscription(ackCtx, &filer_pb.AckMetadataSubscriptionRequest{
				SubscriptionName: option.NamedSubscription,
				TsNs:             timestamps,
			})
			if ackErr != nil {
				glog.V(1).Infof("ack named subscription %s at %v failed: %v", option.NamedSubscription, timestamps, ackErr)
				return
			}
			glog.V(4).Infof("acked named subscription %s through %d", option.NamedSubscription, resp.GetConfirmedTsNs())
		}

		if option.Resubscribe != nil {
			go func() {
				select {
				case <-option.Resubscribe:
					cancel()
				case <-ctx.Done():
				}
			}()
		}

		handleErr := func(resp *filer_pb.SubscribeMetadataResponse, err error) {
			switch option.EventErrorType {
			case TrivialOnError:
				glog.Errorf("process %v: %v", resp, err)
			case FatalOnError:
				glog.Fatalf("process %v: %v", resp, err)
			case RetryForeverOnError:
				waitTime := time.Second
				for ctx.Err() == nil {
					if err := processEventFn(resp); err == nil {
						break
					} else {
						glog.Errorf("process %v: %v", resp, err)
					}
					select {
					case <-ctx.Done():
					case <-time.After(waitTime):
					}
					if waitTime < util.RetryWaitTime {
						waitTime += waitTime / 2
					}
				}
			case DontLogError:
				// pass
			default:
				glog.Errorf("process %v: %v", resp, err)
			}
		}

		// handleOneEvent processes a single response, whether it arrived as the
		// batch envelope or inside resp.Events. Freshness signals carry a timestamp
		// but no entry: the idle heartbeat (nil EventNotification) and the
		// MaxUnsyncedEvents marker (empty one). Both route to OnIdleHeartbeat,
		// else the marker pins sync_offset to the stale processed watermark and a
		// nil heartbeat folded into a batch nil-derefs in the sync job path.
		handleOneEvent := func(resp *filer_pb.SubscribeMetadataResponse) {
			if resp.EventNotification == nil || filer_pb.IsEmpty(resp) {
				if resp.TsNs > 0 && option.OnIdleHeartbeat != nil {
					option.OnIdleHeartbeat(resp.TsNs)
				}
				// The marker advances the resume cursor past the filtered range; the
				// heartbeat leaves it put so a restart cannot outrun a straggler. A
				// consumer with a resume callback keeps its cursor in its processed
				// watermark, so it sees the marker instead.
				if resp.EventNotification != nil && resp.TsNs > 0 {
					if option.GetResumeTsNs != nil {
						if err := processEventFn(resp); err != nil {
							handleErr(resp, err)
						}
					} else {
						option.StartTsNs = resp.TsNs
					}
				}
				return
			}
			if err := processEventFn(resp); err != nil {
				handleErr(resp, err)
				// RetryForeverOnError only returns once the event was handled;
				// other modes leave it failed and the cursor stays behind it.
				if option.EventErrorType != RetryForeverOnError {
					return
				}
			}
			if option.GetResumeTsNs == nil {
				option.StartTsNs = resp.TsNs
			}
		}

		var pendingRefs []*filer_pb.LogFileChunkRef

		// drainPendingRefs reads whatever chunk refs have accumulated. The server
		// sends refs in their own responses, so they can only be read once
		// something tells us the run of refs has ended — either a normal event, or
		// the end of the stream. A bounded subscription (StopTsNs set, range
		// already in the past) may get nothing but refs and then EOF, so draining
		// only on the former silently returns no events at all.
		drainPendingRefs := func() error {
			if len(pendingRefs) == 0 || option.LogFileReaderFn == nil {
				return nil
			}
			readFromNs := option.StartTsNs
			if option.GetResumeTsNs != nil {
				// The resume cursor lives in the processed watermark, so a
				// resubscribed ref replay must not be filtered by positions the
				// previous stream had only seen.
				readFromNs = sinceNs
			}
			lastTs, readErr := ReadLogFileRefs(pendingRefs, option.LogFileReaderFn,
				readFromNs, option.StopTsNs,
				PathFilter{
					PathPrefix:             option.PathPrefix,
					AdditionalPathPrefixes: option.AdditionalPathPrefixes,
					DirectoriesToWatch:     option.DirectoriesToWatch,
				},
				processEventFn)
			if readErr != nil {
				return fmt.Errorf("%w: %w", ErrLogFileRead, readErr)
			}
			if lastTs > 0 && option.GetResumeTsNs == nil {
				option.StartTsNs = lastTs
			}
			pendingRefs = nil
			return nil
		}

		for {
			resp, listenErr := stream.Recv()
			if listenErr == io.EOF {
				return drainPendingRefs()
			}
			if listenErr != nil {
				select {
				case <-option.Resubscribe:
					return ErrResubscribe
				default:
				}
				return listenErr
			}

			// Accumulate log file chunk references (metadata chunks mode)
			if len(resp.LogFileRefs) > 0 {
				pendingRefs = append(pendingRefs, resp.LogFileRefs...)
				continue
			}

			// Process accumulated refs before handling normal events (transition point)
			if err := drainPendingRefs(); err != nil {
				return err
			}

			// The first response on a named subscription names the server-side
			// owner and the resume position; it is a control message, not an
			// event or heartbeat.
			if resp.SubscriptionOwnerInstance != "" {
				ownerInstance = resp.SubscriptionOwnerInstance
				glog.V(3).Infof("named subscription %s active as %s, resumed after %d",
					option.NamedSubscription, ownerInstance, resp.SubscriptionConfirmedTsNs)
				if option.OnSubscriptionStart != nil {
					option.OnSubscriptionStart(ownerInstance, resp.SubscriptionConfirmedTsNs)
				}
				continue
			}

			// Process the envelope event (top-level fields) and any batched tail.
			// The server folds a backlog into one response: the first event lives in
			// the top-level fields, the rest in resp.Events. Either slot can hold a
			// freshness signal, so both go through the same handler. Every
			// successfully handled non-heartbeat message of a named subscription is
			// confirmed back, so reconnects resume behind the processed watermark.
			var ackTs []int64
			handleAndCollect := func(r *filer_pb.SubscribeMetadataResponse) {
				handleOneEvent(r)
				if option.NamedSubscription != "" && r.EventNotification != nil && r.TsNs > 0 {
					ackTs = append(ackTs, r.TsNs)
				}
			}
			handleAndCollect(resp)
			for _, batchedEvent := range resp.Events {
				handleAndCollect(batchedEvent)
			}
			if len(ackTs) > 0 {
				ackCursor(ackTs)
			}
		}
	}
}

func AddOffsetFunc(processEventFn ProcessMetadataFunc, offsetInterval time.Duration, offsetFunc func(counter int64, offset int64) error) ProcessMetadataFunc {
	var counter int64
	var lastWriteTime = time.Now()
	return func(resp *filer_pb.SubscribeMetadataResponse) error {
		if err := processEventFn(resp); err != nil {
			return err
		}
		counter++
		if lastWriteTime.Add(offsetInterval).Before(time.Now()) {
			lastWriteTime = time.Now()
			if err := offsetFunc(counter, resp.TsNs); err != nil {
				return err
			}
			counter = 0
		}
		return nil
	}

}
