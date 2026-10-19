package weed_server

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// AckMetadataSubscription persists a named subscription's confirmed position.
// The position is owned by the whole cluster: while a stream on another filer
// is actively serving the name, that filer holds the outstanding-event
// bookkeeping, so the ack is forwarded there.
func (fs *FilerServer) AckMetadataSubscription(ctx context.Context, req *filer_pb.AckMetadataSubscriptionRequest) (*filer_pb.AckMetadataSubscriptionResponse, error) {
	mgr, err := fs.getNamedCursorManager()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	confirmed, err := mgr.ack(ctx, req.SubscriptionName, req.TsNs)
	if err != nil {
		var forward errAckAtOtherOwner
		if errors.As(err, &forward) {
			addr := pb.ServerAddress(forward.ownerAddress)
			var resp *filer_pb.AckMetadataSubscriptionResponse
			forwardErr := pb.WithFilerClient(false, 0, addr, fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				var callErr error
				resp, callErr = client.AckMetadataSubscription(ctx, req)
				return callErr
			})
			if forwardErr != nil {
				return nil, status.Errorf(codes.Unavailable, "forward ack for %s to owner %s: %v", req.SubscriptionName, addr, forwardErr)
			}
			return resp, nil
		}
		// A confirmation behind the confirmed position is a client error;
		// anything else (unknown name, store trouble) must not silently move
		// the position either.
		code := codes.Internal
		if strings.Contains(err.Error(), "cannot move backwards") || strings.Contains(err.Error(), "not registered") {
			code = codes.FailedPrecondition
		}
		return nil, status.Error(code, err.Error())
	}
	return &filer_pb.AckMetadataSubscriptionResponse{ConfirmedTsNs: confirmed}, nil
}

// ListNamedMetadataSubscriptions returns the persisted named cursors and
// whether each currently has an active owner.
func (fs *FilerServer) ListNamedMetadataSubscriptions(ctx context.Context, req *filer_pb.ListNamedMetadataSubscriptionsRequest) (*filer_pb.ListNamedMetadataSubscriptionsResponse, error) {
	mgr, err := fs.getNamedCursorManager()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	subs, err := mgr.list(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list named subscriptions: %v", err)
	}
	return &filer_pb.ListNamedMetadataSubscriptionsResponse{Subscriptions: subs}, nil
}

// ListMetadataSubscriptionDispositions returns the disposal records left when
// named cursors were reclaimed (newest first).
func (fs *FilerServer) ListMetadataSubscriptionDispositions(ctx context.Context, req *filer_pb.ListMetadataSubscriptionDispositionsRequest) (*filer_pb.ListMetadataSubscriptionDispositionsResponse, error) {
	mgr, err := fs.getNamedCursorManager()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	records, err := mgr.dispositions(ctx, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list subscription dispositions: %v", err)
	}
	return &filer_pb.ListMetadataSubscriptionDispositionsResponse{Dispositions: records}, nil
}
