package weed_server

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) KvGet(ctx context.Context, req *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {

	value, err := fs.filer.Store.KvGet(ctx, req.Key)
	if err == filer.ErrKvNotFound {
		return &filer_pb.KvGetResponse{}, nil
	}

	if err != nil {
		return &filer_pb.KvGetResponse{Error: err.Error()}, nil
	}

	return &filer_pb.KvGetResponse{
		Value: value,
	}, nil

}

// KvPut sets the key~value. if empty value, delete the kv entry.
//
// The recursive-delete-job namespace is reserved: a put to a
// filer.deleteJob.v1.cancel.<id> control key requests job cancellation (the
// request body is ignored), and every other write under the prefix is refused
// so clients cannot forge or corrupt job records.
func (fs *FilerServer) KvPut(ctx context.Context, req *filer_pb.KvPutRequest) (*filer_pb.KvPutResponse, error) {

	if strings.HasPrefix(string(req.Key), filer.KvDeleteJobCancelPrefix) {
		jobId := strings.TrimPrefix(string(req.Key), filer.KvDeleteJobCancelPrefix)
		if fs.filer.DeleteJobs == nil {
			return &filer_pb.KvPutResponse{Error: filer.ErrKvNotFound.Error()}, nil
		}
		if _, err := fs.filer.DeleteJobs.Cancel(ctx, jobId); err != nil {
			return &filer_pb.KvPutResponse{Error: err.Error()}, nil
		}
		return &filer_pb.KvPutResponse{}, nil
	}
	if strings.HasPrefix(string(req.Key), filer.KvDeleteJobIndex) ||
		strings.HasPrefix(string(req.Key), filer.KvDeleteJobJobPrefix) {
		return &filer_pb.KvPutResponse{Error: "reserved key: " + string(req.Key)}, nil
	}

	if len(req.Value) == 0 {
		if err := fs.filer.Store.KvDelete(ctx, req.Key); err != nil {
			return &filer_pb.KvPutResponse{Error: err.Error()}, nil
		}
		return &filer_pb.KvPutResponse{}, nil
	}

	err := fs.filer.Store.KvPut(ctx, req.Key, req.Value)
	if err != nil {
		return &filer_pb.KvPutResponse{Error: err.Error()}, nil
	}

	return &filer_pb.KvPutResponse{}, nil
}
