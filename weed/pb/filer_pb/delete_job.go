package filer_pb

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// DeleteJobIdTrailerKey carries the recursive-deletion job id on the gRPC
// response of a DeleteEntry call that was registered as (or attached to) an
// asynchronous job. The RPC resolves when the job reaches a terminal state, so
// the trailer is primarily observability; dedicated status/cancel access uses
// the reserved KV keys below. It is a trailer (not a proto field) by design:
// the wire schema stays unchanged.
const DeleteJobIdTrailerKey = "seaweedfs-delete-job-id"

// DeleteJobKvPrefix and friends name the reserved KV namespace for recursive
// deletion jobs. Keep these in lockstep with filer.KvDeleteJob*.
const (
	DeleteJobKvPrefix        = "filer.deleteJob.v1."
	DeleteJobKvJobPrefix     = DeleteJobKvPrefix + "job."
	DeleteJobKvCancelPrefix  = DeleteJobKvPrefix + "cancel."
)

// DeleteEntryJobID extracts the job id from a response trailer.
func DeleteEntryJobID(trailer metadata.MD) string {
	values := trailer.Get(DeleteJobIdTrailerKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// CancelDeleteJob requests cancellation through the reserved KV control key,
// which the owning filer intercepts instead of writing raw.
func CancelDeleteJob(ctx context.Context, client SeaweedFilerClient, jobId string) error {
	_, err := client.KvPut(ctx, &KvPutRequest{
		Key:   []byte(DeleteJobKvCancelPrefix + jobId),
		Value: []byte("cancel"),
	})
	return err
}

// GetDeleteJob reads the JSON job descriptor through the reserved KV key.
func GetDeleteJob(ctx context.Context, client SeaweedFilerClient, jobId string) ([]byte, error) {
	resp, err := client.KvGet(ctx, &KvGetRequest{Key: []byte(DeleteJobKvJobPrefix + jobId)})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, &KvError{msg: resp.Error}
	}
	return resp.Value, nil
}

// KvError is a response-carried KV error.
type KvError struct{ msg string }

func (e *KvError) Error() string { return e.msg }

// IsReservedDeleteJobKey reports a key inside the recursive-delete job KV
// namespace. Raw client writes to the namespace are refused by the filer.
func IsReservedDeleteJobKey(key string) bool {
	return strings.HasPrefix(key, DeleteJobKvPrefix)
}
