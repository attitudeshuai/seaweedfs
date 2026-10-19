package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MaybeStartDeleteJob decides whether a local recursive directory delete must
// run as a registered, batched job. It returns (job, true, nil) when a job was
// registered or attached, and (nil, false, nil) when the caller must take the
// original synchronous path -- anything that is not a recursive local delete
// of an existing directory (single files, non-recursive deletes, sub-threshold
// directories, a disabled feature, the ifNotModifiedAfter no-op, or a path the
// store cannot find). A store error is returned rather than guessed at.
func (f *Filer) MaybeStartDeleteJob(ctx context.Context, p util.FullPath, opts DeleteJobOptions) (*DeleteJobRecord, bool, error) {
	if f.DeleteJobs == nil || !f.DeleteJobs.cfg.Enabled {
		return nil, false, nil
	}
	if p == "/" {
		return nil, false, nil
	}
	entry, findErr := f.FindEntry(ctx, p)
	if findErr == filer_pb.ErrNotFound {
		return nil, false, nil
	}
	if findErr != nil {
		return nil, false, findErr
	}
	if !entry.IsDirectory() {
		return nil, false, nil
	}
	if opts.IfNotModifiedAfter > 0 && entry.Attr.Mtime.Unix() > opts.IfNotModifiedAfter {
		// Same no-op as the synchronous path.
		return nil, false, nil
	}
	return f.DeleteJobs.RegisterOrAttach(ctx, entry, opts)
}

// checkDeletingJobWrite is the write gate shared by CreateEntry and
// UpdateEntry; it is nil-safe for Filer literals and transparent to the
// worker's own context.
func (f *Filer) checkDeletingJobWrite(ctx context.Context, p util.FullPath) error {
	if f.DeleteJobs == nil {
		return nil
	}
	return f.DeleteJobs.CheckWrite(ctx, p)
}

// StartDeleteJobs starts background job scheduling. Called after the store
// and ring membership are ready (see filer server bootstrap).
func (f *Filer) StartDeleteJobs(ctx context.Context) error {
	if f.DeleteJobs == nil {
		return nil
	}
	return f.DeleteJobs.Start(ctx)
}
