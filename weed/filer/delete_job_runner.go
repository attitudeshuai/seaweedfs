package filer

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The deletion-job worker advances a registered job in bounded batches. One
// batch lists at most one page of one frontier directory and removes at most
// cfg.BatchSize entries (files plus emptied directories); every other write
// can land between batches, and cancellation is observed only at batch/entry
// boundaries so nothing a batch committed is rolled back.

// deleteJobBypassKey marks the worker's own context so its per-entry metadata
// deletions are not refused by the deleting-prefix gate. Nothing outside the
// worker (including peer-replayed mutations) sets it.
type deleteJobBypassKey struct{}

func WithDeleteJobBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, deleteJobBypassKey{}, true)
}

func deleteJobBypassed(ctx context.Context) bool {
	bypassed, _ := ctx.Value(deleteJobBypassKey{}).(bool)
	return bypassed
}

// markDeletingRoot stamps the root directory entry with the active job
// marker. It is store-visible (survives restarts, visible to every filer
// sharing the store) and serves as the second line of the write gate.
func (f *Filer) markDeletingRoot(ctx context.Context, rootEntry *Entry, jobId string) error {
	if rootEntry.Extended == nil {
		rootEntry.Extended = make(map[string][]byte)
	}
	if string(rootEntry.Extended[ExtDeletingJobKey]) == jobId {
		return nil
	}
	rootEntry.Extended[ExtDeletingJobKey] = []byte(jobId)
	if err := f.Store.UpdateEntry(ctx, rootEntry); err != nil {
		return fmt.Errorf("mark deleting root %s: %w", rootEntry.FullPath, err)
	}
	return nil
}

// clearDeletingRootMarker removes the job marker from the root entry. A
// missing root or marker is fine: completion deletes the entry outright, and
// cancellation races only with an external delete.
func (f *Filer) clearDeletingRootMarker(ctx context.Context, rootPath util.FullPath) {
	entry, err := f.FindEntry(ctx, rootPath)
	if err != nil || entry == nil {
		return
	}
	if _, ok := entry.Extended[ExtDeletingJobKey]; !ok {
		return
	}
	delete(entry.Extended, ExtDeletingJobKey)
	if err := f.Store.UpdateEntry(ctx, entry); err != nil {
		glog.V(1).InfofCtx(ctx, "clear deleting marker %s: %v", rootPath, err)
	}
}

// executeDeleteJobBatch advances rec by one bounded batch and persists it at
// the end (including on failure, so cursors and counters survive a crash).
// progressed reports whether any deletion happened; a terminal rec.State
// (complete/cancelled/failed) means the worker must not schedule the job
// again.
func (f *Filer) executeDeleteJobBatch(ctx context.Context, rec *DeleteJobRecord, cfg AsyncDeleteConfig) (progressed bool, err error) {
	ctx = WithDeleteJobBypass(ctx)
	if rec.Options.KeepRemoteObject {
		ctx = WithKeepRemoteObject(ctx)
	}
	rootPath := util.FullPath(rec.Path)

	// Cancellation is applied at the batch boundary.
	if rec.State == DeleteJobStateCancelling {
		f.finishJobCancellation(ctx, rec)
		return false, nil
	}

	rootEntry, findErr := f.FindEntry(ctx, rootPath)
	if findErr == filer_pb.ErrNotFound {
		// The root vanished (external delete while the job owned it): nothing
		// is left to delete, so close the job out instead of parking it.
		glog.V(1).InfofCtx(ctx, "delete job %s: root %s already gone, completing", rec.JobId, rootPath)
		rec.State = DeleteJobStateComplete
		rec.FinishedAtMs = millis(time.Now())
		rec.Frontier = nil
		if persistErr := persistDeleteJob(ctx, f.Store, rec); persistErr != nil {
			return false, persistErr
		}
		return false, nil
	}
	if findErr != nil {
		return f.failJobBatch(ctx, rec, cfg, fmt.Errorf("find %s: %w", rootPath, findErr))
	}
	if !rootEntry.IsDirectory() {
		return f.failJobBatch(ctx, rec, cfg, fmt.Errorf("delete job %s: root %s is no longer a directory", rec.JobId, rootPath))
	}

	isBucket := f.IsBucket(rootEntry)
	collectionName := ""
	if isBucket {
		collectionName = f.bucketCollection(ctx, rootEntry.Name())
	}
	dropsCollection := isBucket && collectionName != ""

	// First batch for the record: become running, tombstone the root (same
	// ancestor-tombstone the synchronous walk takes before listing), and seed
	// the DFS frontier. The root marker was stamped at registration.
	//
	// A bucket on a store that drops a whole bucket subtree in one call takes
	// the same fast path the synchronous delete does (chunks leave with the
	// private collection, or chunk deletion was not requested): seed the root
	// frontier already exhausted so the root finalization's bulk drop is the
	// only metadata operation.
	if rec.StartedAtMs == 0 {
		rec.State = DeleteJobStateRunning
		rec.StartedAtMs = millis(time.Now())
		f.noteRemoteDeletion(rootPath, true, time.Now().UnixNano())
		rootFrontier := DeleteJobFrontier{Dir: rec.Path}
		if isBucket && f.Store.CanDropWholeBucket() && (dropsCollection || !rec.Options.ShouldDeleteChunks) {
			rootFrontier.Exhausted = true
		}
		rec.Frontier = []DeleteJobFrontier{rootFrontier}
		progressed = true
	}

	// Mirrors DeleteEntryMetaAndData: a dropped private collection carries the
	// bucket chunks, so per-entry chunk deletion is skipped.
	shouldDeleteChunks := rec.Options.ShouldDeleteChunks && !dropsCollection
	onHardLinkIdsFn := OnHardLinkIdsFunc(func(hardLinkIds []HardLinkId) error {
		if rec.Options.ShouldDeleteChunks {
			f.maybeDeleteHardLinks(ctx, hardLinkIds)
		}
		return nil
	})

	budget := cfg.BatchSize
	if budget <= 0 {
		budget = defaultAsyncDeleteBatchSize
	}

	var batchErr error
	for budget > 0 && len(rec.Frontier) > 0 && rec.State != DeleteJobStateCancelling {
		topIdx := len(rec.Frontier) - 1
		top := &rec.Frontier[topIdx]

		if !top.Exhausted {
			pageLimit := int64(PaginationSize)
			if int64(budget) < pageLimit {
				pageLimit = int64(budget)
			}
			page, hasMore, listErr := f.ListDirectoryEntries(ctx, util.FullPath(top.Dir), top.LastFileName, false, pageLimit, "", "", "")
			if listErr != nil {
				batchErr = fmt.Errorf("list %s: %w", top.Dir, listErr)
				break
			}
			for _, child := range page {
				if rec.State == DeleteJobStateCancelling {
					break
				}
				f.noteRemoteDeletion(child.FullPath, child.IsDirectory(), time.Now().UnixNano())
				if child.IsDirectory() {
					// Dive into the subtree before deleting this directory
					// itself; the cursor advances past it so it is not listed
					// again when control returns to this level.
					rec.Frontier = append(rec.Frontier, DeleteJobFrontier{Dir: string(child.FullPath)})
					top.LastFileName = child.Name()
					continue
				}
				removed, childErr := f.deleteJobFile(ctx, rec, child, shouldDeleteChunks, onHardLinkIdsFn)
				if childErr != nil {
					// The cursor is not advanced past this child, so a retry
					// re-lists and reprocesses it (at-least-once events).
					batchErr = childErr
					break
				}
				top.LastFileName = child.Name()
				if removed {
					rec.Deleted++
					budget--
					progressed = true
				}
			}
			if batchErr != nil {
				break
			}
			if !hasMore {
				top.Exhausted = true
			}
			if rec.State == DeleteJobStateCancelling {
				break
			}
			// A full page may leave this directory unexhausted; loop on: the
			// stacked subdirectories are processed first (LIFO), then the
			// next page of this directory follows its cursor.
			continue
		}

		// This directory's subtree is gone: remove the directory entry. The
		// root is the last frontier entry and gets the full bucket finalize.
		isRoot := topIdx == 0 && top.Dir == rec.Path
		dirEntry, dirFindErr := f.FindEntry(ctx, util.FullPath(top.Dir))
		if dirFindErr != nil && dirFindErr != filer_pb.ErrNotFound {
			batchErr = fmt.Errorf("find emptied dir %s: %w", top.Dir, dirFindErr)
			break
		}
		if isRoot {
			if delErr := f.finishJobRoot(ctx, rec, rootEntry, collectionName, shouldDeleteChunks); delErr != nil {
				batchErr = delErr
				break
			}
			// The bulk-drop fast path removes the subtree without per-entry
			// counts; report the (lower-bound) estimate as removed so
			// remainingEstimate reads zero.
			if rec.Deleted < rec.InitialEstimate {
				rec.Deleted = rec.InitialEstimate
			}
			rec.Frontier = nil
			rec.State = DeleteJobStateComplete
			rec.FinishedAtMs = millis(time.Now())
			progressed = true
			break
		}
		if dirEntry != nil {
			if sweepErr := f.Store.DeleteFolderChildren(ctx, dirEntry.FullPath); sweepErr != nil {
				batchErr = fmt.Errorf("sweep dir %s: %w", top.Dir, sweepErr)
				break
			}
			f.NotifyUpdateEvent(ctx, deleteEventEntry(ctx, dirEntry), nil, shouldDeleteChunks, false, nil)
			if delErr := f.Store.DeleteEntry(ctx, dirEntry.FullPath); delErr != nil {
				batchErr = fmt.Errorf("delete dir %s: %w", top.Dir, delErr)
				break
			}
			rec.Deleted++
			budget--
			progressed = true
		}
		rec.Frontier = rec.Frontier[:topIdx]
	}

	if rec.State == DeleteJobStateCancelling {
		f.finishJobCancellation(ctx, rec)
		return progressed, nil
	}

	if batchErr != nil {
		return progressed, f.failJobBatch(ctx, rec, cfg, batchErr)
	}

	// A batch that moved without an error clears the consecutive-failure run.
	if rec.RetryCount != 0 || rec.Error != "" {
		rec.RetryCount = 0
		rec.Error = ""
	}

	if persistErr := persistDeleteJob(ctx, f.Store, rec); persistErr != nil {
		return progressed, persistErr
	}
	return progressed, nil
}

// deleteJobFile deletes one file child exactly as the synchronous walk would:
// remote object, delete event, hardlink/chunk handling, then the metadata row.
// removed reports whether the row was deleted in this call.
func (f *Filer) deleteJobFile(ctx context.Context, rec *DeleteJobRecord, child *Entry, shouldDeleteChunks bool, onHardLinkIdsFn OnHardLinkIdsFunc) (removed bool, err error) {
	var fileChunks []*filer_pb.FileChunk
	remoteErr, hardlinkErr := f.deleteFileChildSideEffects(ctx, child, &fileChunks, rec.Options.IgnoreRecursiveError, shouldDeleteChunks, false, onHardLinkIdsFn)
	if remoteErr != nil {
		return false, fmt.Errorf("remote delete %s: %w", child.FullPath, remoteErr)
	}
	if hardlinkErr != nil {
		return false, fmt.Errorf("hardlink delete %s: %w", child.FullPath, hardlinkErr)
	}
	// Match the storage rule of the file's own path for chunk-deletion
	// disabling, not the job root's rule.
	f.DeleteChunks(ctx, child.FullPath, fileChunks)
	if delErr := f.Store.DeleteEntry(ctx, child.FullPath); delErr != nil {
		return false, fmt.Errorf("delete file %s: %w", child.FullPath, delErr)
	}
	return true, nil
}

// finishJobRoot performs the synchronous tail for the emptied root directory:
// the children sweep, the directory delete event, the root entry deletion
// (remote object + tombstone + row), and the bucket collection/metrics
// finalize.
func (f *Filer) finishJobRoot(ctx context.Context, rec *DeleteJobRecord, rootEntry *Entry, collectionName string, effectiveShouldDeleteChunks bool) error {
	if sweepErr := f.Store.DeleteFolderChildren(ctx, rootEntry.FullPath); sweepErr != nil {
		return fmt.Errorf("sweep root %s: %w", rootEntry.FullPath, sweepErr)
	}
	f.NotifyUpdateEvent(ctx, deleteEventEntry(ctx, rootEntry), nil, effectiveShouldDeleteChunks, false, nil)
	// doDeleteEntryMetaAndData uses the request's chunk flag (the collection
	// drop is handled separately by finalizeBucketDeletion), mirroring
	// DeleteEntryMetaAndData.
	if delErr := f.doDeleteEntryMetaAndData(ctx, rootEntry, rec.Options.ShouldDeleteChunks, false, nil); delErr != nil {
		return fmt.Errorf("delete root %s: %w", rootEntry.FullPath, delErr)
	}
	if f.IsBucket(rootEntry) {
		f.finalizeBucketDeletion(ctx, rootEntry, collectionName)
	}
	return nil
}

// failJobBatch records a batch failure with the declared retry policy:
// consecutive failures up to retry_max are retried by the worker loop; past
// it the job enters failed, keeping the prefix gated and exposing the error
// to status queries. A successful batch resets the counter.
func (f *Filer) failJobBatch(ctx context.Context, rec *DeleteJobRecord, cfg AsyncDeleteConfig, batchErr error) error {
	rec.RetryCount++
	if rec.RetryCount > cfg.RetryMax {
		rec.State = DeleteJobStateFailed
		rec.FinishedAtMs = millis(time.Now())
		rec.Error = batchErr.Error()
		glog.V(0).InfofCtx(ctx, "delete job %s on %s failed after %d retries: %v", rec.JobId, rec.Path, rec.RetryCount, batchErr)
	} else {
		rec.Error = batchErr.Error()
		glog.V(1).InfofCtx(ctx, "delete job %s on %s batch attempt %d/%d failed: %v", rec.JobId, rec.Path, rec.RetryCount, cfg.RetryMax, batchErr)
	}
	if persistErr := persistDeleteJob(ctx, f.Store, rec); persistErr != nil {
		return persistErr
	}
	return batchErr
}

// finishJobCancellation releases everything the job held: the prefix marker
// goes away and the state becomes cancelled. Already-deleted entries stay
// deleted; the root and its surviving children remain in place.
func (f *Filer) finishJobCancellation(ctx context.Context, rec *DeleteJobRecord) {
	f.clearDeletingRootMarker(ctx, util.FullPath(rec.Path))
	rec.State = DeleteJobStateCancelled
	rec.FinishedAtMs = millis(time.Now())
	rec.Error = ""
	if err := persistDeleteJob(ctx, f.Store, rec); err != nil {
		glog.V(0).InfofCtx(ctx, "persist cancelled delete job %s: %v", rec.JobId, err)
	}
}

// resetJobForRetry clears the failed/finished bookkeeping so a retry (explicit
// action or a repeated delete request) runs the surviving remainder again.
func resetJobForRetry(rec *DeleteJobRecord) {
	rec.State = DeleteJobStateQueued
	rec.RetryCount = 0
	rec.Error = ""
	rec.FinishedAtMs = 0
	rec.StartedAtMs = 0
	rec.Owner = ""
	rec.LeaseUntilMs = 0
}
