package batchjob

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func sampleJob() (*Job, []Item) {
	items := make([]Item, 205) // 3 chunks (100/100/5)
	for i := range items {
		items[i] = Item{Index: i, Key: fmt.Sprintf("dir/obj-%03d", i)}
	}
	now := time.Now()
	return &Job{
		ID:           NewJobID(),
		Bucket:       "bucket-a",
		Operation:    OpDelete,
		Concurrency:  4,
		Rate:         0,
		RetentionSec: DefaultRetentionSeconds,
		State:        StateRunning,
		TotalItems:   len(items),
		CreatedAt:    now,
		StartedAt:    now,
		Creator:      Creator{Name: "alice"},
	}, items
}

func runStoreContract(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	job, items := sampleJob()

	// Create + readback.
	if err := store.CreateJob(ctx, job, items); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	got, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Version != 1 || got.TotalItems != 205 || got.Bucket != "bucket-a" {
		t.Fatalf("job roundtrip mismatch: %+v", got)
	}
	// Duplicate create conflicts.
	dup := *job
	if err := store.CreateJob(ctx, &dup, items); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate CreateJob = %v, want ErrConflict", err)
	}
	// Manifests read back in order and full.
	var total int
	for c := 0; c < ChunkCount(len(items)); c++ {
		chunk, err := store.ReadManifest(ctx, job.ID, c)
		if err != nil {
			t.Fatalf("ReadManifest(%d): %v", c, err)
		}
		for i, item := range chunk {
			if item.Index != c*ChunkSize+i || item.Key != items[c*ChunkSize+i].Key {
				t.Fatalf("manifest %d item %d mismatch", c, i)
			}
		}
		total += len(chunk)
	}
	if total != len(items) {
		t.Fatalf("manifest total = %d, want %d", total, len(items))
	}

	// CAS update: stale version conflicts; winner increments.
	stale := *got
	stale.State = StatePaused
	if err := store.SaveJob(ctx, &stale); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	if stale.Version != 2 {
		t.Fatalf("version after save = %d, want 2", stale.Version)
	}
	got2 := *got // version 1
	got2.State = StateRunning
	if err := store.SaveJob(ctx, &got2); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale SaveJob = %v, want ErrConflict", err)
	}

	// Chunk results: checkpoint then complete publish, overwrite keeps read atomic.
	cr := &ChunkResult{Chunk: 1, Results: []ItemResult{
		{Index: 100, State: ItemSucceeded, FinishedAt: time.Now().Unix()},
	}}
	if err := store.WriteChunkResult(ctx, job.ID, cr); err != nil {
		t.Fatalf("WriteChunkResult: %v", err)
	}
	back, err := store.ReadChunkResult(ctx, job.ID, 1)
	if err != nil || !back.Results[0].State.IsSuccess() {
		t.Fatalf("ReadChunkResult: %+v err=%v", back, err)
	}
	if _, err := store.ReadChunkResult(ctx, job.ID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing chunk = %v, want ErrNotFound", err)
	}
	cr.Complete = true
	if err := store.WriteChunkResult(ctx, job.ID, cr); err != nil {
		t.Fatalf("publish complete: %v", err)
	}
	chunks, complete, err := store.ListResultChunks(ctx, job.ID)
	if err != nil || len(chunks) != 1 || chunks[0] != 1 || !complete[1] {
		t.Fatalf("ListResultChunks = %v %v err=%v", chunks, complete, err)
	}

	// Lease fencing.
	now := time.Now()
	f1, err := store.AcquireLease(ctx, job.ID, "A", now)
	if err != nil || f1 != 1 {
		t.Fatalf("A AcquireLease fence=%d err=%v", f1, err)
	}
	if _, err := store.AcquireLease(ctx, job.ID, "B", now.Add(10*time.Second)); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("B AcquireLease while held = %v, want ErrLeaseHeld", err)
	}
	if err := store.RenewLease(ctx, job.ID, "B", 1, now.Add(10*time.Second)); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("wrong-owner renew = %v, want ErrLeaseHeld", err)
	}
	if err := store.RenewLease(ctx, job.ID, "A", 1, now.Add(10*time.Second)); err != nil {
		t.Fatalf("A RenewLease: %v", err)
	}
	// Heartbeats stop; after TTL B steals with a new fence.
	f2, err := store.AcquireLease(ctx, job.ID, "B", now.Add(40*time.Second))
	if err != nil || f2 != 2 {
		t.Fatalf("B steal fence=%d err=%v", f2, err)
	}
	if err := store.RenewLease(ctx, job.ID, "A", 1, now.Add(40*time.Second)); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("stale A renew after steal = %v, want ErrLeaseHeld", err)
	}
	if err := store.ReleaseLease(ctx, job.ID, "B", f2); err != nil {
		t.Fatalf("B ReleaseLease: %v", err)
	}
	// Released lease is acquirable immediately (restart/resume).
	f3, err := store.AcquireLease(ctx, job.ID, "A", now.Add(41*time.Second))
	if err != nil || f3 != 4 {
		t.Fatalf("A reacquire after release fence=%d err=%v (want 4: steal + release + reacquire)", f3, err)
	}
	if err := store.ReleaseLease(ctx, job.ID, "A", f3); err != nil {
		t.Fatalf("A release: %v", err)
	}

	// Listing surfaces the job.
	ids, err := store.ListJobIDs(ctx)
	if err != nil {
		t.Fatalf("ListJobIDs: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == job.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("job %s not listed: %v", job.ID, ids)
	}

	// Delete removes everything.
	if err := store.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := store.GetJob(ctx, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetJob after delete = %v, want ErrNotFound", err)
	}
	if err := store.DeleteJob(ctx, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double delete = %v, want ErrNotFound", err)
	}
}

func (s ItemState) IsSuccess() bool { return s == ItemSucceeded }

func TestMemStoreContract(t *testing.T) {
	runStoreContract(t, newMemStore())
}

func TestFilerStoreContract(t *testing.T) {
	client, _ := startGrpcMemFiler(t)
	store := NewFilerStore("/etc/s3/batch-jobs-test", func(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
		return fn(client)
	})
	runStoreContract(t, store)
}

// TestFilerStoreLeaseMultiRacer verifies mutual exclusion when two gateways
// race for an unowned job: exactly one wins with fence 1.
func TestFilerStoreLeaseRace(t *testing.T) {
	client, _ := startGrpcMemFiler(t)
	store := NewFilerStore("/etc/s3/batch-jobs-race", func(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
		return fn(client)
	}).(*filerStore)
	ctx := context.Background()
	job, items := sampleJob()
	if err := store.CreateJob(ctx, job, items); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	const racers = 16
	type result struct {
		fence int64
		err   error
	}
	resCh := make(chan result, racers)
	now := time.Now()
	for i := 0; i < racers; i++ {
		go func(i int) {
			f, err := store.AcquireLease(ctx, job.ID, fmt.Sprintf("gw-%d", i), now)
			resCh <- result{f, err}
		}(i)
	}
	winners := 0
	for i := 0; i < racers; i++ {
		r := <-resCh
		if r.err == nil && r.fence == 1 {
			winners++
		} else if !errors.Is(r.err, ErrLeaseHeld) {
			t.Fatalf("unexpected racer outcome: %+v", r)
		}
	}
	if winners != 1 {
		t.Fatalf("lease winners = %d, want exactly 1", winners)
	}
}
