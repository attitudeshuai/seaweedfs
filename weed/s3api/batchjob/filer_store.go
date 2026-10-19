package batchjob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FilerInvoker runs fn against a filer client, matching
// S3ApiServer.WithFilerClient's shape so the gateway wires it directly.
type FilerInvoker func(readOnly bool, fn func(client filer_pb.SeaweedFilerClient) error) error

// NewFilerStore returns a Store backed by filer metadata. All job state is
// kept as small inline JSON entries; no volume data is involved.
func NewFilerStore(root string, invoke FilerInvoker) Store {
	if root == "" {
		root = DefaultJobsRoot
	}
	return &filerStore{root: root, invoke: invoke}
}

type filerStore struct {
	root   string
	invoke FilerInvoker
}

func (s *filerStore) withClient(ctx context.Context, readOnly bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return s.invoke(readOnly, func(client filer_pb.SeaweedFilerClient) error {
		return fn(client)
	})
}

// ensureDir creates a directory entry, tolerating an existing entry. It walks
// one level only; callers build paths bottom-up.
func (s *filerStore) ensureDir(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string) error {
	now := time.Now().Unix()
	parent, name := pathSplit(dir)
	resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: parent,
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: now, Crtime: now},
		},
	})
	if err != nil {
		return fmt.Errorf("ensure dir %s: %w", dir, err)
	}
	if resp.ErrorCode != filer_pb.FilerError_OK && resp.ErrorCode != filer_pb.FilerError_ENTRY_ALREADY_EXISTS {
		return fmt.Errorf("ensure dir %s: %s", dir, resp.Error)
	}
	return nil
}

// ancestorDirs returns root and every ancestor above it (excluding "/"),
// ordered top-down so directories can be created idempotently in order.
func ancestorDirs(root string) []string {
	var dirs []string
	for p := root; p != "/" && p != "." && p != ""; {
		dirs = append(dirs, p)
		idx := strings.LastIndex(p, "/")
		if idx <= 0 {
			break
		}
		p = p[:idx]
	}
	for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
		dirs[i], dirs[j] = dirs[j], dirs[i]
	}
	return dirs
}

func (s *filerStore) ensureJobDirs(ctx context.Context, client filer_pb.SeaweedFilerClient, jobID string) error {
	// Top-down root ancestry, then per-job dirs; every create is idempotent.
	for _, dir := range ancestorDirs(s.root) {
		if err := s.ensureDir(ctx, client, dir); err != nil {
			return err
		}
	}
	for _, dir := range []string{
		jobDirPath(s.root, jobID),
		manifestDirPath(s.root, jobID),
		resultsDirPath(s.root, jobID),
	} {
		if err := s.ensureDir(ctx, client, dir); err != nil {
			return err
		}
	}
	return nil
}

// createFileExclusive creates a small file; maps ENTRY_ALREADY_EXISTS.
func (s *filerStore) createFileExclusive(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string, content []byte, extended map[string][]byte) error {
	resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		OExcl:     true,
		Entry: &filer_pb.Entry{
			Name:       name,
			Content:    content,
			Extended:   extended,
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(content))},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return ErrConflict
		}
		return err
	}
	if resp.ErrorCode == filer_pb.FilerError_ENTRY_ALREADY_EXISTS {
		return ErrConflict
	}
	if resp.ErrorCode != filer_pb.FilerError_OK {
		return fmt.Errorf("create %s/%s: %s", dir, name, resp.Error)
	}
	return nil
}

// updateCAS writes an entry with an ExpectedExtended precondition.
func (s *filerStore) updateCAS(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string, content []byte, expected, extended map[string][]byte) error {
	if extended == nil {
		extended = map[string][]byte{}
	}
	_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory:       dir,
		ExpectedExtended: expected,
		Entry: &filer_pb.Entry{
			Name:       name,
			Content:    content,
			Extended:   extended,
			Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(content))},
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.AlreadyExists {
			return ErrConflict
		}
		return err
	}
	return nil
}

func (s *filerStore) atomicWrite(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string, content []byte, extended map[string][]byte) error {
	tmp := tempName(name)
	if err := s.createFileExclusive(ctx, client, dir, tmp, content, extended); err != nil {
		return err
	}
	_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: dir,
		OldName:      tmp,
		NewDirectory: dir,
		NewName:      name,
	})
	if err != nil {
		// Best-effort cleanup of the temp entry; the publish failure is what
		// the caller must react to (it retries the whole chunk).
		_, _ = client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{Directory: dir, Name: tmp})
		return fmt.Errorf("publish %s/%s: %w", dir, name, err)
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound ||
		strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())
}

func (s *filerStore) CreateJob(ctx context.Context, job *Job, items []Item) error {
	if job == nil || job.ID == "" {
		return fmt.Errorf("CreateJob: empty job")
	}
	job.SchemaVersion = SchemaVersion
	job.Version = 1
	jobData, err := marshalJob(job)
	if err != nil {
		return err
	}
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		if err := s.ensureJobDirs(ctx, client, job.ID); err != nil {
			return err
		}
		// Manifists first: a job.json observer never sees a half-provisioned job.
		for c := 0; c < ChunkCount(len(items)); c++ {
			start, end := ChunkRange(len(items), c)
			data, err := marshalManifest(c, items[start:end])
			if err != nil {
				return err
			}
			if err := s.atomicWrite(ctx, client, manifestDirPath(s.root, job.ID), manifestName(c), data, nil); err != nil {
				return err
			}
		}
		if err := s.createFileExclusive(ctx, client, jobDirPath(s.root, job.ID), jobDocName, jobData, versionExt(1)); err != nil {
			if errors.Is(err, ErrConflict) {
				return ErrConflict
			}
			return err
		}
		return nil
	})
}

func (s *filerStore) loadEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, name string) (*filer_pb.Entry, error) {
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return resp.Entry, nil
}

func (s *filerStore) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var job *Job
	err := s.withClient(ctx, true, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := s.loadEntry(ctx, client, jobDirPath(s.root, jobID), jobDocName)
		if err != nil {
			return err
		}
		j, err := unmarshalJob(entry.Content)
		if err != nil {
			return err
		}
		j.Version, _ = strconv.ParseInt(string(entry.Extended[extVersion]), 10, 64)
		job = j
		return nil
	})
	return job, err
}

func (s *filerStore) SaveJob(ctx context.Context, job *Job) error {
	data, err := marshalJob(job)
	if err != nil {
		return err
	}
	next := job.Version + 1
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		if err := s.updateCAS(ctx, client, jobDirPath(s.root, job.ID), jobDocName, data,
			versionExt(job.Version), versionExt(next)); err != nil {
			return err
		}
		job.Version = next
		return nil
	})
}

func (s *filerStore) listEntryNames(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, prefix string) ([]string, error) {
	var names []string
	startFrom := ""
	for {
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             prefix,
			StartFromFileName:  startFrom,
			InclusiveStartFrom: false,
			Limit:              1000,
		})
		if err != nil {
			if isNotFound(err) {
				return names, nil
			}
			return nil, err
		}
		var last string
		count := 0
		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				if isNotFound(recvErr) {
					return names, nil
				}
				return nil, recvErr
			}
			names = append(names, resp.Entry.Name)
			last = resp.Entry.Name
			count++
		}
		if count < 1000 {
			return names, nil
		}
		startFrom = last
	}
}

func (s *filerStore) ListJobIDs(ctx context.Context) ([]string, error) {
	var ids []string
	err := s.withClient(ctx, true, func(client filer_pb.SeaweedFilerClient) error {
		names, err := s.listEntryNames(ctx, client, s.root, "bj-")
		if err != nil {
			return err
		}
		for _, name := range names {
			// Only entries that actually carry a job.json are jobs; a temp dir
			// left by a crashed create must not surface as a job.
			if _, err := s.loadEntry(ctx, client, jobDirPath(s.root, name), jobDocName); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return err
			}
			ids = append(ids, name)
		}
		return nil
	})
	return ids, err
}

func (s *filerStore) ReadManifest(ctx context.Context, jobID string, chunk int) (items []Item, err error) {
	err = s.withClient(ctx, true, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := s.loadEntry(ctx, client, manifestDirPath(s.root, jobID), manifestName(chunk))
		if err != nil {
			return err
		}
		doc, err := unmarshalManifest(entry.Content)
		if err != nil {
			return err
		}
		items = doc.Items
		return nil
	})
	return items, err
}

func (s *filerStore) ReadChunkResult(ctx context.Context, jobID string, chunk int) (result *ChunkResult, err error) {
	err = s.withClient(ctx, true, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := s.loadEntry(ctx, client, resultsDirPath(s.root, jobID), resultName(chunk))
		if err != nil {
			return err
		}
		cr, err := unmarshalChunkResult(entry.Content)
		if err != nil {
			return err
		}
		result = cr
		return nil
	})
	return result, err
}

func (s *filerStore) WriteChunkResult(ctx context.Context, jobID string, result *ChunkResult) error {
	data, err := marshalChunkResult(result)
	if err != nil {
		return err
	}
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		return s.atomicWrite(ctx, client, resultsDirPath(s.root, jobID), resultName(result.Chunk), data, nil)
	})
}

func (s *filerStore) ListResultChunks(ctx context.Context, jobID string) ([]int, map[int]bool, error) {
	var chunks []int
	complete := map[int]bool{}
	err := s.withClient(ctx, true, func(client filer_pb.SeaweedFilerClient) error {
		names, err := s.listEntryNames(ctx, client, resultsDirPath(s.root, jobID), resultsPrefix)
		if err != nil {
			return err
		}
		for _, name := range names {
			idx, ok := chunkFromName(resultsPrefix, name)
			if !ok {
				continue
			}
			chunks = append(chunks, idx)
			entry, err := s.loadEntry(ctx, client, resultsDirPath(s.root, jobID), name)
			if err != nil {
				return err
			}
			if cr, err := unmarshalChunkResult(entry.Content); err == nil {
				complete[idx] = cr.Complete
			}
		}
		return nil
	})
	return chunks, complete, err
}

func (s *filerStore) leaseEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, jobID string) (*filer_pb.Entry, int64, *Lease, error) {
	entry, err := s.loadEntry(ctx, client, jobDirPath(s.root, jobID), leaseName)
	if err != nil {
		return nil, 0, nil, err
	}
	fence, lease, err := unmarshalLease(entry.Content)
	if err != nil {
		return nil, 0, nil, err
	}
	// Extended attributes are the CAS authority.
	if v := string(entry.Extended[extFence]); v != "" {
		if f, perr := strconv.ParseInt(v, 10, 64); perr == nil {
			fence = f
		}
	}
	if owner := string(entry.Extended[extOwner]); owner != "" {
		lease.Owner = owner
	}
	return entry, fence, lease, nil
}

func (s *filerStore) AcquireLease(ctx context.Context, jobID, owner string, now time.Time) (int64, error) {
	var fence int64
	err := s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		entry, oldFence, lease, err := s.leaseEntry(ctx, client, jobID)
		if errors.Is(err, ErrNotFound) {
			fence = 1
			l := Lease{JobID: jobID, Owner: owner, Fence: 1, ExpiresAt: now.Add(LeaseTTL)}
			data, _ := marshalLease(1, l)
			if cerr := s.createFileExclusive(ctx, client, jobDirPath(s.root, jobID), leaseName, data, leaseExt(1, owner, false)); cerr != nil {
				if errors.Is(cerr, ErrConflict) {
					// Racer created it first; let the caller retry as a conflict.
					return ErrLeaseHeld
				}
				return cerr
			}
			return nil
		}
		if err != nil {
			return err
		}
		released := string(entry.Extended[extRelease]) == "1"
		_ = lease
		if !released && now.Before(expiryFromEntry(entry)) {
			return ErrLeaseHeld
		}
		newFence := oldFence + 1
		l := Lease{JobID: jobID, Owner: owner, Fence: newFence, ExpiresAt: now.Add(LeaseTTL)}
		data, _ := marshalLease(newFence, l)
		if cerr := s.updateCAS(ctx, client, jobDirPath(s.root, jobID), leaseName, data,
			map[string][]byte{extFence: []byte(strconv.FormatInt(oldFence, 10))},
			leaseExt(newFence, owner, false)); cerr != nil {
			return ErrLeaseHeld
		}
		fence = newFence
		return nil
	})
	return fence, err
}

func expiryFromEntry(entry *filer_pb.Entry) time.Time {
	_, lease, err := unmarshalLease(entry.Content)
	if err != nil || lease == nil {
		return time.Time{}
	}
	return lease.ExpiresAt
}

func (s *filerStore) RenewLease(ctx context.Context, jobID, owner string, fence int64, now time.Time) error {
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		entry, _, _, err := s.leaseEntry(ctx, client, jobID)
		if err != nil {
			return err
		}
		if string(entry.Extended[extOwner]) != owner ||
			string(entry.Extended[extFence]) != strconv.FormatInt(fence, 10) {
			return ErrLeaseHeld
		}
		l := Lease{JobID: jobID, Owner: owner, Fence: fence, ExpiresAt: now.Add(LeaseTTL)}
		data, _ := marshalLease(fence, l)
		if err := s.updateCAS(ctx, client, jobDirPath(s.root, jobID), leaseName, data,
			map[string][]byte{
				extOwner: []byte(owner),
				extFence: []byte(strconv.FormatInt(fence, 10)),
			}, leaseExt(fence, owner, false)); err != nil {
			return ErrLeaseHeld
		}
		return nil
	})
}

func (s *filerStore) ReleaseLease(ctx context.Context, jobID, owner string, fence int64) error {
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		entry, _, _, err := s.leaseEntry(ctx, client, jobID)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if string(entry.Extended[extOwner]) != owner ||
			string(entry.Extended[extFence]) != strconv.FormatInt(fence, 10) {
			return ErrLeaseHeld
		}
		newFence := fence + 1
		l := Lease{JobID: jobID, Owner: "", Fence: newFence}
		data, _ := marshalLease(newFence, l)
		if err := s.updateCAS(ctx, client, jobDirPath(s.root, jobID), leaseName, data,
			map[string][]byte{
				extOwner: []byte(owner),
				extFence: []byte(strconv.FormatInt(fence, 10)),
			}, leaseExt(newFence, "", true)); err != nil {
			return ErrLeaseHeld
		}
		return nil
	})
}

func (s *filerStore) DeleteJob(ctx context.Context, jobID string) error {
	return s.withClient(ctx, false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:    s.root,
			Name:         jobID,
			IsRecursive:  true,
			IsDeleteData: true,
		})
		if err != nil {
			if isNotFound(err) {
				return ErrNotFound
			}
			return err
		}
		return nil
	})
}

func leaseExt(fence int64, owner string, released bool) map[string][]byte {
	rel := "0"
	if released {
		rel = "1"
	}
	return map[string][]byte{
		extFence:   []byte(strconv.FormatInt(fence, 10)),
		extOwner:   []byte(owner),
		extRelease: []byte(rel),
	}
}

// pathSplit splits "/a/b/c" into ("/a/b", "c") and handles the root case.
func pathSplit(p string) (string, string) {
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return "/", p
	}
	if idx == 0 {
		return "/", p[1:]
	}
	return p[:idx], p[idx+1:]
}
