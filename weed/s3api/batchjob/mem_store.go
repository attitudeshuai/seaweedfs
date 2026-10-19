package batchjob

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// memStore is the in-memory Store twin. Its concurrency semantics mirror what
// the filer implementation relies on: O_EXCL create, ExpectedExtended CAS and
// atomic rename-over.
type memStore struct {
	mu sync.Mutex
	// dirs is the set of directory paths. files maps dir -> name -> memFile.
	dirs  map[string]bool
	files map[string]map[string]*memFile
	// failHooks let fault-injection tests break a publish mid-flight.
	failRename func(jobID, from, to string) error
}

type memFile struct {
	content  []byte
	extended map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{dirs: map[string]bool{"/": true}, files: map[string]map[string]*memFile{}}
}

func (m *memStore) mkdirLocked(dir string) {
	for d := dir; d != "/" && d != "." && d != ""; {
		m.dirs[d] = true
		idx := strings.LastIndex(d, "/")
		if idx <= 0 {
			break
		}
		d = d[:idx]
	}
}

func (m *memStore) createLocked(dir, name string, f *memFile, oexcl bool) error {
	m.mkdirLocked(dir)
	if _, ok := m.files[dir]; !ok {
		m.files[dir] = map[string]*memFile{}
	}
	if oexcl {
		if _, exists := m.files[dir][name]; exists {
			return ErrConflict
		}
	}
	m.files[dir][name] = f
	return nil
}

func (m *memStore) casLocked(dir, name string, expected map[string][]byte, f *memFile) error {
	existing, ok := m.files[dir][name]
	if !ok {
		return ErrNotFound
	}
	for k, v := range expected {
		if string(existing.extended[k]) != string(v) {
			return ErrConflict
		}
	}
	m.files[dir][name] = f
	return nil
}

// renameLocked moves src over dst atomically, creating the destination dir.
func (m *memStore) renameLocked(srcDir, srcName, dstDir, dstName string) error {
	src, ok := m.files[srcDir][srcName]
	if !ok {
		return ErrNotFound
	}
	m.mkdirLocked(dstDir)
	if _, ok := m.files[dstDir]; !ok {
		m.files[dstDir] = map[string]*memFile{}
	}
	delete(m.files[srcDir], srcName)
	m.files[dstDir][dstName] = src
	return nil
}

func (m *memStore) CreateJob(_ context.Context, job *Job, items []Item) error {
	if job == nil || job.ID == "" {
		return ErrNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	root := jobDirPath(memRoot, job.ID)
	if _, exists := m.files[root][jobDocName]; exists {
		return ErrConflict
	}
	m.mkdirLocked(manifestDirPath(memRoot, job.ID))
	m.mkdirLocked(resultsDirPath(memRoot, job.ID))

	chunkCount := ChunkCount(len(items))
	for c := 0; c < chunkCount; c++ {
		start, end := ChunkRange(len(items), c)
		data, err := marshalManifest(c, items[start:end])
		if err != nil {
			return err
		}
		if err := m.createLocked(manifestDirPath(memRoot, job.ID), manifestName(c),
			&memFile{content: data, extended: map[string][]byte{}}, true); err != nil {
			return err
		}
	}

	job.SchemaVersion = SchemaVersion
	job.Version = 1
	data, err := marshalJob(job)
	if err != nil {
		return err
	}
	return m.createLocked(root, jobDocName, &memFile{content: data, extended: versionExt(1)}, true)
}

func (m *memStore) GetJob(_ context.Context, jobID string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[jobDirPath(memRoot, jobID)][jobDocName]
	if !ok {
		return nil, ErrNotFound
	}
	job, err := unmarshalJob(f.content)
	if err != nil {
		return nil, err
	}
	job.Version, _ = strconv.ParseInt(string(f.extended[extVersion]), 10, 64)
	return job, nil
}

func (m *memStore) SaveJob(_ context.Context, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := marshalJob(job)
	if err != nil {
		return err
	}
	next := job.Version + 1
	f := &memFile{content: data, extended: versionExt(next)}
	if err := m.casLocked(jobDirPath(memRoot, job.ID), jobDocName, versionExt(job.Version), f); err != nil {
		return err
	}
	job.Version = next
	return nil
}

func (m *memStore) ListJobIDs(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for dir := range m.dirs {
		if parent, name := splitLeaf(dir); parent == memRoot && strings.HasPrefix(name, "bj-") {
			if _, ok := m.files[dir][jobDocName]; ok {
				ids = append(ids, name)
			}
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (m *memStore) ReadManifest(_ context.Context, jobID string, chunk int) ([]Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[manifestDirPath(memRoot, jobID)][manifestName(chunk)]
	if !ok {
		return nil, ErrNotFound
	}
	doc, err := unmarshalManifest(f.content)
	if err != nil {
		return nil, err
	}
	return doc.Items, nil
}

func (m *memStore) ReadChunkResult(_ context.Context, jobID string, chunk int) (*ChunkResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[resultsDirPath(memRoot, jobID)][resultName(chunk)]
	if !ok {
		return nil, ErrNotFound
	}
	return unmarshalChunkResult(f.content)
}

func (m *memStore) WriteChunkResult(_ context.Context, jobID string, result *ChunkResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := marshalChunkResult(result)
	if err != nil {
		return err
	}
	dir := resultsDirPath(memRoot, jobID)
	tmp := tempName(resultName(result.Chunk))
	m.mkdirLocked(dir)
	if err := m.createLocked(dir, tmp, &memFile{content: data, extended: map[string][]byte{}}, true); err != nil {
		return err
	}
	if hook := m.failRename; hook != nil {
		if err := hook(jobID, tmp, resultName(result.Chunk)); err != nil {
			return err
		}
	}
	return m.renameLocked(dir, tmp, dir, resultName(result.Chunk))
}

func (m *memStore) ListResultChunks(_ context.Context, jobID string) ([]int, map[int]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := resultsDirPath(memRoot, jobID)
	var chunks []int
	complete := map[int]bool{}
	for name, f := range m.files[dir] {
		idx, ok := chunkFromName(resultsPrefix, name)
		if !ok {
			continue
		}
		chunks = append(chunks, idx)
		if cr, err := unmarshalChunkResult(f.content); err == nil {
			complete[idx] = cr.Complete
		}
	}
	sort.Ints(chunks)
	return chunks, complete, nil
}

func (m *memStore) AcquireLease(_ context.Context, jobID, owner string, now time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := jobDirPath(memRoot, jobID)
	exp := now.Add(LeaseTTL)
	existing, ok := m.files[dir][leaseName]
	if !ok {
		f := leaseFile(1, Lease{JobID: jobID, Owner: owner, Fence: 1, ExpiresAt: exp})
		if err := m.createLocked(dir, leaseName, f, true); err != nil {
			return 0, err
		}
		return 1, nil
	}
	fence, lease, err := unmarshalLease(existing.content)
	if err != nil {
		return 0, err
	}
	released := string(existing.extended[extRelease]) == "1"
	if !released && now.Before(lease.ExpiresAt) {
		return 0, ErrLeaseHeld
	}
	newFence := fence + 1
	next := leaseFile(newFence, Lease{JobID: jobID, Owner: owner, Fence: newFence, ExpiresAt: exp})
	if err := m.casLocked(dir, leaseName, map[string][]byte{extFence: []byte(strconv.FormatInt(fence, 10))}, next); err != nil {
		return 0, err
	}
	return newFence, nil
}

func (m *memStore) RenewLease(_ context.Context, jobID, owner string, fence int64, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := jobDirPath(memRoot, jobID)
	existing, ok := m.files[dir][leaseName]
	if !ok {
		return ErrNotFound
	}
	if string(existing.extended[extOwner]) != owner ||
		string(existing.extended[extFence]) != strconv.FormatInt(fence, 10) {
		return ErrLeaseHeld
	}
	exp := now.Add(LeaseTTL)
	next := leaseFile(fence, Lease{JobID: jobID, Owner: owner, Fence: fence, ExpiresAt: exp})
	return m.casLocked(dir, leaseName,
		map[string][]byte{extOwner: []byte(owner), extFence: []byte(strconv.FormatInt(fence, 10))}, next)
}

func (m *memStore) ReleaseLease(_ context.Context, jobID, owner string, fence int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := jobDirPath(memRoot, jobID)
	existing, ok := m.files[dir][leaseName]
	if !ok {
		return nil
	}
	if string(existing.extended[extOwner]) != owner ||
		string(existing.extended[extFence]) != strconv.FormatInt(fence, 10) {
		return ErrLeaseHeld
	}
	newFence := fence + 1
	lease := Lease{JobID: jobID, Owner: "", Fence: newFence}
	f := leaseFile(newFence, lease)
	f.extended[extRelease] = []byte("1")
	return m.casLocked(dir, leaseName,
		map[string][]byte{extOwner: []byte(owner), extFence: []byte(strconv.FormatInt(fence, 10))}, f)
}

func (m *memStore) DeleteJob(_ context.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := jobDirPath(memRoot, jobID)
	found := false
	for dir := range m.dirs {
		if dir == prefix || strings.HasPrefix(dir, prefix+"/") {
			delete(m.dirs, dir)
			delete(m.files, dir)
			found = true
		}
	}
	if d, ok := m.files[prefix]; ok && len(d) > 0 {
		found = true
		delete(m.files, prefix)
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

func leaseFile(fence int64, lease Lease) *memFile {
	data, _ := marshalLease(fence, lease)
	return &memFile{content: data, extended: map[string][]byte{
		extFence:   []byte(strconv.FormatInt(fence, 10)),
		extOwner:   []byte(lease.Owner),
		extRelease: []byte("0"),
	}}
}

func splitLeaf(p string) (string, string) {
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return "", p
	}
	return p[:idx], p[idx+1:]
}

// memRoot is the synthetic root for memStore. It is deliberately not "/" so a
// leaked delete cannot sweep unrelated test state.
const memRoot = "/batch-jobs-test"
