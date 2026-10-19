package batchjob

import (
	"context"
	"net"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// grpcMemFiler is a gRPC filer twin used to validate filerStore against the
// exact response/status contract the store relies on (O_EXCL,
// ExpectedExtended CAS, atomic overwrite rename, recursive delete).
type grpcMemFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]map[string]*filer_pb.Entry
}

func newGrpcMemFiler() *grpcMemFiler {
	return &grpcMemFiler{entries: map[string]map[string]*filer_pb.Entry{}}
}

func (f *grpcMemFiler) mkdirParentsLocked(dir string) {
	parts := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	cur := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		parent := cur
		if parent == "" {
			parent = "/"
		}
		if f.entries[parent] == nil {
			f.entries[parent] = map[string]*filer_pb.Entry{}
		}
		if _, ok := f.entries[parent][p]; !ok {
			f.entries[parent][p] = &filer_pb.Entry{Name: p, IsDirectory: true}
		}
		cur = path.Join(cur, p)
	}
}

func (f *grpcMemFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.entries[req.Directory] == nil {
		f.entries[req.Directory] = map[string]*filer_pb.Entry{}
	}
	if _, exists := f.entries[req.Directory][req.Entry.Name]; exists {
		if req.OExcl {
			return nil, status.Error(codes.AlreadyExists, filer_pb.ErrEntryAlreadyExists.Error())
		}
		if req.Entry.IsDirectory {
			return &filer_pb.CreateEntryResponse{ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS}, nil
		}
	}
	f.mkdirParentsLocked(req.Directory)
	if f.entries[req.Directory] == nil {
		f.entries[req.Directory] = map[string]*filer_pb.Entry{}
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *grpcMemFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.entries[req.Directory]
	existing, ok := d[req.Entry.Name]
	if !ok {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	for k, expected := range req.ExpectedExtended {
		actual := existing.Extended[k]
		if string(actual) != string(expected) {
			return nil, status.Errorf(codes.FailedPrecondition, "extended attribute %q changed", k)
		}
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *grpcMemFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.entries[req.Directory]; ok {
		if e, ok := d[req.Name]; ok {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "%s: %s/%s", filer_pb.ErrNotFound.Error(), req.Directory, req.Name)
}

func (f *grpcMemFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.mu.Lock()
	d, ok := f.entries[req.Directory]
	names := make([]string, 0, len(d))
	snapshot := make(map[string]*filer_pb.Entry, len(d))
	for name, entry := range d {
		names = append(names, name)
		snapshot[name] = entry
	}
	f.mu.Unlock()
	if !ok {
		return nil
	}
	sort.Strings(names)
	sent := uint32(0)
	for _, name := range names {
		if req.Prefix != "" && !strings.HasPrefix(name, req.Prefix) {
			continue
		}
		if req.StartFromFileName != "" {
			if name < req.StartFromFileName || (name == req.StartFromFileName && !req.InclusiveStartFrom) {
				continue
			}
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: snapshot[name]}); err != nil {
			return err
		}
		sent++
		if req.Limit > 0 && sent >= req.Limit {
			return nil
		}
	}
	return nil
}

func (f *grpcMemFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	target := path.Join(req.Directory, req.Name)
	if d, ok := f.entries[req.Directory]; ok {
		if _, ok := d[req.Name]; !ok && req.IsRecursive {
			// fall through to prefix sweep
		} else if !ok {
			return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
		}
		delete(d, req.Name)
	}
	if req.IsRecursive {
		for dir := range f.entries {
			if strings.HasPrefix(dir, target+"/") {
				delete(f.entries, dir)
			}
		}
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *grpcMemFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	src, ok := f.entries[req.OldDirectory][req.OldName]
	if !ok {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	f.mkdirParentsLocked(req.NewDirectory)
	if f.entries[req.NewDirectory] == nil {
		f.entries[req.NewDirectory] = map[string]*filer_pb.Entry{}
	}
	delete(f.entries[req.OldDirectory], req.OldName)
	// Overwrite semantics, mirroring the filer's moveEntry.
	f.entries[req.NewDirectory][req.NewName] = src
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (f *grpcMemFiler) Ping(_ context.Context, _ *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	now := time.Now().UnixNano()
	return &filer_pb.PingResponse{StartTimeNs: now, RemoteTimeNs: now, StopTimeNs: now}, nil
}

// startGrpcMemFiler serves an in-memory filer over gRPC and returns the
// client plus the server impl (for fault injection).
func startGrpcMemFiler(t *testing.T) (filer_pb.SeaweedFilerClient, *grpcMemFiler) {
	t.Helper()
	fs := newGrpcMemFiler()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := filer_pb.NewSeaweedFilerClient(conn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := client.Ping(ctx, &filer_pb.PingRequest{})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("filer not ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return client, fs
}
