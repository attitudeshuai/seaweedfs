package filer

import (
	"context"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCountTestFiler(store FilerStore) *Filer {
	return &Filer{
		Store: NewFilerStoreWrapper(store),
	}
}

func putTestEntry(t *testing.T, store FilerStore, p string, isDir bool) {
	t.Helper()
	e := &Entry{FullPath: util.FullPath(p)}
	if isDir {
		e.Attr.Mode = os.ModeDir | 0755
	}
	require.NoError(t, store.InsertEntry(context.Background(), e))
}

// buildCountTree lays out:
// /root
//   /root/f1 /root/f2
//   /root/d1(f3)/d1f
//   /root/d2(f4,f5)/
// i.e. 5 children below root in total (3 files + 2 dirs), root not counted.
func buildCountTree(t *testing.T) FilerStore {
	store := newStubFilerStore()
	putTestEntry(t, store, "/root", true)
	putTestEntry(t, store, "/root/f1", false)
	putTestEntry(t, store, "/root/f2", false)
	putTestEntry(t, store, "/root/d1", true)
	putTestEntry(t, store, "/root/d1/d1f", false)
	putTestEntry(t, store, "/root/d2", true)
	putTestEntry(t, store, "/root/d2/f4", false)
	putTestEntry(t, store, "/root/d2/f5", false)
	return store
}

func TestCountSubtreeEntriesBounded(t *testing.T) {
	ctx := context.Background()
	store := buildCountTree(t)
	f := newCountTestFiler(store)

	count, truncated, err := f.countSubtreeEntriesBounded(ctx, "/root", 100)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, 7, count)

	// Empty directory: zero children, not truncated.
	putTestEntry(t, store, "/empty", true)
	count, truncated, err = f.countSubtreeEntriesBounded(ctx, "/empty", 10)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, 0, count)
}

func TestCountSubtreeEntriesBoundedTruncates(t *testing.T) {
	ctx := context.Background()
	store := buildCountTree(t)
	f := newCountTestFiler(store)

	for _, limit := range []int{0, 2, 6} {
		count, truncated, err := f.countSubtreeEntriesBounded(ctx, "/root", limit)
		require.NoError(t, err)
		assert.True(t, truncated, "limit %d should truncate", limit)
		assert.Equal(t, limit+1, count)
	}

	// Exactly at the limit: not truncated.
	count, truncated, err := f.countSubtreeEntriesBounded(ctx, "/root", 7)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, 7, count)
}
