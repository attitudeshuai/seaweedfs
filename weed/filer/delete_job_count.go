package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// countSubtreeEntriesBounded counts entries (files and directories) below
// root, not counting root itself, with a breadth-first walk bounded at
// limit+1: once more than limit entries have been observed it stops and
// returns truncated=true so the caller never pays an unbounded scan just to
// decide whether a recursive delete needs a job. The metadata work is O(limit)
// list calls at the configured pagination.
func (f *Filer) countSubtreeEntriesBounded(ctx context.Context, root util.FullPath, limit int) (count int, truncated bool, err error) {
	if limit < 0 {
		limit = 0
	}
	dirs := []util.FullPath{root}
	for len(dirs) > 0 {
		dir := dirs[0]
		dirs = dirs[1:]
		lastFileName := ""
		for {
			var page []*Entry
			page, _, err = f.ListDirectoryEntries(ctx, dir, lastFileName, false, PaginationSize, "", "", "")
			if err != nil {
				return count, false, err
			}
			for _, entry := range page {
				count++
				if count > limit {
					return count, true, nil
				}
				if entry.IsDirectory() {
					dirs = append(dirs, entry.FullPath)
				}
			}
			if len(page) < PaginationSize {
				break
			}
			lastFileName = page[len(page)-1].Name()
		}
	}
	return count, false, nil
}
