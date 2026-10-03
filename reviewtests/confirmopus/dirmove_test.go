package confirmreview

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

// Moving one directory of N files is O(N^2): rename detection loops over every
// index entry for every new path and calls exactExists (a ReadDir per path
// component) before the cheap hash test, and each rename pair is reserved,
// re-uploaded and committed alone while every folder write decodes all rows.
// Sync holds syncMu throughout.
func TestDirectoryMoveRenameDetectionIsQuadratic(t *testing.T) {
	n := 1000
	if v := os.Getenv("MOVE_FILES"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "t", Subject: "owner"})
	f, k := mkFolder(t, c, ds.Limits{})
	a := attach(t, c, f, k, "a")
	for i := 0; i < n; i++ {
		write(t, a, fmt.Sprintf("project/src/file-%05d.txt", i), fmt.Sprintf("content %d", i))
	}
	start := time.Now()
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	initial := time.Since(start)
	if e := os.Rename(filepath.Join(a.dir, "project"), filepath.Join(a.dir, "renamed")); e != nil {
		t.Fatal(e)
	}
	start = time.Now()
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	moved := time.Since(start)
	t.Logf("files=%d initial upload sync=%v sync after directory rename=%v", n, initial, moved)
	if moved > 3*initial+5*time.Second {
		t.Fatalf("renaming a %d-file directory took %v to sync vs %v to upload it", n, moved, initial)
	}
}
