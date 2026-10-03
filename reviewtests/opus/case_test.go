package dsreview

import (
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// On a case-insensitive volume, a case-only rename is never propagated as a delete
// (Lstat of the old name still succeeds). Both names now exist remotely; the old
// one keeps stale content forever on case-sensitive peers.
func TestCaseOnlyRenameLeavesStaleDuplicate(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	a, adir := attach(t, oc, f, k, "a")
	write(t, adir, "Readme.md", "v1")
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(adir, "README.MD")); e != nil {
		t.Skip("volume is case-sensitive")
	}
	if e := os.Rename(filepath.Join(adir, "Readme.md"), filepath.Join(adir, "README.md")); e != nil {
		t.Fatal(e)
	}
	write(t, adir, "README.md", "v2 after rename")
	for i := 0; i < 3; i++ {
		if e := a.Sync(bg); e != nil {
			t.Fatal(e)
		}
	}
	d, _ := oc.Changes(bg, f.ID, 0)
	live := 0
	for _, r := range d.Rows {
		if !r.Deleted {
			m, _ := ds.OpenMetadata(k, f.ID, r)
			t.Logf("remote: %s (v%d)", m.Path, r.Version)
			live++
		}
	}
	if live != 1 {
		t.Errorf("BUG: case-only rename left %d live remote paths", live)
	}
}
