package sharing

import (
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// A tracked file inside a directory that became unreadable is "missing" to
// rename detection (which skips the blockedLocal guard used by deletions). Any new
// file with the same content (e.g. an empty file) then deletes it remotely.
func TestUnreadableDirFeedsRenameDetection(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	a, adir := attach(t, oc, f, k, "a")
	b, bdir := attach(t, oc, f, k, "b")
	write(t, adir, "pkg/__init__.py", "")
	write(t, adir, "pkg/core.py", "print(1)")
	a.Sync(bg)
	b.Sync(bg)
	os.Chmod(filepath.Join(adir, "pkg"), 0)
	defer os.Chmod(filepath.Join(adir, "pkg"), 0700)
	write(t, adir, "notes/empty.txt", "")
	t.Logf("A sync: %v", a.Sync(bg))
	b.Sync(bg)
	got := files(t, bdir)
	t.Logf("B: %s", keys(got))
	if _, ok := got["pkg/__init__.py"]; !ok {
		t.Errorf("BUG: unreadable local dir + new empty file deleted pkg/__init__.py everywhere")
	}
}
