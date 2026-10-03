package dsreview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// In a full folder, renaming a file deletes the old path everywhere while the
// new path is rejected by quota; the only copy left is the renaming replica's.
func TestRenameInFullFolderRemovesFileFromOtherReplicas(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	content := strings.Repeat("r", 1000)
	f, k := mkFolder(t, oc, ds.Limits{MaxTotalBytes: ds.SealedSize(1000) + 256 + 600})
	a, adir := attach(t, oc, f, k, "a")
	b, bdir := attach(t, oc, f, k, "b")
	write(t, adir, "report.pdf", content)
	a.Sync(bg)
	b.Sync(bg)
	if files(t, bdir)["report.pdf"] != content {
		t.Fatal("setup")
	}
	os.Rename(filepath.Join(adir, "report.pdf"), filepath.Join(adir, "final-report.pdf"))
	a.Sync(bg)
	b.Sync(bg)
	t.Logf("A: %d files, rejected=%v", len(files(t, adir)), a.Status().Rejected)
	t.Logf("B: %v", files(t, bdir))
	if len(files(t, bdir)) == 0 {
		t.Errorf("BUG: rename near quota removed the file from every other replica")
	}
}
