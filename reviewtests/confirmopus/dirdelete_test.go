package confirmreview

import (
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Replica b has an ignored file (.DS_Store, created by Finder on any macOS
// directory it has browsed) inside a directory that replica a deletes. b cannot
// remove the non-empty directory, marks the row deleted anyway, then the next
// scan sees an untracked live directory and re-uploads its marker: the delete
// never sticks and the directory reappears on every replica.
func TestDeletedDirectoryResurrectedByIgnoredFile(t *testing.T) {
	s, _ := newServer(t)
	p := ds.Principal{Tenant: "t", Subject: "owner"}
	c := s.Client(p)
	f, k := mkFolder(t, c, ds.Limits{})
	a, b := attach(t, c, f, k, "a"), attach(t, c, f, k, "b")
	write(t, a, "photos/one.jpg", "jpeg")
	syncAll(t, a, b)
	if files(t, b)["photos/one.jpg"] != "jpeg" {
		t.Fatal("setup", files(t, b))
	}
	write(t, b, "photos/.DS_Store", "finder") // ignored by default
	if e := os.RemoveAll(filepath.Join(a.dir, "photos")); e != nil {
		t.Fatal(e)
	}
	syncAll(t, a, b, a)
	if _, e := os.Lstat(filepath.Join(a.dir, "photos")); e == nil {
		t.Fatalf("directory deleted on a was resurrected from b's ignored .DS_Store")
	}
}
