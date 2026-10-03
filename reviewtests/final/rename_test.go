package finalreview

import (
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// Finding: a full-folder rename onto a name that has a local tombstone is not
// detected as a rename, so the old remote path is deleted while the new path
// is rejected. Peers lose their only copy until capacity frees.
func TestFullFolderRenameOntoTombstonedNameDeletesOldRemote(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
	f, k := mkFolder(t, c, ds.Limits{})
	a := attach(t, c, f, k, "a")
	b := attach(t, c, f, k, "b")
	write(t, a, "final.txt", "earlier")
	write(t, a, "draft.txt", "precious draft")
	syncAll(t, a, b)
	if e := os.Remove(filepath.Join(a.dir, "final.txt")); e != nil {
		t.Fatal(e)
	}
	syncAll(t, a, b) // final.txt is now a known tombstone on both replicas
	got, _ := c.GetFolder(bg, f.ID)
	if e := s.CollectGarbage(bg); e != nil {
		t.Fatal(e)
	}
	got, _ = c.GetFolder(bg, f.ID)
	if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: got.Usage.Bytes + 64, MaxRows: got.Usage.Rows + 1}); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(a.dir, "draft.txt"), filepath.Join(a.dir, "final.txt")); e != nil {
		t.Fatal(e)
	}
	syncAll(t, a, b)
	t.Logf("a=%v rejected=%v", files(t, a), a.Status().Rejected)
	t.Logf("b=%v", files(t, b))
	if len(files(t, b)) == 0 {
		t.Error("peer lost draft.txt: old remote path deleted while the rename target was rejected")
	}
}

// Control: the same rename onto a never-seen name keeps the old remote path.
func TestFullFolderRenameOntoFreshNameKeepsOldRemote(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
	f, k := mkFolder(t, c, ds.Limits{})
	a := attach(t, c, f, k, "a")
	b := attach(t, c, f, k, "b")
	write(t, a, "draft.txt", "precious draft")
	syncAll(t, a, b)
	got, _ := c.GetFolder(bg, f.ID)
	if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: got.Usage.Bytes + 64, MaxRows: got.Usage.Rows + 1}); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(a.dir, "draft.txt"), filepath.Join(a.dir, "final.txt")); e != nil {
		t.Fatal(e)
	}
	syncAll(t, a, b)
	if files(t, b)["draft.txt"] != "precious draft" {
		t.Fatalf("control failed: %v", files(t, b))
	}
}

// Case-only rename between two replicas on this (case-insensitive APFS) volume.
func TestCaseOnlyRenameOnCaseInsensitiveVolume(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
	f, k := mkFolder(t, c, ds.Limits{})
	a := attach(t, c, f, k, "a")
	b := attach(t, c, f, k, "b")
	write(t, a, "Docs/Report.txt", "v1")
	syncAll(t, a, b)
	if e := os.Rename(filepath.Join(a.dir, "Docs"), filepath.Join(a.dir, "docs")); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(a.dir, "docs/Report.txt"), filepath.Join(a.dir, "docs/report.txt")); e != nil {
		t.Fatal(e)
	}
	syncAll(t, a, b)
	t.Logf("a=%v errors=%v", files(t, a), a.Status().Errors)
	t.Logf("b=%v errors=%v", files(t, b), b.Status().Errors)
	if got := files(t, b); len(got) != 1 || got["docs/report.txt"] != "v1" {
		t.Fatalf("case-only rename created aliases: %v", got)
	}
	write(t, b, firstKey(files(t, b)), "edited on b")
	syncAll(t, a, b)
	t.Logf("after edit a=%v b=%v", files(t, a), files(t, b))
	if files(t, a)["docs/report.txt"] != "edited on b" {
		t.Errorf("edit on b did not reach a's renamed path")
	}
}
func firstKey(m map[string]string) string {
	for k := range m {
		return k
	}
	return ""
}
