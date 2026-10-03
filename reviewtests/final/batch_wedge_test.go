package finalreview

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

// Finding: Reserve admits a rename and an unrelated small file, but the batched
// commit adds an unreserved garbage row for the renamed blob and fails. Both
// files are rejected together and re-batched together on every retry, so the
// unrelated file never uploads even though it fits on its own.
func TestPendingRenameBlocksUnrelatedUploadForever(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
	f, k := mkFolder(t, c, ds.Limits{})
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, e := ds.Attach(bg, c, f.ID, k, dir, ds.Options{Name: "a", StateDir: filepath.Join(base, "state"), Manual: true, RetryInterval: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	a := rep{r, func() string { d, _ := filepath.EvalSymlinks(dir); return d }()}
	write(t, a, "old.txt", "renamed content")
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	got, _ := c.GetFolder(bg, f.ID)
	// Room for the rename alone (new row + garbage row) or the small file alone.
	if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: got.Usage.Rows + 2}); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(a.dir, "old.txt"), filepath.Join(a.dir, "new.txt")); e != nil {
		t.Fatal(e)
	}
	write(t, a, "z.txt", "unrelated")
	for i := 0; i < 5; i++ {
		time.Sleep(5 * time.Millisecond)
		_ = a.Sync(bg)
	}
	d, _ := c.Changes(bg, f.ID, 0)
	live := 0
	for _, row := range d.Rows {
		if !row.Deleted {
			live++
		}
	}
	t.Logf("remote live rows=%d rejected=%v", live, a.Status().Rejected)
	b := attach(t, c, f, k, "b")
	_ = b.Sync(bg)
	t.Logf("fresh peer sees %v", files(t, b))
	if _, ok := files(t, b)["z.txt"]; !ok {
		t.Error("z.txt never uploaded although it fits; it is wedged behind the pending rename")
	}
}

// Control: with the same cap, the rename alone succeeds and z alone succeeds.
func TestRenameAloneAndFileAloneFitSameCap(t *testing.T) {
	for _, variant := range []string{"rename", "file"} {
		t.Run(variant, func(t *testing.T) {
			s, _ := newServer(t)
			c := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
			f, k := mkFolder(t, c, ds.Limits{})
			a := attach(t, c, f, k, "a")
			write(t, a, "old.txt", "renamed content")
			if e := a.Sync(bg); e != nil {
				t.Fatal(e)
			}
			got, _ := c.GetFolder(bg, f.ID)
			if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: got.Usage.Rows + 2}); e != nil {
				t.Fatal(e)
			}
			if variant == "rename" {
				os.Rename(filepath.Join(a.dir, "old.txt"), filepath.Join(a.dir, "new.txt"))
			} else {
				write(t, a, "z.txt", "unrelated")
			}
			if e := a.Sync(bg); e != nil || len(a.Status().Rejected) != 0 {
				t.Fatalf("%v %v", e, a.Status().Rejected)
			}
		})
	}
}
