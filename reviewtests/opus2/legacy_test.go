package dsreview2

import (
	"errors"
	ds "github.com/pnegahdar/drivesync"
	"os"
	"path/filepath"
	"testing"
)

// The original legacy injection is archived in fixtures. This prototype test
// exercises its independent failure: a failed tombstone must not stop downloads.
func TestDeleteFailureDoesNotBlockDownloads(t *testing.T) {
	s := newServer(t)
	ac := s.Client(ds.Principal{Tenant: "acme", Subject: "alice"})
	f, k := mkFolder(t, ac, ds.Limits{})
	putFile(t, ac, f, k, "old.txt", 0, []byte("old"))
	r, dir := attach(t, ac, f, k, "alice-laptop")
	if e := r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	r.Close()
	h := &hookClient{Client: ac, commit: func(m []ds.Mutation) error {
		for _, v := range m {
			if v.Deleted {
				return ds.ErrQuota
			}
		}
		return nil
	}}
	r, e := ds.Attach(bg, h, f.ID, k, dir, ds.Options{StateDir: filepath.Join(filepath.Dir(dir), "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = os.Remove(filepath.Join(dir, "old.txt")); e != nil {
		t.Fatal(e)
	}
	putFile(t, ac, f, k, "teammate-new.txt", 0, []byte("new"))
	e = r.Sync(bg)
	_, got := files(t, dir)["teammate-new.txt"]
	t.Logf("pending tombstone: %v; teammate download: %v", e, got)
	if !errors.Is(e, ds.ErrQuota) || !got {
		t.Fatal("tombstone failure blocked downloads", e, got)
	}
}
