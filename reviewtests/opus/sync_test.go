package dsreview

import (
	"strings"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// A network blip during commit leaves the replica's own reservation behind. The
// next sync treats that as a conflict, renames the user's file away, and the sync
// after that commits a tombstone: the original path disappears on every replica.
func TestNetworkBlipDeletesEditedPathEverywhere(t *testing.T) {
	s := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "o"}
	base := s.Client(owner)
	f, k := mkFolder(t, base, ds.Limits{})
	hc := &hookClient{Client: base}
	a, adir := attach(t, hc, f, k, "a")
	b, bdir := attach(t, base, f, k, "b")
	write(t, adir, "app/config.yaml", "v1")
	for i := 0; i < 2; i++ {
		if e := a.Sync(bg); e != nil {
			t.Fatal(e)
		}
		if e := b.Sync(bg); e != nil {
			t.Fatal(e)
		}
	}
	if files(t, bdir)["app/config.yaml"] != "v1" {
		t.Fatal("setup", files(t, bdir))
	}
	write(t, adir, "app/config.yaml", "v2 important edit")
	hc.commit = func([]ds.Mutation) error { return errNet }
	hc.cancel = func() error { return errNet }
	if e := a.Sync(bg); e == nil {
		t.Fatal("expected network error")
	}
	hc.commit, hc.cancel = nil, nil // network is back
	for i := 0; i < 3; i++ {
		_ = a.Sync(bg)
		_ = b.Sync(bg)
	}
	t.Logf("A: %s", keys(files(t, adir)))
	t.Logf("B: %s", keys(files(t, bdir)))
	if _, ok := files(t, bdir)["app/config.yaml"]; !ok {
		for p := range files(t, bdir) {
			if strings.Contains(p, "conflict") {
				t.Errorf("BUG: app/config.yaml was deleted on every replica after a transient network failure; content survives only as %q", p)
			}
		}
	}
}

// Two replicas edit the same file at about the same time. Instead of one winner
// at the path plus one conflict copy, the path is deleted and both versions end up
// as conflict copies.
func TestConcurrentEditDeletesPath(t *testing.T) {
	s := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "o"}
	base := s.Client(owner)
	f, k := mkFolder(t, base, ds.Limits{})
	ha := &hookClient{Client: base}
	a, adir := attach(t, ha, f, k, "a")
	b, bdir := attach(t, base, f, k, "b")
	write(t, adir, "notes.txt", "v1")
	for i := 0; i < 2; i++ {
		a.Sync(bg)
		b.Sync(bg)
	}
	write(t, adir, "notes.txt", "A's edit")
	write(t, bdir, "notes.txt", "B's edit")
	// A is mid-upload (slow link) when B syncs twice (B's watcher reacts to its own rename).
	ha.commit = func([]ds.Mutation) error {
		ha.commit = nil
		_ = b.Sync(bg)
		_ = b.Sync(bg)
		return nil
	}
	ha.mu.Lock()
	ha.mu.Unlock()
	_ = a.Sync(bg)
	for i := 0; i < 3; i++ {
		_ = a.Sync(bg)
		_ = b.Sync(bg)
	}
	t.Logf("A: %s", keys(files(t, adir)))
	t.Logf("B: %s", keys(files(t, bdir)))
	if _, ok := files(t, adir)["notes.txt"]; !ok {
		t.Errorf("BUG: notes.txt deleted on all replicas after a concurrent edit")
	}
}

// One delete racing a batch of edits turns every co-batched edited file into a
// conflict copy and then deletes the original paths globally.
func TestBatchConflictDeletesUnrelatedFiles(t *testing.T) {
	s := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "o"}
	base := s.Client(owner)
	f, k := mkFolder(t, base, ds.Limits{})
	ha := &hookClient{Client: base}
	a, adir := attach(t, ha, f, k, "a")
	b, bdir := attach(t, base, f, k, "b")
	for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
		write(t, adir, p, "v1 "+p)
	}
	for i := 0; i < 2; i++ {
		a.Sync(bg)
		b.Sync(bg)
	}
	for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
		write(t, adir, p, "v2 "+p)
	}
	// B deletes a.txt while A is between reserve and commit.
	ha.commit = func([]ds.Mutation) error {
		ha.commit = nil
		pid, _ := ds.PathID(k, f.ID, "a.txt")
		d, _ := base.Changes(bg, f.ID, 0)
		for _, r := range d.Rows {
			if r.PathID == pid {
				if _, e := base.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: r.Version, Deleted: true}}); e != nil {
					t.Fatal(e)
				}
			}
		}
		return nil
	}
	_ = a.Sync(bg)
	for i := 0; i < 3; i++ {
		_ = a.Sync(bg)
		_ = b.Sync(bg)
	}
	got := files(t, bdir)
	t.Logf("B: %s", keys(got))
	for _, p := range []string{"b.txt", "c.txt"} {
		if _, ok := got[p]; !ok {
			t.Errorf("BUG: %s (never touched by B, no conflict) deleted everywhere", p)
		}
	}
}
