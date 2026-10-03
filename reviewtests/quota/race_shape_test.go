package quota

import (
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Benign race: A creates file "notes" while B creates "notes/todo.txt". After B
// syncs once and goes offline, a third replica is stuck for every later row.
func TestBenignFileDirRaceWedgesThirdReplica(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	a, adir := attach(t, oc, f, k, "a")
	b, bdir := attach(t, oc, f, k, "b")
	c, cdir := attach(t, oc, f, k, "c")
	write(t, adir, "notes", "A's file")
	write(t, bdir, "notes/todo.txt", "B's todo")
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	t.Logf("B first sync: %v", b.Sync(bg))
	b.Close() // laptop lid closed
	write(t, adir, "zz/later.txt", "later work")
	t.Logf("A sync: %v", a.Sync(bg))
	var e error
	for i := 0; i < 3; i++ {
		e = c.Sync(bg)
	}
	t.Logf("C sync: %v; C files=%v", e, files(t, cdir))
	if _, ok := files(t, cdir)["zz/later.txt"]; !ok {
		t.Errorf("BUG: C cannot receive later files until B comes back online")
	}
}
