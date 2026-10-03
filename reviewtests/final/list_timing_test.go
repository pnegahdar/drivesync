package finalreview

import (
	"fmt"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

func TestForeignFoldersSlowVictimList(t *testing.T) {
	s, m := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "a"})
	mkFolder(t, victim, ds.Limits{})
	list := func() time.Duration {
		return median(t, 51, func() {
			if _, e := victim.ListFolders(bg); e != nil {
				t.Fatal(e)
			}
		})
	}
	l0 := list()
	const n = 20000
	start := time.Now()
	k := ds.NewFolderKey()
	// Admit one folder publicly, then retain the exact 20,000-folder scale
	// through one legal fixture transaction instead of 20,000 durable writes.
	seed, e := ds.CreateFolder(bg, attacker, ds.FolderSpec{Name: "x0"}, k)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Transaction(bg, func(meta *ds.Metadata) error {
		original := meta.Folders[seed.ID]
		for i := 1; i < n; i++ {
			f := original
			f.Folder.ID = randPID()[:32]
			f.Folder.Name = fmt.Sprint("x", i)
			meta.Folders[f.Folder.ID] = f
		}
		if len(meta.Folders) != n+1 {
			t.Fatal("folder cardinality", len(meta.Folders))
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	t.Logf("attacker created %d folders in %v", n, time.Since(start))
	l1 := list()
	t.Logf("victim ListFolders median %v -> %v", l0, l1)
	if l1 > 5*l0 {
		t.Errorf("victim ListFolders latency scales with another tenant's folders")
	}
}
