package qa

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

func randomHex(i int) string { return fmt.Sprintf("%032x", 1<<40+i) }

// Every write transaction on a folder (and every per-object GC cleanup) loads
// all of that folder's tickets and garbage. After DeleteFolder (or heavy
// overwrite churn) leaves G garbage rows, draining is O(G^2) writer time and
// each cleanup holds the global SQLite writer for O(G).
func TestGarbageBacklogDrainIsQuadraticAndHoldsWriter(t *testing.T) {
	g := 20000
	if v := os.Getenv("QA_GARBAGE"); v != "" {
		g, _ = strconv.Atoi(v)
	}
	s, m, _ := newServer(t)
	ev := s.Client(eve)
	ef, _ := mkFolder(t, ev, ds.Limits{})
	// State equivalent to eve deleting a folder of g files (seeded directly).
	if e := m.Transaction(bg, func(md *ds.Metadata) error {
		for i := 0; i < g; i++ {
			id := randomHex(i)
			md.Garbage[ef.ID+"/"+id] = ds.Garbage{FolderID: ef.ID, BlobID: id, Owner: eve, Size: 300}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	al := s.Client(alice)
	af, ak := mkFolder(t, al, ds.Limits{})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var worst time.Duration
	wg.Add(1)
	go func() { defer wg.Done(); worst, _ = victimWrites(t, al, af, ak, stop) }()
	start := time.Now()
	_ = s.CollectGarbage(bg) // one maintenance pass (5 s budget)
	pass := time.Since(start)
	close(stop)
	wg.Wait()
	f, _ := ev.GetFolder(bg, ef.ID)
	collected := int64(g) - f.Usage.GarbageRows
	t.Logf("garbage backlog %d: one GC pass (%v) collected %d; victim worst write %v", g, pass, collected, worst)
	if collected < int64(g)/2 {
		t.Errorf("GC throughput collapses with backlog size: %d of %d per pass", collected, g)
	}
	if worst > 250*time.Millisecond {
		t.Errorf("another tenant's GC cleanup held the writer: victim waited %v", worst)
	}
}
