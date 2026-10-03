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

type drainResult struct {
	n         int
	pass      time.Duration
	collected int64
	worst     time.Duration
}

func (d drainResult) perItem() time.Duration {
	if d.collected <= 0 {
		return d.pass
	}
	return d.pass / time.Duration(d.collected)
}

// A garbage backlog must drain in linear time. Each batch loads only the
// garbage it deletes and releases the global writer before the next batch, so
// time per collected row stays flat as the backlog grows and another tenant's
// write has a fixed hold ceiling.
func TestGarbageBacklogDrainIsQuadraticAndHoldsWriter(t *testing.T) {
	smallN, largeN := 4000, 16000
	if v := os.Getenv("QA_GARBAGE"); v != "" {
		largeN, _ = strconv.Atoi(v)
		if largeN < 4 {
			t.Fatalf("QA_GARBAGE=%s", v)
		}
		smallN = largeN / 4
	}
	// Scaling is measured without a competing writer. That writer's own commits
	// otherwise land inside the drain and make a larger backlog look slower.
	small := measureDrain(t, smallN, false)
	large := measureDrain(t, largeN, false)
	held := measureDrain(t, largeN, true)
	t.Logf("garbage backlog %d: pass %v collected %d (%v/item)", small.n, small.pass, small.collected, small.perItem())
	t.Logf("garbage backlog %d: pass %v collected %d (%v/item)", large.n, large.pass, large.collected, large.perItem())
	t.Logf("garbage backlog %d with a competing writer: pass %v collected %d victim %v", held.n, held.pass, held.collected, held.worst)
	if small.collected < int64(small.n)/2 || large.collected < int64(large.n)/2 || held.collected < int64(held.n)/2 {
		t.Fatalf("GC pass did not collect half the backlog: %d/%d, %d/%d, and %d/%d", small.collected, small.n, large.collected, large.n, held.collected, held.n)
	}
	// A quadratic drain makes the larger backlog several times more expensive
	// per row. Fixed overhead can make the smaller one look slightly worse.
	if large.perItem() > small.perItem()*2 {
		t.Errorf("GC time per item grew with backlog: %v at %d rows vs %v at %d", small.perItem(), small.collected, large.perItem(), large.collected)
	}
	// One page is a few hundred rows. A hold that grows with the whole backlog
	// is far above this; slow runners still have to stay under it.
	const hold = 200 * time.Millisecond
	if held.worst > hold {
		t.Errorf("GC cleanup held the writer: victim waited %v", held.worst)
	}
}

func measureDrain(t *testing.T, g int, compete bool) drainResult {
	t.Helper()
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
	var worst time.Duration
	if compete {
		al := s.Client(alice)
		af, ak := mkFolder(t, al, ds.Limits{})
		stop := make(chan struct{})
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			var once sync.Once
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				pid, _ := ds.PathID(ak, af.ID, fmt.Sprintf("v/%d-%d", seq.Add(1), i))
				i++
				start := time.Now()
				_, e := al.Commit(bg, af.ID, []ds.Mutation{{PathID: pid, Deleted: true}})
				if d := time.Since(start); d > worst {
					worst = d
				}
				once.Do(func() { close(ready) })
				if e != nil {
					t.Logf("victim error: %v", e)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(stop)
			wg.Wait()
			t.Fatal("victim write did not start")
		}
		start := time.Now()
		_ = s.CollectGarbage(bg) // one maintenance pass (5 s budget)
		pass := time.Since(start)
		close(stop)
		wg.Wait()
		f, e := ev.GetFolder(bg, ef.ID)
		if e != nil {
			t.Fatal(e)
		}
		return drainResult{n: g, pass: pass, collected: int64(g) - f.Usage.GarbageRows, worst: worst}
	}
	start := time.Now()
	_ = s.CollectGarbage(bg) // one maintenance pass (5 s budget)
	pass := time.Since(start)
	f, e := ev.GetFolder(bg, ef.ID)
	if e != nil {
		t.Fatal(e)
	}
	return drainResult{n: g, pass: pass, collected: int64(g) - f.Usage.GarbageRows, worst: worst}
}
