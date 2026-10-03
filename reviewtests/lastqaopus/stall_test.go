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

// fillTombstones gives a folder n rows cheaply: deletes of never-existing paths
// are charged growth (one row each) and need no upload.
func fillTombstones(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, n int, prefix string) {
	t.Helper()
	for i := 0; i < n; i += 256 {
		var muts []ds.Mutation
		for j := i; j < min(i+256, n); j++ {
			pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("%s/%07d", prefix, j))
			muts = append(muts, ds.Mutation{PathID: pid, Deleted: true})
		}
		if _, e := c.Commit(bg, f.ID, muts); e != nil {
			t.Fatal(e)
		}
	}
}

func victimWrites(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, stop <-chan struct{}) (worst time.Duration, failures int) {
	i := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("v/%d-%d", seq.Add(1), i))
		i++
		start := time.Now()
		_, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, Deleted: true}})
		if d := time.Since(start); d > worst {
			worst = d
		}
		if e != nil {
			if failures == 0 {
				t.Logf("victim error: %v", e)
			}
			failures++
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Another tenant's whole-folder writer transaction (DeleteFolder or
// CompactTombstones) holds the single SQLite writer for time proportional to
// that tenant's row count. Victims' write latency grows with it.
func TestWholeFolderWriterTransactionsStallOtherTenants(t *testing.T) {
	n := 60000
	if v := os.Getenv("QA_ROWS"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	for _, op := range []string{"compact", "delete"} {
		t.Run(op, func(t *testing.T) {
			s, _, _ := newServer(t)
			clk := &clock{t: time.Unix(1_800_000_000, 0)}
			s.Now = clk.Now
			s.TombstoneTTL = time.Hour
			ev := s.Client(eve)
			ef, ek := mkFolder(t, ev, ds.Limits{})
			start := time.Now()
			fillTombstones(t, ev, ef, ek, n, "old")
			t.Logf("seeded %d rows in %v", n, time.Since(start))
			al := s.Client(alice)
			af, ak := mkFolder(t, al, ds.Limits{})
			// Baseline victim latency.
			stop := make(chan struct{})
			go func() { time.Sleep(300 * time.Millisecond); close(stop) }()
			base, _ := victimWrites(t, al, af, ak, stop)

			if op == "compact" {
				clk.Add(time.Hour + time.Second)
				// One fresh tombstone keeps every later GC pass selecting this folder.
			}
			stop = make(chan struct{})
			var wg sync.WaitGroup
			var worst time.Duration
			var failures int
			wg.Add(1)
			go func() { defer wg.Done(); worst, failures = victimWrites(t, al, af, ak, stop) }()
			time.Sleep(50 * time.Millisecond)
			opStart := time.Now()
			var e error
			if op == "compact" {
				e = s.CompactTombstones(bg)
			} else {
				e = s.DeleteFolder(bg, eve, ef.ID)
			}
			opTime := time.Since(opStart)
			time.Sleep(50 * time.Millisecond)
			close(stop)
			wg.Wait()
			t.Logf("%s of %d other-tenant rows: op=%v err=%v; victim baseline worst=%v, during worst=%v failures=%d", op, n, opTime, e, base, worst, failures)
			if worst > 20*base+100*time.Millisecond || failures > 0 {
				t.Errorf("victim write latency grows with another tenant's rows: %v vs baseline %v (failures %d)", worst, base, failures)
			}
			if op == "delete" && e != nil {
				t.Errorf("owner cannot delete a large folder: %v", e)
			}
		})
	}
}
