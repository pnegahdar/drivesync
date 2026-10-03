package confirmreview

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

// Every transaction (including GetFolder and denied requests) first takes the
// single SQLite writer lock, and every folder-scoped write decodes all rows of
// that folder while holding it. An unrelated tenant's read latency therefore
// grows linearly with the size of whichever other tenant is writing.
func TestVictimLatencyGrowsWithOtherTenantRows(t *testing.T) {
	rows := 20480
	if v := os.Getenv("BIG_ROWS"); v != "" {
		rows, _ = strconv.Atoi(v)
	}
	s, m := newServer(t)
	big := s.Client(ds.Principal{Tenant: "big", Subject: "x"})
	bf, bk := mkFolder(t, big, ds.Limits{})
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, _ := mkFolder(t, victim, ds.Limits{})

	measure := func() (time.Duration, time.Duration) {
		stop := make(chan struct{})
		commits := make(chan time.Duration, 1)
		go func() {
			var worst time.Duration
			for i := 0; ; i++ {
				select {
				case <-stop:
					commits <- worst
					return
				default:
				}
				pid, _ := ds.PathID(bk, bf.ID, fmt.Sprintf("probe/%d-%d", time.Now().UnixNano(), i))
				start := time.Now()
				if _, e := big.Commit(bg, bf.ID, []ds.Mutation{{PathID: pid, Deleted: true}}); e != nil {
					t.Error(e)
				}
				worst = max(worst, time.Since(start))
			}
		}()
		var lat []time.Duration
		for i := 0; i < 40; i++ {
			start := time.Now()
			if _, e := victim.GetFolder(bg, vf.ID); e != nil {
				t.Fatal(e)
			}
			lat = append(lat, time.Since(start))
			time.Sleep(5 * time.Millisecond)
		}
		close(stop)
		w := <-commits
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		return lat[len(lat)*9/10], w
	}
	p90Small, commitSmall := measure()
	// Grow the other tenant's folder with tombstones, 256 per CAS batch.
	seedTombstones(t, m, big, bf, bk, "seed", rows)
	p90Big, commitBig := measure()
	t.Logf("other-tenant rows=%d victim GetFolder p90: small=%v big=%v; other tenant commit worst: small=%v big=%v", rows, p90Small, p90Big, commitSmall, commitBig)
	if p90Big > 10*p90Small+20*time.Millisecond {
		t.Fatalf("victim p90 latency grew from %v to %v with another tenant's %d rows", p90Small, p90Big, rows)
	}
}
