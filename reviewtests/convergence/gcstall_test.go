package convergence

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// With the documented RunGC(ctx, time.Second) worker and ~6,000 owners (one
// empty folder each), each GC pass holds the writer lock until its 5 s deadline
// and fails, then the next tick starts another. Unrelated tenants' requests
// stall for seconds and time out; the victim's garbage is never freed.
func TestRunGCStallsServerAtModestOwnerCount(t *testing.T) {
	n := 6000
	if v := os.Getenv("GC_OWNERS"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	s, m := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, vk := mkFolder(t, victim, ds.Limits{})
	seedOwners(t, s, m, n, nil)
	row := putFile(t, victim, vf, vk, "a.txt", 0, []byte("one"))
	putFile(t, victim, vf, vk, "a.txt", row.Version, []byte("two"))
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { defer close(done); s.RunGC(ctx, time.Second) }()
	var worst time.Duration
	failures, calls := 0, 0
	deadline := time.Now().Add(15 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		start := time.Now()
		_, e := tryPut(victim, vf, vk, fmt.Sprintf("w%d.txt", i), 0, []byte("x"))
		worst = max(worst, time.Since(start))
		calls++
		if e != nil {
			failures++
		}
	}
	cancel()
	<-done
	got, _ := victim.GetFolder(bg, vf.ID)
	t.Logf("owners=%d victim puts=%d failed=%d worst=%v garbageRows=%d", n, calls, failures, worst, got.Usage.GarbageRows)
	if failures > 0 || worst > time.Second || got.Usage.GarbageRows != 0 {
		t.Fatalf("background GC with %d unrelated owners stalled the victim: %d/%d failed, worst %v, garbage left %d", n, failures, calls, worst, got.Usage.GarbageRows)
	}
}
