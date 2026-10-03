package finalreview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Idle long-polls each re-query SQLite every 250 ms and on every commit anywhere.
func TestIdleWaitersSlowOtherTenants(t *testing.T) {
	s, _ := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "a"})
	vf, vk := mkFolder(t, victim, ds.Limits{})
	af, _ := mkFolder(t, attacker, ds.Limits{})
	i := 0
	op := func() {
		i++
		putFile(t, victim, vf, vk, time.Now().Format("150405.000000000"), 0, []byte("x"))
	}
	before := median(t, 21, op)
	ctx, cancel := context.WithCancel(bg)
	var wg sync.WaitGroup
	var busy atomic.Int32
	for n := 0; n < 2000; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := attacker.Wait(ctx, af.ID, 0)
			if e == ds.ErrWaitLimit {
				busy.Add(1)
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	after := median(t, 21, op)
	cancel()
	wg.Wait()
	if busy.Load() == 0 {
		t.Fatal("concurrent waits were not capped")
	}
	t.Logf("victim put median %v -> %v with 2000 idle attacker waits", before, after)
}
