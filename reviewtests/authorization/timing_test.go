package authorization

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

func randPID() string { var b [32]byte; rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func median(t *testing.T, n int, fn func()) time.Duration {
	ds := make([]time.Duration, n)
	for i := range ds {
		start := time.Now()
		fn()
		ds[i] = time.Since(start)
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[n/2]
}

// Finding: another tenant's outstanding unused tickets make every scoped
// transaction scan the global tickets table (no expression-index use).
func TestForeignTicketsSlowVictimTransactions(t *testing.T) {
	s, m := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "a"})
	vf, _ := mkFolder(t, victim, ds.Limits{})
	af, _ := mkFolder(t, attacker, ds.Limits{})
	measure := func() (time.Duration, time.Duration) {
		get := median(t, 101, func() {
			if _, e := victim.GetFolder(bg, vf.ID); e != nil {
				t.Fatal(e)
			}
		})
		res := median(t, 51, func() {
			tk, e := victim.Reserve(bg, vf.ID, ds.UploadRequest{PathID: randPID(), SealedSize: 41})
			if e != nil {
				t.Fatal(e)
			}
			if e = victim.CancelUpload(bg, vf.ID, tk.ID); e != nil {
				t.Fatal(e)
			}
		})
		return get, res
	}
	g0, r0 := measure()
	const n = 3000
	start := time.Now()
	// One public admission plus the same 3,000 authorized unused tickets in
	// one transaction keeps this scale regression affordable under -race.
	seed, e := attacker.Reserve(bg, af.ID, ds.UploadRequest{PathID: randPID(), SealedSize: 0})
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Transaction(bg, func(meta *ds.Metadata) error {
		for i := 1; i < n; i++ {
			tk := seed
			tk.ID = randPID()[:32]
			tk.BlobID = randPID()[:32]
			tk.PathID = randPID()
			meta.Tickets[tk.ID] = tk
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if got, e := attacker.GetFolder(bg, af.ID); e != nil || got.Usage.ReservedRows != n {
		t.Fatal("ticket cardinality", got.Usage, e)
	}
	t.Logf("attacker created %d unused tickets in %v", n, time.Since(start))
	g1, r1 := measure()
	t.Logf("victim GetFolder median %v -> %v; Reserve+Cancel median %v -> %v", g0, g1, r0, r1)
	if g1 > 5*g0 || r1 > 5*r0 {
		t.Errorf("victim latency scales with another tenant's tickets")
	}
}
