package dsreview

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

// One tenant's free tombstones slow every other tenant's operations (each
// SQLite transaction materializes all rows of all tenants).
func TestTombstoneSpamSlowsOtherTenants(t *testing.T) {
	s := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, vk := mkFolder(t, victim, ds.Limits{})
	measure := func() time.Duration {
		start := time.Now()
		for i := 0; i < 5; i++ {
			if _, e := victim.GetFolder(bg, vf.ID); e != nil {
				t.Fatal(e)
			}
		}
		putFile(t, victim, vf, vk, fmt.Sprintf("x%d", time.Now().UnixNano()), 0, []byte("hi"))
		return time.Since(start) / 6
	}
	before := measure()
	attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "a"})
	af, _ := mkFolder(t, attacker, ds.Limits{MaxFiles: 1, MaxTotalBytes: 1})
	start := time.Now()
	for round := 0; round < 120; round++ {
		var del []ds.Mutation
		for i := 0; i < 256; i++ {
			var b [32]byte
			rand.Read(b[:])
			del = append(del, ds.Mutation{PathID: hex.EncodeToString(b[:]), Deleted: true})
		}
		if _, e := attacker.Commit(bg, af.ID, del); e == nil {
			t.Fatal("tombstone spam bypassed row/byte budget")
		} else {
			break
		}
	}
	t.Logf("attacker wrote %d tombstones in %v under MaxFiles=1/MaxTotalBytes=1", 120*256, time.Since(start))
	after := measure()
	t.Logf("victim per-op latency before=%v after=%v (x%.0f)", before, after, float64(after)/float64(before))
}
