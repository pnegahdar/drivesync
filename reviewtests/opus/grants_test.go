package dsreview

import (
	"fmt"
	"strings"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Grants have no count cap and principals no length cap; every transaction on
// the server parses every folder record, so one tenant slows all tenants.
func TestHugeGrantsSlowEveryone(t *testing.T) {
	s := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, _ := mkFolder(t, victim, ds.Limits{})
	measure := func() time.Duration {
		start := time.Now()
		for i := 0; i < 5; i++ {
			victim.GetFolder(bg, vf.ID)
		}
		return time.Since(start) / 5
	}
	before := measure()
	attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "a"})
	af, _ := mkFolder(t, attacker, ds.Limits{MaxFiles: 1, MaxTotalBytes: 1})
	big := strings.Repeat("x", 1000_000) // fits the HTTP 1 MiB request cap
	for i := 0; i < 40; i++ {
		if e := attacker.Grant(bg, af.ID, ds.Principal{Tenant: fmt.Sprint(i), Subject: big}, ds.Reader); e == nil {
			t.Fatal("oversized principal accepted")
		}
	}
	after := measure()
	t.Logf("victim GetFolder latency before=%v after 40 x 1MB grants by another tenant=%v", before, after)
}
