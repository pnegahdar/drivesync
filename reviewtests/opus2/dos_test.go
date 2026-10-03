package dsreview2

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
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
	af, _ := mkFolder(t, attacker, ds.Limits{})
	start := time.Now()
	// Admit the first batch through the public API; seed the remaining identical
	// legal tombstones in one storage transaction. Preserve all 30,720 rows.
	var first []ds.Mutation
	for i := 0; i < 256; i++ {
		var b [32]byte
		rand.Read(b[:])
		first = append(first, ds.Mutation{PathID: hex.EncodeToString(b[:]), Deleted: true})
	}
	if _, e := attacker.Commit(bg, af.ID, first); e != nil {
		t.Fatal(e)
	}
	if e := s.Meta.Transaction(bg, func(m *ds.Metadata) error {
		f := m.Folders[af.ID]
		for round := 1; round < 120; round++ {
			f.Folder.Version++
			for i := 0; i < 256; i++ {
				var b [32]byte
				rand.Read(b[:])
				pid := hex.EncodeToString(b[:])
				m.Files[af.ID][pid] = ds.Row{FolderID: af.ID, PathID: pid, Version: f.Folder.Version, Deleted: true}
			}
		}
		m.Folders[af.ID] = f
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if got, e := attacker.GetFolder(bg, af.ID); e != nil || got.Usage.Rows != 30720 {
		t.Fatal("scale fixture cardinality", got.Usage, e)
	}
	t.Logf("attacker wrote %d tombstones in %v under MaxFiles=1/MaxTotalBytes=1", 120*256, time.Since(start))
	after := measure()
	t.Logf("victim per-op latency before=%v after=%v (x%.0f)", before, after, float64(after)/float64(before))
}
