package confirmreview

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// An up-to-date replica that observed a deletion keeps the tombstone version in
// its index. After compaction removes the tombstone (and the replica's cursor is
// already at or beyond the new horizon, so no reconcile runs), recreating that
// path reserves with the stale tombstone base and is turned into a conflict copy.
func TestRecreateAfterCompactionOnCurrentReplica(t *testing.T) {
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			s, _ := newServer(t)
			now := time.Now().UTC()
			s.Now = func() time.Time { return now }
			p := ds.Principal{Tenant: "t", Subject: "owner"}
			var c ds.Client = s.Client(p)
			if transport == "http" {
				c = httpClients(t, s)(p)
			}
			f, k := mkFolder(t, c, ds.Limits{})
			a, b := attach(t, c, f, k, "a"), attach(t, c, f, k, "b")
			write(t, a, "notes.txt", "v1")
			syncAll(t, a, b)
			if e := os.Remove(filepath.Join(a.dir, "notes.txt")); e != nil {
				t.Fatal(e)
			}
			syncAll(t, a, b)
			now = now.Add(31 * 24 * time.Hour)
			if e := s.CollectGarbage(bg); e != nil {
				t.Fatal(e)
			}
			syncAll(t, a, b) // both replicas are current; cursor >= horizon
			for attempt := 0; attempt < 2; attempt++ {
				write(t, a, "notes.txt", "recreated")
				if e := a.Sync(bg); e != nil {
					t.Log("sync error:", e)
				}
			}
			syncAll(t, a, b)
			if got := files(t, a); got["notes.txt"] != "recreated" {
				t.Fatalf("replica a cannot recreate a compacted path; local files=%v conflicts=%v", got, a.Status().Conflicts)
			}
			if got := files(t, b); got["notes.txt"] != "recreated" {
				t.Fatalf("recreated path never reached b; b files=%v", got)
			}
		})
	}
}
