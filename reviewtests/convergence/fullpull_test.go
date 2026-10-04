package convergence

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// compactingClient runs background compaction exactly when the replica issues
// its incremental pull, i.e. after Sync's initial GetFolder horizon check.
type compactingClient struct {
	ds.Client
	s    *ds.Server
	t    *testing.T
	once sync.Once
	arm  bool
}

func (c *compactingClient) Changes(ctx context.Context, id string, after uint64) (ds.Delta, error) {
	if c.arm && after > 0 {
		c.once.Do(func() {
			if e := c.s.CollectGarbage(ctx); e != nil {
				c.t.Fatal(e)
			}
		})
	}
	return c.Client.Changes(ctx, id, after)
}

// Sync checks the horizon only at its start. If compaction lands between that
// check and the incremental pull, Changes returns Full=true without the
// compacted tombstone; the replica ignores Full and advances its cursor, so the
// remote deletion is never applied.
func TestPullIgnoresFullDelta(t *testing.T) {
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
			wrapped := &compactingClient{Client: c, s: s, t: t}
			a, b := attach(t, c, f, k, "a"), attach(t, wrapped, f, k, "b")
			write(t, a, "keep.txt", "keep")
			write(t, a, "gone.txt", "deleted elsewhere")
			syncAll(t, a, b)
			if e := os.Remove(filepath.Join(a.dir, "gone.txt")); e != nil {
				t.Fatal(e)
			}
			if e := a.Sync(bg); e != nil {
				t.Fatal(e)
			}
			now = now.Add(31 * 24 * time.Hour) // tombstone expires; GC runs during b's sync
			wrapped.arm = true
			for i := 0; i < 3; i++ {
				if e := b.Sync(bg); e != nil {
					t.Log("sync:", e)
				}
			}
			if _, ok := files(t, b)["gone.txt"]; ok {
				st, _ := c.GetFolder(bg, f.ID)
				t.Fatalf("b kept a remotely deleted file after compaction during sync: b=%v a=%v horizon=%d b.version=%d", files(t, b), files(t, a), st.Horizon, b.Status().Version)
			}
		})
	}
}
