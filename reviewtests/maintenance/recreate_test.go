package maintenance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

type countingClient struct {
	*ds.InProcessClient
	changes atomic.Int64
	rows    atomic.Int64
}

func (c *countingClient) Changes(ctx context.Context, id string, after uint64) (ds.Delta, error) {
	c.changes.Add(1)
	d, e := c.InProcessClient.Changes(ctx, id, after)
	c.rows.Add(int64(len(d.Rows)))
	return d, e
}

// A replica that never indexed a tombstone (fresh attach, or attached after the
// delete) re-downloads the entire folder listing once per recreated file.
func TestRecreatingUnindexedTombstonesScansWholeFolderPerFile(t *testing.T) {
	s, _, _ := newServer(t)
	a := s.Client(alice)
	f, k := mkFolder(t, a, ds.Limits{})
	const n = 300
	// History: n build outputs created then cleaned on another node, plus
	// unrelated rows that make each full listing expensive.
	fillTombstones(t, a, f, k, 3000, "unrelated")
	for i := 0; i < n; i += 100 {
		var muts []ds.Mutation
		for j := i; j < i+100; j++ {
			m, e := upload(a, f, k, fmt.Sprintf("out/%04d.o", j), 0, []byte{byte(j)})
			if e != nil {
				t.Fatal(e)
			}
			muts = append(muts, m)
		}
		d, e := a.Commit(bg, f.ID, muts)
		if e != nil {
			t.Fatal(e)
		}
		var dels []ds.Mutation
		for _, r := range d.Rows {
			dels = append(dels, ds.Mutation{PathID: r.PathID, BaseVersion: r.Version, Deleted: true})
		}
		if _, e = a.Commit(bg, f.ID, dels); e != nil {
			t.Fatal(e)
		}
	}
	cc := &countingClient{InProcessClient: s.Client(alice)}
	r, dir := attach(t, cc, f, k, "fresh")
	if e := r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, "out", fmt.Sprintf("%04d.o", i))
		_ = os.MkdirAll(filepath.Dir(p), 0700)
		if e := os.WriteFile(p, []byte("rebuilt"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	cc.changes.Store(0)
	cc.rows.Store(0)
	if e := r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	t.Logf("one sync recreating %d paths: %d Changes calls returning %d rows", n, cc.changes.Load(), cc.rows.Load())
	if cc.changes.Load() > 10 {
		t.Fatalf("recreating %d previously deleted paths cost %d full-folder listings (%d rows)", n, cc.changes.Load(), cc.rows.Load())
	}
}
