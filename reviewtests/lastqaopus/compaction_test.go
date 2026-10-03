package qa

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// An incremental pull that straddles tombstone compaction silently drops the
// deletes on later pages. The replica's cursor then sits at/above the new
// horizon, so it never reconciles: deleted files stay on that node forever.
func TestCompactionBetweenIncrementalPagesLosesDeletes(t *testing.T) {
	s, _, _ := newServer(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	s.Now = clk.Now
	s.TombstoneTTL = time.Hour
	a := s.Client(alice)
	f, k := mkFolder(t, a, ds.Limits{})

	const n = 600
	rows := map[string]uint64{}
	var batch []ds.Mutation
	paths := []string{}
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("d/f%04d.txt", i)
		m, e := upload(a, f, k, p, 0, []byte(p))
		if e != nil {
			t.Fatal(e)
		}
		batch = append(batch, m)
		paths = append(paths, p)
		if len(batch) == 200 || i == n-1 {
			d, e := a.Commit(bg, f.ID, batch)
			if e != nil {
				t.Fatal(e)
			}
			for _, r := range d.Rows {
				rows[r.PathID] = r.Version
			}
			batch = nil
		}
	}

	var armed, fired atomic.Bool
	client := mount(t, s, func(next http.RoundTripper) http.RoundTripper {
		return rtFunc(func(r *http.Request) (*http.Response, error) {
			body := readBody(r)
			resp, e := next.RoundTrip(r)
			if e != nil || !armed.Load() || !bytes.Contains(body, []byte(`"Op":"changes"`)) || !bytes.Contains(body, []byte(`"Page":""`)) || fired.Load() {
				return resp, e
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			resp.Body = io.NopCloser(bytes.NewReader(b))
			fired.Store(true)
			// Background maintenance happens to run between page 1 and page 2.
			if ce := s.CompactTombstones(bg); ce != nil {
				t.Errorf("compact: %v", ce)
			}
			return resp, nil
		})
	})(alice)
	b, bdir := attach(t, client, f, k, "b")
	if e := b.Sync(bg); e != nil {
		t.Fatal(e)
	}
	if got := len(filesIn(t, bdir)); got != n {
		t.Fatalf("initial sync: %d files", got)
	}

	// Writer deletes everything; GC releases the tombstones' blobs.
	for i := 0; i < n; i += 200 {
		var dels []ds.Mutation
		for _, p := range paths[i:min(i+200, n)] {
			pid, _ := ds.PathID(k, f.ID, p)
			dels = append(dels, ds.Mutation{PathID: pid, BaseVersion: rows[pid], Deleted: true})
		}
		if _, e := a.Commit(bg, f.ID, dels); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.CollectGarbage(bg); e != nil {
		t.Fatal(e)
	}
	// Node b pulls exactly when the 30-day (here 1h) retention boundary passes.
	clk.Add(time.Hour + time.Second)
	armed.Store(true)
	if e := b.Sync(bg); e != nil {
		t.Logf("sync: %v", e)
	}
	if !fired.Load() {
		t.Fatal("hook did not fire")
	}
	for i := 0; i < 3; i++ {
		_ = b.Sync(bg)
	}
	remote, e := a.Changes(bg, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	left := filesIn(t, bdir)
	t.Logf("server rows=%d horizon=%d version=%d; replica keeps %d deleted files", len(remote.Rows), remote.Horizon, remote.Version, len(left))
	if len(left) != 0 {
		t.Fatalf("replica never applies %d remote deletes after compaction raced its incremental pull", len(left))
	}
}
