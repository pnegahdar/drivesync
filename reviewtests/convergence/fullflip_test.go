package convergence

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

type compactBeforeSecondPage struct {
	base  http.RoundTripper
	s     *ds.Server
	calls atomic.Int32
	arm   atomic.Bool
}

func (c *compactBeforeSecondPage) RoundTrip(r *http.Request) (*http.Response, error) {
	if c.arm.Load() && r.URL.Path == "/rpc" && c.calls.Add(1) == 2 {
		if e := c.s.CompactTombstones(r.Context()); e != nil {
			return nil, e
		}
	}
	return c.base.RoundTrip(r)
}

// changesPage decides Full independently on every page. If compaction lands
// between pages, HTTPClient.Changes returns Full=true for a set whose first
// page was incremental, so live rows at or below the cursor are missing. Any
// client that (correctly) treats Full as authoritative would delete them.
func TestHTTPChangesFullFlipsMidPagination(t *testing.T) {
	m, _ := newServerMeta(t)
	s := ds.NewServer(m, ds.NewMemoryBlobStore())
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	s.TombstoneTTL = time.Hour
	owner := ds.Principal{Tenant: "t", Subject: "owner"}
	admin := s.Client(owner)
	f, k := mkFolder(t, admin, ds.Limits{})
	live := putFile(t, admin, f, k, "live.txt", 0, []byte("must stay")) // version 1
	tomb := func(prefix string, n int) {
		for i := 0; i < n; i += 256 {
			var muts []ds.Mutation
			for j := i; j < min(n, i+256); j++ {
				pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("%s/%d", prefix, j))
				muts = append(muts, ds.Mutation{PathID: pid, Deleted: true})
			}
			if _, e := admin.Commit(bg, f.ID, muts); e != nil {
				t.Fatal(e)
			}
		}
	}
	tomb("old", 10) // version 2: will expire
	cursor := uint64(1)
	now = now.Add(50 * time.Minute)
	tomb("new", 600) // versions 3..5: will not expire
	now = now.Add(20 * time.Minute)

	rt := &compactBeforeSecondPage{base: http.DefaultTransport, s: s}
	srv := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) { return owner, nil }))
	t.Cleanup(srv.Close)
	hc := ds.NewHTTPClient(srv.URL, nil)
	hc.HTTP = &http.Client{Transport: rt}
	rt.arm.Store(true)
	d, e := hc.Changes(bg, f.ID, cursor)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, r := range d.Rows {
		if r.PathID == live.PathID {
			found = true
		}
	}
	if d.Full && !found {
		t.Fatalf("Changes returned Full=true (horizon %d) with %d rows but without live row live.txt", d.Horizon, len(d.Rows))
	}
}
