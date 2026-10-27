package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type changesCounter struct {
	Client
	rows          atomic.Int64
	snapshots     atomic.Int64
	beforeCatchup func()
	noSnapshot    bool
}

func (c *changesCounter) Changes(ctx context.Context, folder string, after uint64) (Delta, error) {
	d, e := c.Client.Changes(ctx, folder, after)
	c.rows.Add(int64(len(d.Rows)))
	return d, e
}
func (c *changesCounter) Snapshot(ctx context.Context, folder string) (uint64, string, error) {
	c.snapshots.Add(1)
	if c.noSnapshot {
		return 0, "", ErrInvalid
	}
	v, u, e := c.Client.Snapshot(ctx, folder)
	if c.beforeCatchup != nil {
		fn := c.beforeCatchup
		c.beforeCatchup = nil
		fn()
	}
	return v, u, e
}

func TestReplicaSnapshotCatchupAndFallback(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			s, _ := testServer(t)
			s.SnapshotRows = 1
			store := &signedTestStore{BlobStore: s.Blobs}
			var gets atomic.Int64
			store.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				gets.Add(1)
				if req.Header.Get("Authorization") != "" || req.Header.Get("X-Authority") != "" {
					t.Error("authority credentials in snapshot GET")
				}
				if bad && strings.HasSuffix(req.URL.Path, "/snapshot") {
					io.WriteString(w, "bad snapshot https://secret.invalid/?credential=private")
					return
				}
				parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
				stream, e := store.Open(req.Context(), parts[0], parts[1])
				if e != nil {
					http.Error(w, "missing", 404)
					return
				}
				defer stream.Close()
				io.Copy(w, stream)
			}))
			defer store.server.Close()
			s.Blobs = store
			authority := httptest.NewServer(s.Handler(func(*http.Request) (Principal, error) { return owner, nil }))
			defer authority.Close()
			base := NewHTTPClient(authority.URL, http.Header{"Authorization": {"Bearer private"}, "X-Authority": {"secret"}})
			f, k := folderFor(t, base, Limits{})
			row := putRemote(t, base, f, k, "one", 0, []byte("old"), 0600)
			putRemote(t, base, f, k, "two", 0, []byte("two"), 0600)
			c := &changesCounter{Client: base}
			c.beforeCatchup = func() { putRemote(t, base, f, k, "one", row.Version, []byte("new"), 0600) }
			r := lazyReplica(t, c, f, k)
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			want := int64(1)
			if bad {
				want = 2
			}
			if c.rows.Load() != want || gets.Load() != 1 {
				t.Fatal("feed rows", c.rows.Load(), "gets", gets.Load())
			}
			fetchOK(t, r, "one")
			if r.index["one"].Size != 3 || strings.Contains(fmt.Sprint(r.Status()), "credential=") {
				t.Fatal(r.Status())
			}
		})
	}
}

func TestReplicaOldAuthoritySnapshotOnlyOnce(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	c := &changesCounter{Client: base, noSnapshot: true}
	r := lazyReplica(t, c, f, k)
	for n := 0; n < 3; n++ {
		if e := r.Sync(syncCtx); e != nil {
			t.Fatal(e)
		}
	}
	if c.snapshots.Load() != 1 {
		t.Fatal(c.snapshots.Load())
	}
}
