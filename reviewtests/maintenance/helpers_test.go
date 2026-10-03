package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/zeebo/blake3"
)

var bg = context.Background()
var seq atomic.Int64

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newServer(t testing.TB) (*ds.Server, *ds.SQLiteMetaStore, ds.BlobStore) {
	m, e := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	b := ds.NewMemoryBlobStore()
	return ds.NewServer(m, b), m, b
}

func mkFolder(t testing.TB, c ds.Client, l ds.Limits) (ds.Folder, ds.FolderKey) {
	t.Helper()
	k := ds.NewFolderKey()
	f, e := ds.CreateFolder(bg, c, ds.FolderSpec{Name: fmt.Sprintf("f%d", seq.Add(1)), Limits: l}, k)
	if e != nil {
		t.Fatal(e)
	}
	return f, k
}
func hash(b []byte) string { h := blake3.Sum256(b); return fmt.Sprintf("%x", h[:]) }

// upload reserves, uploads and returns a ready mutation.
func upload(c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data []byte) (ds.Mutation, error) {
	pid, e := ds.PathID(k, f.ID, p)
	if e != nil {
		return ds.Mutation{}, e
	}
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: base, SealedSize: ds.SealedSize(int64(len(data))), MetadataBytes: 512})
	if e != nil {
		return ds.Mutation{}, e
	}
	var sealed bytes.Buffer
	if e = ds.SealContent(&sealed, bytes.NewReader(data), k, f.ID, tk.BlobID, pid); e != nil {
		return ds.Mutation{}, e
	}
	if e = c.Upload(bg, f.ID, tk, &sealed); e != nil {
		return ds.Mutation{}, e
	}
	meta, e := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hash(data)})
	if e != nil {
		return ds.Mutation{}, e
	}
	return ds.Mutation{PathID: pid, BaseVersion: base, TicketID: tk.ID, Metadata: meta}, nil
}

func put(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data []byte) ds.Row {
	t.Helper()
	m, e := upload(c, f, k, p, base, data)
	if e != nil {
		t.Fatal(e)
	}
	d, e := c.Commit(bg, f.ID, []ds.Mutation{m})
	if e != nil {
		t.Fatal(e)
	}
	return d.Rows[0]
}

// mount serves s over HTTP; each principal gets its own bearer token.
func mount(t testing.TB, s *ds.Server, rt func(http.RoundTripper) http.RoundTripper) func(ds.Principal) *ds.HTTPClient {
	var mu sync.Mutex
	tokens := map[string]ds.Principal{}
	srv := httptest.NewServer(s.Handler(func(r *http.Request) (ds.Principal, error) {
		mu.Lock()
		defer mu.Unlock()
		p, ok := tokens[r.Header.Get("Authorization")]
		if !ok {
			return ds.Principal{}, ds.ErrDenied
		}
		return p, nil
	}))
	t.Cleanup(srv.Close)
	return func(p ds.Principal) *ds.HTTPClient {
		tok := fmt.Sprintf("Bearer t%d", seq.Add(1))
		mu.Lock()
		tokens[tok] = p
		mu.Unlock()
		c := ds.NewHTTPClient(srv.URL, http.Header{"Authorization": {tok}})
		if rt != nil {
			c.HTTP = &http.Client{Transport: rt(http.DefaultTransport)}
		}
		return c
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func attach(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, name string) (*ds.Replica, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	r, e := ds.Attach(bg, c, f.ID, k, dir, ds.Options{StateDir: filepath.Join(t.TempDir(), "state"), Name: name, Manual: true, RescanInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r, dir
}

func filesIn(t testing.TB, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if e := filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if d != nil && d.Name() == ".drivesync-root" {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return out
}

func readBody(r *http.Request) []byte {
	if r.Body == nil {
		return nil
	}
	b, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(b))
	return b
}

var (
	alice = ds.Principal{Tenant: "t1", Subject: "alice"}
	bob   = ds.Principal{Tenant: "t1", Subject: "bob"}
	eve   = ds.Principal{Tenant: "t2", Subject: "eve"}
)
