package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lazyCounter struct {
	Client
	downloads, reserves, uploads atomic.Int64
}

func (c *lazyCounter) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	c.downloads.Add(1)
	return c.Client.Download(ctx, f, b)
}
func (c *lazyCounter) Reserve(ctx context.Context, f string, u UploadRequest) (Ticket, error) {
	c.reserves.Add(1)
	return c.Client.Reserve(ctx, f, u)
}
func (c *lazyCounter) Upload(ctx context.Context, f string, t Ticket, r io.Reader) error {
	c.uploads.Add(1)
	return c.Client.Upload(ctx, f, t, r)
}
func lazyReplica(t *testing.T, c Client, f Folder, k FolderKey) *Replica {
	t.Helper()
	d := t.TempDir()
	r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(d, "files"), Options{Manual: true, OnDemand: true, StateDir: filepath.Join(d, "state")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func assertPlaceholder(t *testing.T, r *Replica, p string) {
	t.Helper()
	info, e := os.Lstat(filepath.Join(r.dir, p))
	if e != nil {
		t.Fatal(e)
	}
	if !info.Mode().IsRegular() || info.Size() != 0 || info.Mode().Perm() != 0 {
		t.Fatalf("not a placeholder %s: %v", p, info)
	}
}
func fetchOK(t *testing.T, r *Replica, p ...string) FetchResult {
	t.Helper()
	got, e := r.Fetch(syncCtx, p...)
	if e != nil || len(got.Failed) != 0 {
		t.Fatalf("fetch: %+v %v", got, e)
	}
	return got
}
func TestOnDemandLifecycle(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	writer := replicaFor(t, base, f, k, "source", true)
	for _, p := range []string{"one", "dir/two", "dir/three", "other/four"} {
		writeLocal(t, writer, p, "content:"+p)
		if e := os.Chmod(filepath.Join(writer.dir, p), 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e := writer.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	rows := map[string]Row{}
	for _, p := range []string{"one", "dir/two", "dir/three", "other/four"} {
		rows[p] = Row{Version: writer.index[p].Version}
	}
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.downloads.Load() != 0 {
		t.Fatalf("attach downloads %d", c.downloads.Load())
	}
	for p := range rows {
		assertPlaceholder(t, r, p)
	}
	entries, e := r.Entries("./dir/")
	if e != nil || len(entries) != 3 || entries[1].Size != int64(len("content:dir/three")) {
		t.Fatalf("entries %+v %v", entries, e)
	}
	if got := r.Status(); got.Placeholders != 4 || got.PlaceholderBytes != 61 {
		t.Fatalf("status %+v", got)
	}
	for n := 0; n < 3; n++ {
		if e := r.Sync(syncCtx); e != nil {
			t.Fatal(e)
		}
	}
	if c.reserves.Load() != 0 || c.uploads.Load() != 0 {
		t.Fatal("uploaded placeholders")
	}
	got := fetchOK(t, r, "one")
	if !reflect.DeepEqual(got.Fetched, []string{"one"}) {
		t.Fatal(got)
	}
	got = fetchOK(t, r, "dir")
	if len(got.Fetched) != 2 {
		t.Fatal(got)
	}
	got = fetchOK(t, r, "**/f?ur", "missing")
	if len(got.Fetched) != 1 || !reflect.DeepEqual(got.Unmatched, []string{"missing"}) {
		t.Fatal(got)
	}
	got = fetchOK(t, r)
	if got.Present != 4 || len(got.Fetched) != 0 || c.downloads.Load() != 4 {
		t.Fatal(got, c.downloads.Load())
	}
	for p := range rows {
		b, e := os.ReadFile(filepath.Join(r.dir, p))
		if e != nil || string(b) != "content:"+p {
			t.Fatal(p, string(b), e)
		}
		info, _ := os.Stat(filepath.Join(r.dir, p))
		if info.Mode()&0100 == 0 {
			t.Fatal("lost execute bit")
		}
	}
	if r.Status().Placeholders != 0 {
		t.Fatal(r.Status())
	}
	// Downloaded files continue to follow remote updates.
	putRemote(t, base, f, k, "one", rows["one"].Version, []byte("new"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.downloads.Load() != 5 {
		t.Fatal(c.downloads.Load())
	}
}
func TestOnDemandUpdatesDeletesAndUnlock(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	row := putRemote(t, base, f, k, "file", 0, []byte("v1"), 0600)
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	row = putRemote(t, base, f, k, "file", row.Version, []byte("version two"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	assertPlaceholder(t, r, "file")
	if c.downloads.Load() != 0 {
		t.Fatal(c.downloads.Load())
	}
	if e := os.Chmod(filepath.Join(r.dir, "file"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if b, _ := os.ReadFile(filepath.Join(r.dir, "file")); string(b) != "version two" {
		t.Fatal(string(b))
	}
	if c.uploads.Load() != 0 {
		t.Fatal("chmod uploaded empty bytes")
	}
	// A write into an unlocked placeholder publishes real local bytes.
	putRemote(t, base, f, k, "write", 0, []byte("old"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(filepath.Join(r.dir, "write"), 0600); e != nil {
		t.Fatal(e)
	}
	writeLocal(t, r, "write", "local")
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.uploads.Load() != 1 {
		t.Fatal(c.uploads.Load())
	}
	putRemote(t, base, f, k, "delete", 0, []byte("delete"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	pid, _ := PathID(k, f.ID, "delete")
	_, e := base.Commit(syncCtx, f.ID, []Mutation{{PathID: pid, BaseVersion: r.index["delete"].Version, Deleted: true}})
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Lstat(filepath.Join(r.dir, "delete")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
	putRemote(t, base, f, k, "remove", 0, []byte("remove"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(r.dir, "remove")); e != nil {
		t.Fatal(e)
	}
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if !r.index["remove"].Deleted {
		t.Fatal("local remove not propagated")
	}
}
func TestOnDemandMovesCopiesAndModeSwitch(t *testing.T) {
	for _, folder := range []bool{false, true} {
		t.Run(fmt.Sprint(folder), func(t *testing.T) {
			s, _ := testServer(t)
			base := s.Client(owner)
			f, k := folderFor(t, base, Limits{})
			writer := replicaFor(t, base, f, k, "writer", true)
			if folder {
				writeLocal(t, writer, "old/a", "aaa")
				writeLocal(t, writer, "old/b", "bbb")
			} else {
				writeLocal(t, writer, "old", "aaa")
			}
			if e := writer.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			c := &lazyCounter{Client: base}
			r := lazyReplica(t, c, f, k)
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			before := c.downloads.Load()
			if e := os.Rename(filepath.Join(r.dir, "old"), filepath.Join(r.dir, "new")); e != nil {
				t.Fatal(e)
			}
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			if folder {
				if b, _ := os.ReadFile(filepath.Join(r.dir, "new/a")); string(b) != "aaa" {
					t.Fatal(string(b))
				}
			} else {
				if b, _ := os.ReadFile(filepath.Join(r.dir, "new")); string(b) != "aaa" {
					t.Fatal(string(b))
				}
			}
			delta, e := base.Changes(syncCtx, f.ID, 0)
			if e != nil {
				t.Fatal(e)
			}
			versions := map[uint64]int{}
			for _, row := range delta.Rows {
				versions[row.Version]++
			}
			for _, i := range r.index {
				if strings.HasPrefix(i.Path, "old") && i.Deleted && versions[i.Version] < 2 {
					t.Fatalf("delete-only rename %+v", i)
				}
			}
			if c.downloads.Load() == before {
				t.Fatal("move did not fetch")
			}
		})
	}
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "original", 0, []byte("contents"), 0600)
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(r.dir, "copy"), nil, 0); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.uploads.Load() != 0 || len(r.Status().Skipped) == 0 {
		t.Fatal("copy uploaded", r.Status())
	}
	// Reattachment keeps the persisted placeholders and changing the option hydrates them.
	dir, state := r.dir, r.opts.StateDir
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
	again, e := Attach(syncCtx, c, f.ID, k, dir, Options{Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	defer again.Close()
	if e = again.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if again.Status().Placeholders != 0 {
		t.Fatal(again.Status())
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "original")); string(b) != "contents" {
		t.Fatal(string(b))
	}
}

type parallelDownloadClient struct {
	Client
	target  int
	calls   atomic.Int64
	active  atomic.Int64
	arrived chan struct{}
	once    sync.Once
}

func (c *parallelDownloadClient) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	c.calls.Add(1)
	n := c.active.Add(1)
	defer c.active.Add(-1)
	if n >= int64(c.target) {
		c.once.Do(func() { close(c.arrived) })
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.arrived:
	}
	return c.Client.Download(ctx, f, b)
}
func TestDownloadPoolAndConcurrentFetch(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		for _, n := range []int{1, 4} {
			t.Run(fmt.Sprint(lazy, n), func(t *testing.T) {
				s, _ := testServer(t)
				base := s.Client(owner)
				f, k := folderFor(t, base, Limits{})
				for i := 0; i < 4; i++ {
					putRemote(t, base, f, k, fmt.Sprint(i), 0, []byte("data"), 0600)
				}
				c := &parallelDownloadClient{Client: base, target: n, arrived: make(chan struct{})}
				d := t.TempDir()
				r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(d, "files"), Options{Manual: true, OnDemand: lazy, Downloads: n, StateDir: filepath.Join(d, "state")})
				if e != nil {
					t.Fatal(e)
				}
				defer r.Close()
				ctx, cancel := context.WithTimeout(syncCtx, 5*time.Second)
				defer cancel()
				if e = r.Sync(ctx); e != nil {
					t.Fatal(e)
				}
				if lazy {
					var wg sync.WaitGroup
					for i := 0; i < 2; i++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							res, e := r.Fetch(ctx)
							if e != nil || len(res.Failed) != 0 {
								t.Errorf("%+v %v", res, e)
							}
						}()
					}
					wg.Wait()
				}
				if c.calls.Load() != int64(4) {
					t.Fatalf("duplicate download: %d for %d files", c.calls.Load(), 4)
				}
				if c.active.Load() != 0 {
					t.Fatal("workers leaked")
				}
				files, _ := filepath.Glob(filepath.Join(r.opts.StateDir, "download-*"))
				if len(files) != 0 {
					t.Fatal(files)
				}
			})
		}
	}
}

type signedTestStore struct {
	BlobStore
	server   *httptest.Server
	bad      atomic.Bool
	requests atomic.Int64
}

func (s *signedTestStore) DownloadURL(ctx context.Context, f, b string, ttl time.Duration) (string, error) {
	if ttl != 5*time.Minute {
		return "", ErrInvalid
	}
	return s.server.URL + "/" + f + "/" + b + "?secret=DO-NOT-LEAK", nil
}
func TestDirectDownloads(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			s, _ := testServer(t)
			store := &signedTestStore{BlobStore: s.Blobs}
			store.bad.Store(bad)
			store.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				store.requests.Add(1)
				if r.Header.Get("Authorization") != "" || r.Header.Get("X-Authority") != "" {
					t.Error("authority credentials leaked")
				}
				if store.bad.Load() {
					http.Error(w, "bad", 500)
					return
				}
				parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
				stream, e := store.Open(r.Context(), parts[0], parts[1])
				if e != nil {
					t.Error(e)
					return
				}
				defer stream.Close()
				io.Copy(w, stream)
			}))
			defer store.server.Close()
			s.Blobs = store
			authority := httptest.NewServer(s.Handler(func(r *http.Request) (Principal, error) { return owner, nil }))
			defer authority.Close()
			base := NewHTTPClient(authority.URL, http.Header{"Authorization": {"Bearer authority"}, "X-Authority": {"private"}})
			f, k := folderFor(t, base, Limits{})
			row := putRemote(t, base, f, k, "file", 0, []byte("data"), 0600)
			c := &lazyCounter{Client: base}
			r := lazyReplica(t, c, f, k)
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			fetchOK(t, r)
			want := int64(0)
			if bad {
				want = 1
			}
			if c.downloads.Load() != want || store.requests.Load() != 1 {
				t.Fatal(c.downloads.Load(), store.requests.Load())
			}
			denied := s.Client(Principal{"else", "reader"})
			if _, e := denied.DownloadURLs(syncCtx, f.ID, []string{row.BlobID}); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			if _, e := base.DownloadURLs(syncCtx, f.ID, make([]string, 257)); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
		})
	}
}

func TestOnDemandMassRemovalAndRemoteRename(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	writer := replicaFor(t, base, f, k, "writer", true)
	for n := 0; n < 5; n++ {
		writeLocal(t, writer, fmt.Sprint(n), strings.Repeat("data", n+1))
	}
	if e := writer.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(writer.dir, "0"), filepath.Join(writer.dir, "renamed")); e != nil {
		t.Fatal(e)
	}
	if e := writer.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	assertPlaceholder(t, r, "renamed")
	if c.downloads.Load() != 0 {
		t.Fatal("remote rename downloaded", c.downloads.Load())
	}
	for n := 1; n < 5; n++ {
		if e := os.Remove(filepath.Join(r.dir, fmt.Sprint(n))); e != nil {
			t.Fatal(e)
		}
	}
	if e := r.Sync(syncCtx); e == nil || !strings.Contains(e.Error(), "deletes paused") {
		t.Fatal(e)
	}
	for n := 1; n < 5; n++ {
		if r.index[fmt.Sprint(n)].Deleted {
			t.Fatal("mass delete propagated")
		}
	}
	r.RetryRejected()
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
}

type oldURLClient struct {
	Client
	calls atomic.Int64
}

func (c *oldURLClient) DownloadURLs(ctx context.Context, f string, b []string) ([]string, error) {
	c.calls.Add(1)
	return nil, ErrInvalid
}
func TestOldAuthorityAndURLRedaction(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "a", 0, []byte("data"), 0600)
	putRemote(t, base, f, k, "b", 0, []byte("data"), 0600)
	old := &oldURLClient{Client: base}
	r := lazyReplica(t, old, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	fetchOK(t, r, "a")
	fetchOK(t, r, "b")
	if old.calls.Load() != 1 {
		t.Fatal(old.calls.Load())
	}
	// Both the direct endpoint and proxy fail; status must never retain the URL.
	store := &signedTestStore{BlobStore: s.Blobs}
	store.server = httptest.NewServer(http.NotFoundHandler())
	store.server.Close()
	s.Blobs = store
	putRemote(t, base, f, k, "down", 0, []byte("data"), 0600)
	broken := &interruptedDownload{Client: base, cut: true}
	other := lazyReplica(t, broken, f, k)
	if e := other.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	got, e := other.Fetch(syncCtx, "down")
	if e != nil || len(got.Failed) != 1 {
		t.Fatal(got, e)
	}
	for _, e := range append(other.Status().Errors, got.Failed[0].Reason) {
		if strings.Contains(e, "DO-NOT-LEAK") || strings.Contains(e, store.server.URL) {
			t.Fatal("URL leaked", e)
		}
	}
}

type gatedDownloadClient struct {
	Client
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (c *gatedDownloadClient) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	c.calls.Add(1)
	c.once.Do(func() { close(c.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.proceed:
	}
	return c.Client.Download(ctx, f, b)
}
func TestFetchPreservesReplacementAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			s, _ := testServer(t)
			base := s.Client(owner)
			f, k := folderFor(t, base, Limits{})
			putRemote(t, base, f, k, "file", 0, []byte("remote"), 0600)
			c := &gatedDownloadClient{Client: base, entered: make(chan struct{}), proceed: make(chan struct{})}
			r := lazyReplica(t, c, f, k)
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(syncCtx)
			defer cancel()
			done := make(chan FetchResult, 1)
			go func() { res, _ := r.Fetch(ctx, "file"); done <- res }()
			<-c.entered
			if cancelled {
				cancel()
			} else {
				if e := os.Remove(filepath.Join(r.dir, "file")); e != nil {
					t.Fatal(e)
				}
				writeLocal(t, r, "file", "local")
				close(c.proceed)
			}
			got := <-done
			if len(got.Failed) != 1 {
				t.Fatal(got)
			}
			if !cancelled {
				if b, _ := os.ReadFile(filepath.Join(r.dir, "file")); string(b) != "local" {
					t.Fatal(string(b))
				}
			}
			stages, _ := filepath.Glob(filepath.Join(r.opts.StateDir, "download-*"))
			if len(stages) != 0 {
				t.Fatal(stages)
			}
		})
	}
}

func TestPlaceholderChmodWatcher(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "placeholder")
	if e := os.WriteFile(file, nil, 0); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(syncCtx)
	defer cancel()
	wake := make(chan struct{}, 1)
	stop, kind, e := watchDirectory(ctx, dir, wake)
	if e != nil {
		t.Fatal(e)
	}
	defer stop()
	if kind == "polling" {
		t.Fatal("native watcher unavailable")
	}
	if e := os.Chmod(file, 0600); e != nil {
		t.Fatal(e)
	}
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("chmod did not wake watcher")
	}
}

type countingURLs struct {
	Client
	calls atomic.Int64
}

func (c *countingURLs) DownloadURLs(ctx context.Context, f string, b []string) ([]string, error) {
	c.calls.Add(1)
	return c.Client.DownloadURLs(ctx, f, b)
}
func TestDownloadURLRefresh(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	row := putRemote(t, base, f, k, "file", 0, []byte("data"), 0600)
	c := &countingURLs{Client: base}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r.urls = map[string]blobURL{row.BlobID: {url: "http://127.0.0.1:1/secret", issued: time.Now().Add(-3 * time.Minute)}}
	fetchOK(t, r, "file")
	if c.calls.Load() != 1 {
		t.Fatal(c.calls.Load())
	}
}

type tamperDownloadClient struct {
	Client
	calls  atomic.Int64
	tamper bool
}

func (c *tamperDownloadClient) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	c.calls.Add(1)
	r, e := c.Client.Download(ctx, f, b)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	data, e := io.ReadAll(r)
	if e != nil {
		return nil, e
	}
	if c.tamper {
		data[len(data)-1] ^= 1
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}
func TestOnDemandQuarantine(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "file", 0, []byte("remote"), 0600)
	c := &tamperDownloadClient{Client: base, tamper: true}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	got, e := r.Fetch(syncCtx, "file")
	if e != nil || len(got.Failed) != 1 {
		t.Fatal(got, e)
	}
	assertPlaceholder(t, r, "file")
	if len(r.Status().Quarantined) != 1 {
		t.Fatal(r.Status())
	}
	got, e = r.Fetch(syncCtx, "file")
	if e != nil || len(got.Failed) != 1 || c.calls.Load() != 1 {
		t.Fatal(got, e, c.calls.Load())
	}
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.calls.Load() != 1 {
		t.Fatal("redownloaded quarantine")
	}
	c.tamper = false
	r.RetryRejected()
	fetchOK(t, r, "file")
	if len(r.Status().Quarantined) != 0 {
		t.Fatal(r.Status())
	}
}

func TestOnDemandRootAndSingleLevelPatterns(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"root", "dir/file", "dir/deep/file"} {
		putRemote(t, base, f, k, p, 0, []byte(p), 0600)
	}
	r := lazyReplica(t, base, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	all, e := r.Entries(".")
	if e != nil || len(all) != 5 {
		t.Fatal(all, e)
	}
	one, e := r.Entries("dir/*")
	if e != nil || len(one) != 2 || one[0].Path != "dir/deep" || one[1].Path != "dir/file" {
		t.Fatal(one, e)
	}
	got := fetchOK(t, r, "dir/*")
	if !reflect.DeepEqual(got.Fetched, []string{"dir/file"}) {
		t.Fatal(got)
	}
	assertPlaceholder(t, r, "dir/deep/file")
	got = fetchOK(t, r, ".")
	if len(got.Fetched) != 2 || got.Present != 1 || len(got.Unmatched) != 0 {
		t.Fatal(got)
	}
}

func TestFetchRecollectsRemoteUpdate(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	row := putRemote(t, base, f, k, "file", 0, []byte("old"), 0600)
	c := &gatedDownloadClient{Client: base, entered: make(chan struct{}), proceed: make(chan struct{})}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	done := make(chan FetchResult, 1)
	go func() {
		res, e := r.Fetch(syncCtx, "file")
		if e != nil {
			t.Error(e)
		}
		done <- res
	}()
	<-c.entered
	putRemote(t, base, f, k, "file", row.Version, []byte("new remote"), 0700)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	close(c.proceed)
	got := <-done
	if len(got.Fetched) != 1 || len(got.Failed) != 0 || got.Bytes != 10 {
		t.Fatal(got)
	}
	if b, _ := os.ReadFile(filepath.Join(r.dir, "file")); string(b) != "new remote" {
		t.Fatal(string(b))
	}
	if c.calls.Load() != 2 {
		t.Fatal(c.calls.Load())
	}
}

type lyingSizeClient struct {
	Client
	key FolderKey
}

func (c lyingSizeClient) Changes(ctx context.Context, f string, after uint64) (Delta, error) {
	d, e := c.Client.Changes(ctx, f, after)
	if e != nil {
		return d, e
	}
	for n, row := range d.Rows {
		if row.Deleted {
			continue
		}
		m, e := OpenMetadata(c.key, f, row)
		if e != nil {
			return d, e
		}
		if m.Directory {
			continue
		}
		m.Size = MaxRequestBytes + 1
		d.Rows[n].Metadata, e = SealMetadata(c.key, f, row.PathID, m)
		if e != nil {
			return d, e
		}
	}
	return d, nil
}
func TestOnDemandRejectsFalsePlaceholderSize(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "file", 0, []byte("small"), 0600)
	c := &lazyCounter{Client: lyingSizeClient{Client: base, key: k}}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); !errors.Is(e, ErrIntegrity) {
		t.Fatal(e)
	}
	if r.Status().Placeholders != 0 || len(r.Status().Quarantined) != 1 || c.downloads.Load() != 0 {
		t.Fatal(r.Status(), c.downloads.Load())
	}
	if _, e := os.Lstat(filepath.Join(r.dir, "file")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
}

func TestDownloadStagingFailureBacksOff(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	row := putRemote(t, base, f, k, "file", 0, []byte("contents"), 0600)
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	r.opts.OnDemand = false
	if e := os.Chmod(r.opts.StateDir, 0500); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(r.opts.StateDir, 0700)
	if e := r.Sync(syncCtx); e == nil {
		t.Fatal("staging unexpectedly succeeded")
	}
	if !time.Now().Before(r.retryAt[row.PathID]) {
		t.Fatal("staging failure has no backoff")
	}
	if e := os.Chmod(r.opts.StateDir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if c.downloads.Load() != 0 {
		t.Fatal("retried before backoff", c.downloads.Load())
	}
	r.RetryRejected()
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if b, _ := os.ReadFile(filepath.Join(r.dir, "file")); string(b) != "contents" {
		t.Fatal(string(b))
	}
}

func TestConcurrentFetchesShareDownloadLimit(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for group := 0; group < 3; group++ {
		for n := 0; n < 4; n++ {
			p := fmt.Sprintf("group%d/file%d", group, n)
			putRemote(t, base, f, k, p, 0, []byte(p), 0600)
		}
	}
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	r.opts.Downloads = 1
	r.downloadSlots = make(chan struct{}, 1)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(syncCtx, 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for group := 0; group < 3; group++ {
		wg.Add(1)
		go func(group int) {
			defer wg.Done()
			got, e := r.Fetch(ctx, fmt.Sprintf("group%d", group))
			if e != nil || len(got.Failed) != 0 || len(got.Fetched) != 4 {
				t.Errorf("%+v %v", got, e)
			}
		}(group)
	}
	wg.Wait()
	if c.downloads.Load() != 12 {
		t.Fatal(c.downloads.Load())
	}
	r.stagingMu.Lock()
	defer r.stagingMu.Unlock()
	if r.stagingFiles != 0 || r.stagingBytes != 0 {
		t.Fatal("staging budget leaked", r.stagingFiles, r.stagingBytes)
	}
}
