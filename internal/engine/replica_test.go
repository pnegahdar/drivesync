package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func replicaFor(t testing.TB, c Client, f Folder, k FolderKey, name string, manual bool) *Replica {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "files")
	r, e := Attach(context.Background(), c, f.ID, k, dir, Options{Name: name, Manual: manual, RescanInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func writeLocal(t testing.TB, r *Replica, p, content string) {
	t.Helper()
	full := filepath.Join(r.dir, filepath.FromSlash(p))
	if e := os.MkdirAll(filepath.Dir(full), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(full, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
}
func syncReplicas(t testing.TB, rs ...*Replica) {
	t.Helper()
	for round := 0; round < 5; round++ {
		for _, r := range rs {
			if e := r.Sync(context.Background()); e != nil {
				t.Fatalf("%s: %v", r.opts.Name, e)
			}
		}
	}
}
func filesOn(t testing.TB, r *Replica) map[string]string {
	t.Helper()
	out := map[string]string{}
	if e := filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(r.dir, p)
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
func TestReplicaConvergenceAndConflicts(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := replicaFor(t, c, f, k, "a", true)
	b := replicaFor(t, c, f, k, "b", true)
	writeLocal(t, a, "dir/file.txt", "initial")
	syncReplicas(t, a, b)
	writeLocal(t, a, "dir/file.txt", "from a")
	writeLocal(t, b, "dir/file.txt", "from b")
	syncReplicas(t, a, b)
	af, bf := filesOn(t, a), filesOn(t, b)
	if fmt.Sprint(af) != fmt.Sprint(bf) {
		t.Fatalf("not converged: %v %v", af, bf)
	}
	contents := map[string]bool{}
	for _, v := range af {
		contents[v] = true
	}
	if !contents["from a"] || !contents["from b"] {
		t.Fatal(af)
	}
	if len(b.Status().Conflicts) == 0 {
		t.Fatal("no conflict")
	}
	if e := os.Rename(filepath.Join(a.dir, "dir/file.txt"), filepath.Join(a.dir, "renamed.txt")); e != nil {
		t.Fatal(e)
	}
	syncReplicas(t, a, b)
	if _, ok := filesOn(t, b)["renamed.txt"]; !ok {
		t.Fatal("rename not synced")
	}
	if e := os.Remove(filepath.Join(a.dir, "renamed.txt")); e != nil {
		t.Fatal(e)
	}
	syncReplicas(t, a, b)
	if _, ok := filesOn(t, b)["renamed.txt"]; ok {
		t.Fatal("delete not synced")
	}
}
func TestWrongKeySymlinksAndIgnore(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	if _, e := Attach(context.Background(), c, f.ID, NewFolderKey(), filepath.Join(t.TempDir(), "bad"), Options{Manual: true}); e != ErrKey {
		t.Fatal(e)
	}
	r := replicaFor(t, c, f, k, "r", true)
	outside := filepath.Join(t.TempDir(), "outside")
	if e := os.WriteFile(outside, []byte("secret"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(outside, filepath.Join(r.dir, "link")); e != nil {
		t.Fatal(e)
	}
	writeLocal(t, r, ".DS_Store", "ignored")
	writeLocal(t, r, ".drivesyncignore", "private*\ncache/\n")
	writeLocal(t, r, "private.txt", "ignored")
	writeLocal(t, r, "cache/x", "ignored")
	writeLocal(t, r, "okay", "public")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	d, e := c.Changes(context.Background(), f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	if len(d.Rows) != 1 {
		t.Fatal(d.Rows)
	}
	if len(r.Status().Skipped) != 1 {
		t.Fatal(r.Status())
	}
	writeLocal(t, r, ".drivesyncignore", "private*\ncache/\nokay\n")
	if e = r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	d, _ = c.Changes(context.Background(), f.ID, 0)
	if d.Rows[0].Deleted {
		t.Fatal("ignoring caused remote deletion")
	}
}
func TestMaliciousPeerPathsAndCaseCollisions(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	put(t, c, f, k, "Foo.txt", 0, []byte("upper"))
	put(t, c, f, k, "foo.txt", 0, []byte("lower"))
	r := replicaFor(t, c, f, k, "r", true)
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	contents := map[string]bool{}
	for _, v := range filesOn(t, r) {
		contents[v] = true
	}
	if !contents["upper"] || !contents["lower"] {
		t.Fatal(filesOn(t, r))
	}
	local := r.index["foo.txt"].Local
	if local == "foo.txt" {
		local = r.index["Foo.txt"].Local
	}
	writeLocal(t, r, local, "edited alias")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	// A malicious peer can seal arbitrary bytes directly; validation must still run after decrypting.
	badPath := "../escape"
	pid := strings.Repeat("e", 64)
	blob := randomID()
	m := FileMetadata{Path: badPath, BlobID: blob, Mode: 0600}
	badMeta := sealRawMetadata(k, f.ID, pid, m)
	if _, e := OpenMetadata(k, f.ID, Row{FolderID: f.ID, PathID: pid, BlobID: blob, Metadata: badMeta}); e != ErrIntegrity {
		t.Fatal(e)
	}
	put(t, c, f, k, "nested/file", 0, []byte("safe"))
	outside := t.TempDir()
	if e := os.Symlink(outside, filepath.Join(r.dir, "nested")); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(context.Background()); e == nil {
		t.Fatal("symlink parent accepted")
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatal("wrote outside root")
	}
}
func TestReplicaLimitsAndRestart(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{MaxFileBytes: SealedSize(3)})
	r := replicaFor(t, c, f, k, "a", true)
	writeLocal(t, r, "big", "1234")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(r.Status().Rejected) != 1 {
		t.Fatal(r.Status())
	}
	for i := 0; i < 3; i++ {
		if e := r.Sync(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if filesOn(t, r)["big"] != "1234" {
		t.Fatal("lost rejected file")
	}
	_ = c.SetLimits(context.Background(), f.ID, Limits{})
	r.RetryRejected()
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(r.Status().Rejected) != 0 {
		t.Fatal(r.Status())
	}
	dir, state := r.dir, r.opts.StateDir
	writeLocal(t, r, "restart", "new")
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
	rr, e := Attach(context.Background(), c, f.ID, k, dir, Options{Name: "a", StateDir: state, Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer rr.Close()
	if e = rr.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	if filesOn(t, rr)["restart"] != "new" {
		t.Fatal("restart loss")
	}
}

var errNetwork = errors.New("injected network failure")

type flakyClient struct {
	Client
	mu       sync.Mutex
	rng      *rand.Rand
	failures bool
	calls    int
	dropAck  bool
}

func (c *flakyClient) fail() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.failures && c.rng.Intn(7) == 0
}
func (c *flakyClient) Reserve(x context.Context, id string, r UploadRequest) (Ticket, error) {
	if c.fail() {
		return Ticket{}, errNetwork
	}
	return c.Client.Reserve(x, id, r)
}
func (c *flakyClient) Upload(x context.Context, id string, t Ticket, r io.Reader) error {
	if c.fail() {
		return errNetwork
	}
	return c.Client.Upload(x, id, t, r)
}
func (c *flakyClient) Changes(x context.Context, id string, v uint64) (Delta, error) {
	if c.fail() {
		return Delta{}, errNetwork
	}
	return c.Client.Changes(x, id, v)
}
func (c *flakyClient) Commit(x context.Context, id string, m []Mutation) (Delta, error) {
	if c.fail() {
		return Delta{}, errNetwork
	}
	d, e := c.Client.Commit(x, id, m)
	if e == nil && c.dropAck {
		c.dropAck = false
		return Delta{}, errNetwork
	}
	return d, e
}
func (c *flakyClient) Download(x context.Context, id, blob string) (io.ReadCloser, error) {
	if c.fail() {
		return nil, errNetwork
	}
	return c.Client.Download(x, id, blob)
}
func TestSeededRandomizedReplicas(t *testing.T) {
	for _, seed := range []int64{42, 9817, 20261003} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			s, _ := testServer(t)
			base := s.Client(owner)
			f, k := folderFor(t, base, Limits{})
			rs := []*Replica{}
			cs := []*flakyClient{}
			for i := 0; i < 3; i++ {
				c := &flakyClient{Client: base, rng: rand.New(rand.NewSource(seed + int64(i)))}
				cs = append(cs, c)
				rs = append(rs, replicaFor(t, c, f, k, fmt.Sprint(i), true))
			}
			for round := 0; round < 12; round++ {
				target := fmt.Sprintf("dir%d/file%d.txt", rng.Intn(3), round%4)
				tokens := []string{}
				for i, r := range rs {
					token := fmt.Sprintf("seed=%d round=%d replica=%d", seed, round, i)
					tokens = append(tokens, token)
					writeLocal(t, r, target, token)
					cs[i].failures = true
				}
				for j := 0; j < 12; j++ {
					i := rng.Intn(3)
					e := rs[i].Sync(context.Background())
					if e != nil && !errors.Is(e, errNetwork) {
						t.Fatalf("seed %d round %d: %v", seed, round, e)
					}
				}
				for _, c := range cs {
					c.failures = false
				}
				syncReplicas(t, rs...)
				want := filesOn(t, rs[0])
				for _, r := range rs[1:] {
					if fmt.Sprint(filesOn(t, r)) != fmt.Sprint(want) {
						t.Fatalf("seed %d round %d divergence", seed, round)
					}
				}
				seen := map[string]bool{}
				for _, v := range want {
					seen[v] = true
				}
				for _, token := range tokens {
					if !seen[token] {
						t.Fatalf("seed %d round %d lost %s: %v", seed, round, token, want)
					}
				}
				// Random rename/delete/mkdir interleavings after the simultaneous writes settle.
				victim := rs[rng.Intn(3)]
				if rng.Intn(2) == 0 {
					if e := os.Rename(filepath.Join(victim.dir, target), filepath.Join(victim.dir, fmt.Sprintf("moved-%d.txt", round))); e != nil {
						t.Fatal(e)
					}
				} else {
					if e := os.Remove(filepath.Join(victim.dir, target)); e != nil {
						t.Fatal(e)
					}
				}
				if e := os.MkdirAll(filepath.Join(victim.dir, fmt.Sprintf("empty-%d", round)), 0700); e != nil {
					t.Fatal(e)
				}
				syncReplicas(t, rs...)
			}
		})
	}
}

type interruptedDownload struct {
	Client
	cut bool
}

func (c *interruptedDownload) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	r, e := c.Client.Download(ctx, f, b)
	if e != nil {
		return nil, e
	}
	if c.cut {
		return &cutReader{r: r, left: 30}, nil
	}
	return r, nil
}

type cutReader struct {
	r    io.ReadCloser
	left int
}

func (r *cutReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, errNetwork
	}
	if len(p) > r.left {
		p = p[:r.left]
	}
	n, e := r.r.Read(p)
	r.left -= n
	return n, e
}
func (r *cutReader) Close() error { return r.r.Close() }
func TestInterruptedDownloadAndLostCommitAck(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	put(t, base, f, k, "file", 0, bytes.Repeat([]byte("x"), ChunkSize+10))
	c := &interruptedDownload{base, true}
	r := replicaFor(t, c, f, k, "r", true)
	if e := r.Sync(context.Background()); e == nil {
		t.Fatal("interruption accepted")
	}
	if len(filesOn(t, r)) != 0 {
		t.Fatal("partial published")
	}
	dir, state := r.dir, r.opts.StateDir
	_ = r.Close()
	c.cut = false
	rr, e := Attach(context.Background(), c, f.ID, k, dir, Options{Name: "r", StateDir: state, Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer rr.Close()
	if e = rr.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(filesOn(t, rr)["file"]) != ChunkSize+10 {
		t.Fatal("resume failed")
	}
	fc := &flakyClient{Client: base, rng: rand.New(rand.NewSource(1)), dropAck: true}
	rr.client = fc
	writeLocal(t, rr, "file", "last write")
	if e = rr.Sync(context.Background()); e != errNetwork {
		t.Fatal(e)
	}
	syncReplicas(t, rr)
	if filesOn(t, rr)["file"] != "last write" {
		t.Fatal(filesOn(t, rr))
	}
}
func TestWatcherPropagation(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native watcher")
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := replicaFor(t, c, f, k, "a", false)
	b := replicaFor(t, c, f, k, "b", false)
	want := "inotify"
	if runtime.GOOS == "darwin" {
		want = "FSEvents"
	}
	if a.Status().Watcher != want {
		t.Fatalf("watcher: %+v", a.Status())
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	writeLocal(t, a, "deep/new/file", "fast")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, e := os.ReadFile(filepath.Join(b.dir, "deep/new/file"))
		if e == nil && string(data) == "fast" {
			t.Logf("propagation %s", time.Since(start))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no propagation: %+v %+v", a.Status(), b.Status())
}
func BenchmarkSmallFilePropagation(b *testing.B) {
	s, _ := testServer(b)
	c := s.Client(owner)
	f, k := folderFor(b, c, Limits{})
	a := replicaFor(b, c, f, k, "a", false)
	r := replicaFor(b, c, f, k, "b", false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token := fmt.Sprint(i)
		writeLocal(b, a, "file", token)
		deadline := time.Now().Add(5 * time.Second)
		for {
			data, _ := os.ReadFile(filepath.Join(r.dir, "file"))
			if string(data) == token {
				break
			}
			if time.Now().After(deadline) {
				b.Fatal("timeout")
			}
			time.Sleep(time.Millisecond)
		}
	}
}
func BenchmarkScan10K(b *testing.B) {
	s, _ := testServer(b)
	c := s.Client(owner)
	f, k := folderFor(b, c, Limits{})
	r := replicaFor(b, c, f, k, "r", true)
	for i := 0; i < 10000; i++ {
		writeLocal(b, r, fmt.Sprintf("dir%d/file%d", i/100, i), "small file")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, e := r.ScanLocal()
		if e != nil || len(v) != 10100 {
			b.Fatal(len(v), e)
		}
	}
}
func TestReaderReplica(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := sharedFor(t, base)
	p := Principal{"tenant", "reader"}
	_ = base.Grant(context.Background(), f.ID, p, Reader)
	row := put(t, base, f, k, "file", 0, []byte("server"))
	r := replicaFor(t, s.Client(p), f, k, "reader", true)
	syncReplicas(t, r)
	writeLocal(t, r, "local", "private")
	writeLocal(t, r, "file", "dirty")
	put(t, base, f, k, "file", row.Version, []byte("new winner"))
	syncReplicas(t, r)
	seen := []string{}
	for _, v := range filesOn(t, r) {
		seen = append(seen, v)
	}
	sort.Strings(seen)
	if strings.Join(seen, ",") != "dirty,new winner,private" {
		t.Fatal(seen)
	}
}

func TestMaliciousMetadataThroughReplica(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx := context.Background()
	pid := strings.Repeat("a", 64)
	ticket, e := c.Reserve(ctx, f.ID, UploadRequest{PathID: pid, BaseVersion: 0, SealedSize: SealedSize(0)})
	if e != nil {
		t.Fatal(e)
	}
	var b bytes.Buffer
	_ = SealContent(&b, bytes.NewReader(nil), k, f.ID, ticket.BlobID, pid)
	if e = c.Upload(ctx, f.ID, ticket, &b); e != nil {
		t.Fatal(e)
	}
	meta := sealRawMetadata(k, f.ID, pid, FileMetadata{Path: "../escape", BlobID: ticket.BlobID, Mode: 0600, Hash: hashBytes(nil)})
	if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	r := replicaFor(t, c, f, k, "r", true)
	if e = r.Sync(ctx); e != ErrIntegrity {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(filepath.Dir(r.dir), "escape")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("escaped", e)
	}
}
func TestDeleteRecreateAndLongConflicts(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a, b := replicaFor(t, c, f, k, "a", true), replicaFor(t, c, f, k, "b", true)
	p := strings.Repeat("é", 120) + ".txt"
	writeLocal(t, a, p, "a")
	syncReplicas(t, a, b)
	writeLocal(t, a, p, "winner")
	writeLocal(t, b, p, "loser")
	syncReplicas(t, a, b)
	seen := map[string]bool{}
	for _, v := range filesOn(t, b) {
		seen[v] = true
	}
	if !seen["winner"] || !seen["loser"] {
		t.Fatal(seen)
	}
	_ = os.Remove(filepath.Join(a.dir, p))
	syncReplicas(t, a, b)
	writeLocal(t, b, p, "recreated")
	syncReplicas(t, a, b)
	if filesOn(t, a)[p] != "recreated" {
		t.Fatal(filesOn(t, a))
	}
	fresh := replicaFor(t, c, f, k, "fresh", true)
	_ = os.Remove(filepath.Join(a.dir, p))
	syncReplicas(t, a, b)
	writeLocal(t, fresh, p, "fresh recreation")
	syncReplicas(t, fresh, a, b)
	if filesOn(t, a)[p] != "fresh recreation" {
		t.Fatal(filesOn(t, a))
	}
}
func TestHTTPReplica(t *testing.T) {
	s, _ := testServer(t)
	c := clients(t, s, true)(owner)
	f, k := folderFor(t, c, Limits{})
	a, b := replicaFor(t, c, f, k, "a", true), replicaFor(t, c, f, k, "b", true)
	writeLocal(t, a, "directory/file", strings.Repeat("hello", 20000))
	syncReplicas(t, a, b)
	if filesOn(t, b)["directory/file"] != strings.Repeat("hello", 20000) {
		t.Fatal("HTTP streaming did not converge")
	}
}

func BenchmarkIndexedScan10K(b *testing.B) {
	s, _ := testServer(b)
	c := s.Client(owner)
	f, k := folderFor(b, c, Limits{})
	r := replicaFor(b, c, f, k, "r", true)
	for i := 0; i < 10000; i++ {
		writeLocal(b, r, fmt.Sprintf("dir%d/file%d", i/100, i), "small file")
	}
	scanned, e := r.ScanLocal()
	if e != nil {
		b.Fatal(e)
	}
	for _, v := range scanned {
		r.Remember(IndexEntry{Path: v.Path, Local: v.Local, Hash: v.Hash, Directory: v.Directory, Mode: v.Mode, Version: 1})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, e := r.ScanLocal()
		if e != nil || len(v) != 10100 {
			b.Fatal(len(v), e)
		}
	}
}

func TestCaseDirectoryBatchAndTypeReplacement(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx := context.Background()
	mutations := []Mutation{}
	for _, p := range []string{"Foo", "foo", "Foo/file", "foo/file"} {
		directory := !strings.Contains(p, "/")
		content := []byte(p)
		if directory {
			content = nil
		}
		pid, _ := PathID(k, f.ID, p)
		ticket, e := c.Reserve(ctx, f.ID, UploadRequest{PathID: pid, BaseVersion: 0, SealedSize: SealedSize(int64(len(content)))})
		if e != nil {
			t.Fatal(e)
		}
		var b bytes.Buffer
		_ = SealContent(&b, bytes.NewReader(content), k, f.ID, ticket.BlobID, pid)
		if e = c.Upload(ctx, f.ID, ticket, &b); e != nil {
			t.Fatal(e)
		}
		hash := hashBytes(content)
		if directory {
			hash = "directory"
		}
		meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: p, BlobID: ticket.BlobID, Size: int64(len(content)), Mode: 0700, Hash: hash, Directory: directory})
		if e != nil {
			t.Fatal(e)
		}
		mutations = append(mutations, Mutation{PathID: pid, TicketID: ticket.ID, Metadata: meta})
	}
	if _, e := c.Commit(ctx, f.ID, mutations); e != nil {
		t.Fatal(e)
	}
	r := replicaFor(t, c, f, k, "r", true)
	syncReplicas(t, r)
	seen := map[string]bool{}
	for _, v := range filesOn(t, r) {
		seen[v] = true
	}
	if !seen["Foo/file"] || !seen["foo/file"] {
		t.Fatal(filesOn(t, r))
	}
	if r.index["Foo"].Local == r.index["foo"].Local {
		t.Fatal("directory alias missing")
	}
	// Replace a directory with a file: child tombstones must not get stuck behind its new parent type.
	rows, _ := c.Changes(ctx, f.ID, 0)
	var parent, child Row
	for _, row := range rows.Rows {
		m, _ := OpenMetadata(k, f.ID, row)
		if m.Path == "Foo" {
			parent = row
		}
		if m.Path == "Foo/file" {
			child = row
		}
	}
	if _, e := c.Commit(ctx, f.ID, []Mutation{{PathID: child.PathID, BaseVersion: child.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	put(t, c, f, k, "Foo", parent.Version, []byte("replacement"))
	syncReplicas(t, r)
	if filesOn(t, r)[r.index["Foo"].Local] != "replacement" {
		t.Fatal(filesOn(t, r))
	}
}
