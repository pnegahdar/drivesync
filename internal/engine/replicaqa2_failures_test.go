package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type qa2CountingClient struct {
	Client
	downloads atomic.Int64
}

func (c *qa2CountingClient) Download(ctx context.Context, id, blob string) (io.ReadCloser, error) {
	c.downloads.Add(1)
	return c.Client.Download(ctx, id, blob)
}

// T2. A remote update whose local publication fails after the download
// (unreadable local file here; ENOSPC behaves the same) returns a PathError,
// which isTransferError excludes from backoff (transfer.go:29, replica.go:1403).
// The row is retried on every pull and the whole blob is downloaded again
// before failing at inspect (replica.go:1841). With a watcher, the replica's own
// staging create/remove wakes the next sync: a continuous re-download loop.
func TestQA2PersistentLocalFailureRedownloadsInHotLoop(t *testing.T) {
	t.Run("unreadable local file", qa2HotLoopUnreadable)
	t.Run("disk full", qa2HotLoopDiskFull)
}

func qa2HotLoopDiskFull(t *testing.T) {
	img := qaImage(t, "qa2full")
	at := filepath.Join(t.TempDir(), "small")
	qaMount(t, img, at)
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	counting := &qa2CountingClient{Client: base}
	qaPut(t, base, f, k, "notes.txt", 0, []byte("small"), 0644)
	r, e := Attach(qaCtx, counting, f.ID, k, filepath.Join(at, "shared"), Options{Name: "a", StateDir: filepath.Join(t.TempDir(), "state"), RescanInterval: time.Hour, RetryInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	time.Sleep(500 * time.Millisecond)
	start := counting.downloads.Load()
	qaPut(t, base, f, k, "video.mov", 0, make([]byte, 6<<20), 0644) // larger than the 4 MB volume
	time.Sleep(3 * time.Second)
	n := counting.downloads.Load() - start
	t.Logf("downloads of one 6 MiB row onto a full disk in 3s: %d", n)
	if n > 3 {
		t.Fatalf("ENOSPC re-downloaded the blob %d times in 3s (RetryInterval=1h)", n)
	}
}

func qa2HotLoopUnreadable(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	counting := &qa2CountingClient{Client: base}
	dir := filepath.Join(t.TempDir(), "a")
	row := qaPut(t, base, f, k, "report.pdf", 0, []byte("v1"), 0644)
	r, e := Attach(qaCtx, counting, f.ID, k, dir, Options{Name: "a", StateDir: filepath.Join(t.TempDir(), "state"), RescanInterval: time.Hour, RetryInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if b, _ := os.ReadFile(filepath.Join(dir, "report.pdf")); string(b) == "v1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control: initial download")
		}
		time.Sleep(20 * time.Millisecond)
	}
	full := filepath.Join(dir, "report.pdf")
	if e := os.Chmod(full, 0); e != nil { // e.g. a viewer/AV tool locked it, or an ACL
		t.Fatal(e)
	}
	defer os.Chmod(full, 0600)
	time.Sleep(300 * time.Millisecond)
	start := counting.downloads.Load()
	qaPut(t, base, f, k, "report.pdf", row.Version, make([]byte, 2<<20), 0644)
	time.Sleep(3 * time.Second)
	n := counting.downloads.Load() - start
	t.Logf("downloads of the same 2 MiB row in 3s: %d; status errors: %d", n, len(r.Status().Errors))
	if n > 3 {
		t.Fatalf("persistent local failure re-downloaded the blob %d times in 3s (RetryInterval=1h)", n)
	}
}

// T1 (permanent error state). Peer a deletes folder d/ while peer b adds
// d/new.txt; the authority accepts both (different paths), leaving a tombstone
// for d with a live child. On b the directory delete fails with "directory not
// empty" and, because a tracked child remains, is retried on every pull with
// no backoff and no way to finish (replica.go:1661-1666): every Sync returns
// an error forever, Status.Errors never clears, and any Retry acknowledgement
// stays armed (see TestQA2MassDeleteAckOutlivesAcknowledgedDeletes).
type qa2CommitHook struct {
	Client
	beforeCommit func()
}

func (c *qa2CommitHook) Commit(ctx context.Context, id string, m []Mutation) (Delta, error) {
	if h := c.beforeCommit; h != nil {
		c.beforeCommit = nil
		h()
	}
	return c.Client.Commit(ctx, id, m)
}

func TestQA2ConcurrentFolderDeleteAndAddWedgesAdder(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	hook := &qa2CommitHook{Client: c}
	a := qaAttach(t, hook, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "d/old.txt", "old")
	writeLocal(t, a, "keep.txt", "keep")
	qaSync(t, a, b)
	os.RemoveAll(filepath.Join(a.dir, "d"))
	// b adds d/new.txt after a pulled but before a commits its folder delete.
	hook.beforeCommit = func() {
		writeLocal(t, b, "d/new.txt", "new work")
		if e := b.Sync(qaCtx); e != nil {
			t.Error(e)
		}
	}
	if e := a.Sync(qaCtx); e != nil {
		t.Fatal(e)
	}
	var last error
	for i := 0; i < 4; i++ {
		last = b.Sync(qaCtx)
		_ = a.Sync(qaCtx)
	}
	if last != nil {
		t.Fatalf("b never converges after a concurrent folder delete/add: %v (files b=%v a=%v)", last, keys(filesOn(t, b)), keys(filesOn(t, a)))
	}
}

// T2 (defense in depth). index.sqlite and its WAL/SHM, which hold every
// plaintext path, are created 0644 (modernc default); only the 0700 state
// directory protects them. Copying the state dir (cp -R without -p), a
// restored backup, or state on a filesystem without permissions (exFAT/SMB)
// exposes the names to every local user. lock/status/marker files are 0600.
func TestQA2StateDatabaseFilesWorldReadable(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	writeLocal(t, a, "acquisition-target-2027/term-sheet.pdf", "x")
	qaSync(t, a)
	entries, _ := os.ReadDir(a.opts.StateDir)
	for _, entry := range entries {
		info, _ := entry.Info()
		if info.Mode().Perm()&0077 != 0 {
			t.Errorf("%s is %v", entry.Name(), info.Mode().Perm())
		}
	}
}
