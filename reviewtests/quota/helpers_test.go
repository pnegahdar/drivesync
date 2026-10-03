package quota

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/zeebo/blake3"
)

var bg = context.Background()

func newServer(t testing.TB) *ds.Server {
	m, e := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	return ds.NewServer(m, ds.NewMemoryBlobStore())
}
func mkFolder(t testing.TB, c ds.Client, l ds.Limits) (ds.Folder, ds.FolderKey) {
	if l.MaxTotalBytes > 0 && l.MaxRows == 0 {
		l.MaxRows = max(1, l.MaxTotalBytes/ds.RowCost)
	}
	if l.MaxTotalBytes > 0 && l.MaxFileBytes == 0 {
		l.MaxFileBytes = l.MaxTotalBytes
	}
	k := ds.NewFolderKey()
	f, e := ds.CreateFolder(bg, c, ds.FolderSpec{Name: fmt.Sprint(time.Now().UnixNano()), Limits: l}, k)
	if e != nil {
		t.Fatal(e)
	}
	return f, k
}
func hash(b []byte) string { h := blake3.Sum256(b); return fmt.Sprintf("%x", h[:]) }

func putFile(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data []byte) ds.Row {
	t.Helper()
	pid, e := ds.PathID(k, f.ID, p)
	if e != nil {
		t.Fatal(e)
	}
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: base, SealedSize: ds.SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	var sealed bytes.Buffer
	if e = ds.SealContent(&sealed, bytes.NewReader(data), k, f.ID, tk.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(bg, f.ID, tk, &sealed); e != nil {
		t.Fatal(e)
	}
	meta, e := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hash(data)})
	if e != nil {
		t.Fatal(e)
	}
	d, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: base, TicketID: tk.ID, Metadata: meta}})
	if e != nil {
		t.Fatal(e)
	}
	return d.Rows[0]
}

func attach(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, name string) (*ds.Replica, string) {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, e := ds.Attach(bg, c, f.ID, k, dir, ds.Options{Name: name, StateDir: filepath.Join(base, "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	d, _ := filepath.EvalSymlinks(dir)
	return r, d
}
func write(t testing.TB, dir, p, content string) {
	full := filepath.Join(dir, filepath.FromSlash(p))
	os.MkdirAll(filepath.Dir(full), 0700)
	if e := os.WriteFile(full, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
}
func files(t testing.TB, dir string) map[string]string {
	out := map[string]string{}
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if d != nil && d.Name() == ".drivesync-root" {
			return nil
		}
		if e != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		b, _ := os.ReadFile(p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out
}
func keys(m map[string]string) string {
	var out []string
	for k, v := range m {
		out = append(out, fmt.Sprintf("%q=%q", k, v))
	}
	return strings.Join(out, ", ")
}

var errNet = errors.New("simulated network failure")

// hookClient lets a test intercept individual calls.
type hookClient struct {
	ds.Client
	mu      sync.Mutex
	commit  func([]ds.Mutation) error // return non-nil to fail before reaching server
	cancel  func() error
	upload  func() error
	reserve func(ds.UploadRequest) error
}

func (h *hookClient) Commit(x context.Context, id string, m []ds.Mutation) (ds.Delta, error) {
	h.mu.Lock()
	fn := h.commit
	h.mu.Unlock()
	if fn != nil {
		if e := fn(m); e != nil {
			return ds.Delta{}, e
		}
	}
	return h.Client.Commit(x, id, m)
}
func (h *hookClient) CancelUpload(x context.Context, id, tid string) error {
	h.mu.Lock()
	fn := h.cancel
	h.mu.Unlock()
	if fn != nil {
		if e := fn(); e != nil {
			return e
		}
	}
	return h.Client.CancelUpload(x, id, tid)
}
func (h *hookClient) Upload(x context.Context, id string, t ds.Ticket, r io.Reader) error {
	h.mu.Lock()
	fn := h.upload
	h.mu.Unlock()
	if fn != nil {
		if e := fn(); e != nil {
			io.Copy(io.Discard, r)
			return e
		}
	}
	return h.Client.Upload(x, id, t, r)
}
func (h *hookClient) Reserve(x context.Context, id string, r ds.UploadRequest) (ds.Ticket, error) {
	h.mu.Lock()
	fn := h.reserve
	h.mu.Unlock()
	if fn != nil {
		if e := fn(r); e != nil {
			return ds.Ticket{}, e
		}
	}
	return h.Client.Reserve(x, id, r)
}
