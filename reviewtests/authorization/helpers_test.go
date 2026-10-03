package authorization

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/zeebo/blake3"
)

var bg = context.Background()
var seq atomic.Int64

func newServer(t testing.TB) (*ds.Server, *ds.SQLiteMetaStore) {
	m, e := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	return ds.NewServer(m, ds.NewMemoryBlobStore()), m
}

func mkFolder(t testing.TB, c ds.Client, l ds.Limits) (ds.Folder, ds.FolderKey) {
	t.Helper()
	if l.MaxTotalBytes > 0 && l.MaxFileBytes == 0 {
		l.MaxFileBytes = l.MaxTotalBytes
	}
	k := ds.NewFolderKey()
	f, e := ds.CreateFolder(bg, c, ds.FolderSpec{Name: fmt.Sprintf("f%d-%d", time.Now().UnixNano(), seq.Add(1)), Limits: l}, k)
	if e != nil {
		t.Fatal(e)
	}
	return f, k
}
func hash(b []byte) string { h := blake3.Sum256(b); return fmt.Sprintf("%x", h[:]) }

func tryPut(c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data []byte) (ds.Row, error) {
	pid, e := ds.PathID(k, f.ID, p)
	if e != nil {
		return ds.Row{}, e
	}
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: base, SealedSize: ds.SealedSize(int64(len(data))), MetadataBytes: 512})
	if e != nil {
		return ds.Row{}, e
	}
	var sealed bytes.Buffer
	if e = ds.SealContent(&sealed, bytes.NewReader(data), k, f.ID, tk.BlobID, pid); e != nil {
		return ds.Row{}, e
	}
	if e = c.Upload(bg, f.ID, tk, &sealed); e != nil {
		return ds.Row{}, e
	}
	meta, e := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hash(data)})
	if e != nil {
		return ds.Row{}, e
	}
	d, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: base, TicketID: tk.ID, Metadata: meta}})
	if e != nil {
		return ds.Row{}, e
	}
	return d.Rows[0], nil
}

func putFile(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data []byte) ds.Row {
	t.Helper()
	r, e := tryPut(c, f, k, p, base, data)
	if e != nil {
		t.Fatal(e)
	}
	return r
}

// httpClients mounts the server and returns a factory for HTTP clients per principal.
func httpClients(t testing.TB, s *ds.Server) func(ds.Principal) ds.Client {
	tokens := map[string]ds.Principal{}
	var n atomic.Int64
	srv := httptest.NewServer(s.Handler(func(r *http.Request) (ds.Principal, error) {
		p, ok := tokens[r.Header.Get("Authorization")]
		if !ok {
			return ds.Principal{}, ds.ErrDenied
		}
		return p, nil
	}))
	t.Cleanup(srv.Close)
	return func(p ds.Principal) ds.Client {
		tok := fmt.Sprintf("Bearer t%d", n.Add(1))
		tokens[tok] = p
		return ds.NewHTTPClient(srv.URL, http.Header{"Authorization": {tok}})
	}
}
