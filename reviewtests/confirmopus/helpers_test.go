package confirmreview

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
	f, e := c.CreateFolder(bg, ds.FolderSpec{Name: fmt.Sprintf("f%d-%d", time.Now().UnixNano(), seq.Add(1)), Limits: l, KeyCheck: ds.KeyCheck(k)})
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

func newServerMeta(t testing.TB) (*ds.SQLiteMetaStore, error) {
	m, e := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	return m, nil
}

// Admit one representative through the public API, then populate equivalent
// idle owners in one transaction. Measurements retain the original cardinality
// and run only through public APIs; race instrumentation need not price setup.
func seedOwners(t testing.TB, s *ds.Server, m *ds.SQLiteMetaStore, n int, shared *ds.Principal) {
	t.Helper()
	first := ds.Principal{Tenant: "tenant-0", Subject: "u"}
	limits := ds.Limits{}
	if shared != nil {
		limits = ds.Limits{MaxTotalBytes: 1, MaxRows: 1, MaxFileBytes: 1}
	}
	f, e := s.Client(first).CreateFolder(bg, ds.FolderSpec{Name: "f", Limits: limits, KeyCheck: ds.KeyCheck(ds.NewFolderKey())})
	if e != nil {
		t.Fatal(e)
	}
	if shared != nil {
		if e = s.Client(first).Grant(bg, f.ID, *shared, ds.Reader); e != nil {
			t.Fatal(e)
		}
	}
	e = m.Transaction(bg, func(v *ds.Metadata) error {
		template := v.Folders[f.ID]
		for i := 1; i < n; i++ {
			id := fmt.Sprintf("%032x", i)
			record := template
			record.Folder.ID = id
			record.Folder.Owner = ds.Principal{Tenant: fmt.Sprintf("tenant-%d", i), Subject: "u"}
			record.Grants = map[string]ds.Role{}
			for key, role := range template.Grants {
				record.Grants[key] = role
			}
			v.Folders[id] = record
			v.Files[id] = map[string]ds.Row{}
		}
		count := 0
		for _, record := range v.Folders {
			if strings.HasPrefix(record.Folder.Owner.Tenant, "tenant-") {
				count++
			}
		}
		if count != n {
			return fmt.Errorf("seeded owners %d want %d", count, n)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}

func seedTombstones(t testing.TB, m *ds.SQLiteMetaStore, c ds.Client, f ds.Folder, k ds.FolderKey, prefix string, n int) {
	t.Helper()
	var muts []ds.Mutation
	for j := 0; j < min(256, n); j++ {
		pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("%s/%d", prefix, j))
		muts = append(muts, ds.Mutation{PathID: pid, Deleted: true})
	}
	d, e := c.Commit(bg, f.ID, muts)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Transaction(bg, func(v *ds.Metadata) error {
		before := len(v.Files[f.ID])
		for j := 256; j < n; j++ {
			pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("%s/%d", prefix, j))
			v.Files[f.ID][pid] = ds.Row{FolderID: f.ID, PathID: pid, Version: d.Version + uint64(j/256), Deleted: true, DeletedAt: time.Now().UnixNano()}
		}
		record := v.Folders[f.ID]
		record.Folder.Version = d.Version + uint64((n-1)/256)
		v.Folders[f.ID] = record
		if len(v.Files[f.ID]) != before+max(0, n-256) {
			return fmt.Errorf("tombstone cardinality changed")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
