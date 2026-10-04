package isolation

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/pnegahdar/drivesync/internal/testkit"
	"github.com/zeebo/blake3"
)

var ctx = context.Background()
var owner = ds.Principal{Tenant: "private-tenant", Subject: "owner"}

func setup(t *testing.T) (*ds.Server, ds.Client) {
	t.Helper()
	m := testkit.OpenEngine(t)
	s := ds.NewServer(m, ds.NewMemoryBlobStore())
	return s, s.Client(owner)
}
func folder(t *testing.T, c ds.Client, name string, k ds.FolderKey) ds.Folder {
	t.Helper()
	f, e := ds.CreateFolder(ctx, c, ds.FolderSpec{Name: name, Limits: ds.Limits{MaxTotalBytes: 4000, MaxFileBytes: 4000, MaxRows: 1000}}, k)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func put(t *testing.T, c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, data string) ds.Row {
	t.Helper()
	pid, e := ds.PathID(k, f.ID, p)
	if e != nil {
		t.Fatal(e)
	}
	ticket, e := c.Reserve(ctx, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: base, SealedSize: ds.SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	var b bytes.Buffer
	if e = ds.SealContent(&b, strings.NewReader(data), k, f.ID, ticket.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, &b); e != nil {
		t.Fatal(e)
	}
	h := blake3.Sum256([]byte(data))
	meta, e := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: ticket.BlobID, Mode: 0600, Size: int64(len(data)), Hash: fmt.Sprintf("%x", h)})
	if e != nil {
		t.Fatal(e)
	}
	delta, e := c.Commit(ctx, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: base, TicketID: ticket.ID, Metadata: meta}})
	if e != nil {
		t.Fatal(e)
	}
	return delta.Rows[0]
}
func TestRawJSONPrincipalAlias(t *testing.T) {
	for _, bad := range []string{string([]byte{0xff}), `\ud800`} {
		t.Run(fmt.Sprintf("%x", bad), func(t *testing.T) {
			s, c := setup(t)
			k := ds.NewFolderKey()
			f := folder(t, c, "folder", k)
			p := ds.Principal{Tenant: "guest", Subject: "\uFFFD"}
			if _, e := s.GetFolder(ctx, p, f.ID); !errors.Is(e, ds.ErrDenied) {
				t.Fatal(e)
			}
			body := fmt.Sprintf(`{"Op":"grant","Folder":%q,"Grantee":{"Tenant":"guest","Subject":"%s"},"Role":"reader"}`, f.ID, bad)
			h := s.Handler(func(*http.Request) (ds.Principal, error) { return owner, nil })
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/rpc", strings.NewReader(body)))
			_, e := s.GetFolder(ctx, p, f.ID)
			t.Logf("grant response=%d; replacement principal GetFolder=%v", w.Code, e)
			if e == nil {
				t.Fatal("invalid raw JSON principal granted access to valid replacement-character principal")
			}
		})
	}
}
func TestWriterOwnerUsageLeak(t *testing.T) {
	s, c := setup(t)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) { return ds.Quota{MaxTotalBytes: 10000}, nil })
	k := ds.NewFolderKey()
	uncapped, e := ds.CreateFolder(ctx, c, ds.FolderSpec{Name: "uncapped"}, k)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Grant(ctx, uncapped.ID, ds.Principal{Tenant: "other-tenant", Subject: "collaborator"}, ds.Writer); e == nil {
		t.Error("uncapped grant exposes owner usage oracle")
	}
	shared := folder(t, c, "shared", k)
	private := folder(t, c, "ungranted", k)
	row := put(t, c, private, k, "secret", 0, "private usage amount")
	outsider := ds.Principal{Tenant: "other-tenant", Subject: "collaborator"}
	if e := c.Grant(ctx, shared.ID, outsider, ds.Writer); e != nil {
		t.Fatal(e)
	}
	h := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) { return outsider, nil }))
	defer h.Close()
	guest := ds.NewHTTPClient(h.URL, nil)
	fs, e := guest.ListFolders(ctx)
	if e != nil || len(fs) != 1 {
		t.Fatal(fs, e)
	}
	_, e = guest.Reserve(ctx, shared.ID, ds.UploadRequest{PathID: strings.Repeat("a", 64), SealedSize: 10000})
	var le *ds.LimitError
	if !errors.As(e, &le) {
		t.Fatal(e)
	}
	t.Logf("listed folders=%d; error=%v; inferred private bytes=%d; actual private bytes=%d", len(fs), e, le.Requested-10000, row.SealedSize)
	if le.Requested-10000 == row.SealedSize {
		t.Fatal("cross-tenant writer learns exact sealed usage in ungranted private folder")
	}
}
func newReplica(t *testing.T, c ds.Client, f ds.Folder, k ds.FolderKey) (*ds.Replica, string) {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, e := ds.Attach(ctx, c, f.ID, k, dir, ds.Options{Manual: true, StateDir: filepath.Join(base, "state")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r, dir
}
func readFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	e := filepath.WalkDir(dir, func(p string, d os.DirEntry, e error) error {
		if d != nil && d.Name() == ".drivesync-root" {
			return nil
		}
		if e != nil {
			return e
		}
		if !d.IsDir() {
			b, e := os.ReadFile(p)
			if e != nil {
				return e
			}
			rel, _ := filepath.Rel(dir, p)
			out[rel] = string(b)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestCaseAliasCollision(t *testing.T) {
	_, c := setup(t)
	k := ds.NewFolderKey()
	f := folder(t, c, "folder", k)
	pid, _ := ds.PathID(k, f.ID, "foo.txt")
	alias := "foo (case conflict " + pid[:32] + ").txt"
	put(t, c, f, k, "Foo.txt", 0, "upper content")
	put(t, c, f, k, "foo.txt", 0, "lower content")
	put(t, c, f, k, alias, 0, "preexisting alias content")
	r, dir := newReplica(t, c, f, k)
	for n := 0; n < 4; n++ {
		e := r.Sync(ctx)
		t.Logf("sync %d err=%v files=%v", n, e, readFiles(t, dir))
		if e != nil {
			t.Fatal(e)
		}
	}
	delta, e := c.Changes(ctx, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, row := range delta.Rows {
		if row.Deleted {
			t.Logf("tombstone pathID=%s", row.PathID)
			continue
		}
		m, _ := ds.OpenMetadata(k, f.ID, row)
		t.Logf("remote %s", m.Path)
	}
	db, e := sql.Open("sqlite", filepath.Join(filepath.Dir(dir), "state", "index.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var count int
	e = db.QueryRow("SELECT COUNT(*) FROM entries WHERE json_extract(data,'$.Local')=?", alias).Scan(&count)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("distinct remote entries mapped onto same local filename: %d", count)
	if count != 1 {
		t.Fatalf("one-to-one path mapping violated: %d distinct remote paths share %q", count, alias)
	}
}
func TestUnignoreRemoteFile(t *testing.T) {
	_, c := setup(t)
	k := ds.NewFolderKey()
	f := folder(t, c, "folder", k)
	put(t, c, f, k, "hidden", 0, "must appear after unignore")
	r, dir := newReplica(t, c, f, k)
	if e := os.WriteFile(filepath.Join(dir, ".drivesyncignore"), []byte("hidden\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(filepath.Join(dir, ".drivesyncignore")); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < 3; n++ {
		if e := r.Sync(ctx); e != nil {
			t.Fatal(e)
		}
	}
	_, e := os.ReadFile(filepath.Join(dir, "hidden"))
	if e != nil {
		t.Fatal("remote file remains missing after ignore removed:", e)
	}
}

var _ = json.Marshal
var _ = io.Discard
