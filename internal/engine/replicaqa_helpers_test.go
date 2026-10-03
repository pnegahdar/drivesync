package engine

// Replica/local-filesystem QA reproductions against c32b886.
// Copy this directory's *_test.go files into internal/engine and run
//   go test -run '^TestQA' -count=1 -v ./internal/engine
// Every test uses an explicit StateDir under t.TempDir() unless it is testing
// default state, in which case HOME/XDG_CACHE_HOME point into t.TempDir().

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var qaCtx = context.Background()

func qaAttach(t testing.TB, c Client, f Folder, k FolderKey, dir, name string) *Replica {
	t.Helper()
	r, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: name, Manual: true, StateDir: filepath.Join(t.TempDir(), "state-"+name), RescanInterval: time.Hour, RetryInterval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func qaSync(t testing.TB, rs ...*Replica) {
	t.Helper()
	for round := 0; round < 3; round++ {
		for _, r := range rs {
			if e := r.Sync(qaCtx); e != nil {
				t.Fatalf("%s sync: %v", r.opts.Name, e)
			}
		}
	}
}

// qaRemote decrypts the authority's current live rows: path -> content.
func qaRemote(t testing.TB, c Client, f Folder, k FolderKey) map[string]string {
	t.Helper()
	d, e := c.Changes(qaCtx, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	out := map[string]string{}
	for _, row := range d.Rows {
		if row.Deleted {
			continue
		}
		m, e := OpenMetadata(k, f.ID, row)
		if e != nil {
			t.Fatal(e)
		}
		if m.Directory {
			out[m.Path+"/"] = ""
			continue
		}
		rc, e := c.Download(qaCtx, f.ID, row.BlobID)
		if e != nil {
			t.Fatal(e)
		}
		var b bytes.Buffer
		e = OpenContent(&b, rc, k, f.ID, row.BlobID, row.PathID)
		rc.Close()
		if e != nil {
			t.Fatal(e)
		}
		out[m.Path] = b.String()
	}
	return out
}

// qaPut commits remote content with an explicit peer-chosen mode.
func qaPut(t testing.TB, c Client, f Folder, k FolderKey, p string, base uint64, data []byte, mode uint32) Row {
	t.Helper()
	pid, e := PathID(k, f.ID, p)
	if e != nil {
		t.Fatal(e)
	}
	ticket, e := c.Reserve(qaCtx, f.ID, UploadRequest{PathID: pid, BaseVersion: base, SealedSize: SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	var sealed bytes.Buffer
	if e = SealContent(&sealed, bytes.NewReader(data), k, f.ID, ticket.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(qaCtx, f.ID, ticket, &sealed); e != nil {
		t.Fatal(e)
	}
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: p, BlobID: ticket.BlobID, Size: int64(len(data)), Mode: mode, Hash: hashBytes(data)})
	if e != nil {
		t.Fatal(e)
	}
	d, e := c.Commit(qaCtx, f.ID, []Mutation{{PathID: pid, BaseVersion: base, TicketID: ticket.ID, Metadata: meta}})
	if e != nil {
		t.Fatal(e)
	}
	return d.Rows[0]
}

func qaRemoteVersion(t testing.TB, c Client, f Folder, k FolderKey, p string) uint64 {
	t.Helper()
	pid, _ := PathID(k, f.ID, p)
	d, e := c.Changes(qaCtx, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, row := range d.Rows {
		if row.PathID == pid {
			return row.Version
		}
	}
	return 0
}

// qaHookClient runs a one-shot hook before the next Changes call.
type qaHookClient struct {
	Client
	beforeChanges func()
	commits       int
}

func (c *qaHookClient) Changes(ctx context.Context, id string, v uint64) (Delta, error) {
	if h := c.beforeChanges; h != nil {
		c.beforeChanges = nil
		h()
	}
	return c.Client.Changes(ctx, id, v)
}
func (c *qaHookClient) Commit(ctx context.Context, id string, m []Mutation) (Delta, error) {
	c.commits++
	return c.Client.Commit(ctx, id, m)
}

func qaWrite(t testing.TB, full, content string) {
	t.Helper()
	if e := os.MkdirAll(filepath.Dir(full), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(full, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
}
