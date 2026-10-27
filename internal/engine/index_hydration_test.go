package engine

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Reopening must hydrate from the compact authenticated fields, including
// remote-only children and the original binding of a moved physical placeholder.
func TestCompactIndexHydrationAfterRestart(t *testing.T) {
	for _, remoteOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "physical", true: "remote-only"}[remoteOnly], func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			f, k := folderFor(t, c, Limits{})
			for _, p := range []string{"dir/file", "dir/other"} {
				putRemote(t, c, f, k, p, 0, []byte("content:"+p), 0700)
			}
			base := t.TempDir()
			opts := Options{Manual: true, OnDemand: true, StateDir: filepath.Join(base, "state")}
			if remoteOnly {
				opts.MaxFolderPlaceholders = 1
			}
			dir := filepath.Join(base, "files")
			r, e := Attach(syncCtx, c, f.ID, k, dir, opts)
			if e != nil {
				t.Fatal(e)
			}
			if e = r.Sync(syncCtx); e != nil {
				r.Close()
				t.Fatal(e)
			}
			if r.index["dir/file"].RemoteOnly != remoteOnly {
				t.Fatal(r.index["dir/file"])
			}
			r.Close()
			r, e = Attach(syncCtx, c, f.ID, k, dir, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			if remoteOnly {
				fetchOK(t, r, "dir/file")
				if !r.index["dir/other"].RemoteOnly {
					t.Fatal("fetched sibling")
				}
			} else {
				if e = os.Rename(filepath.Join(dir, "dir/file"), filepath.Join(dir, "dir/moved")); e != nil {
					t.Fatal(e)
				}
				if e = r.Sync(syncCtx); e != nil {
					t.Fatal(e)
				}
				if !r.index["dir/file"].Deleted || r.index["dir/moved"].Deleted {
					t.Fatal("move did not commit")
				}
			}
			p := "dir/file"
			if !remoteOnly {
				p = "dir/moved"
			}
			data, e := os.ReadFile(filepath.Join(dir, p))
			info, se := os.Stat(filepath.Join(dir, p))
			if e != nil || se != nil || string(data) != "content:dir/file" || info.Mode().Perm() != 0700 {
				t.Fatalf("content %q stat %v read %v stat %v", data, info, e, se)
			}
		})
	}
}

func TestCompactIndexChecksStoredContentFields(t *testing.T) {
	for _, field := range []string{"hash", "size", "path binding"} {
		t.Run(field, func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			f, k := folderFor(t, c, Limits{})
			// Equal plaintext hashes cannot make a blob valid at another path.
			putRemote(t, c, f, k, "file", 0, []byte("content"), 0600)
			putRemote(t, c, f, k, "other", 0, []byte("content"), 0600)
			r := lazyReplica(t, c, f, k)
			if e := r.Sync(syncCtx); e != nil {
				t.Fatal(e)
			}
			i := r.index["file"]
			switch field {
			case "hash":
				i.Hash[0] ^= 1
			case "size":
				i.Size++
			case "path binding":
				i.Blob = r.index["other"].Blob
			}
			if e := r.save(i); e != nil {
				t.Fatal(e)
			}
			got, e := r.Fetch(syncCtx, "file")
			if e != nil || len(got.Failed) != 1 || len(got.Fetched) != 0 {
				t.Fatal(got, e)
			}
			assertPlaceholder(t, r, "file")
			if len(r.Status().Quarantined) != 1 || r.Status().Quarantined[0].Path != "file" {
				t.Fatal(r.Status())
			}
		})
	}
}

type compactRetryClient struct {
	Client
	fail bool
}

func (c *compactRetryClient) Download(ctx context.Context, folder, blob string) (io.ReadCloser, error) {
	if c.fail {
		return nil, io.ErrUnexpectedEOF
	}
	return c.Client.Download(ctx, folder, blob)
}

func TestCompactIndexFetchRetryAfterRestart(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "file", 0, []byte("content"), 0600)
	c := &compactRetryClient{Client: base, fail: true}
	r := lazyReplica(t, c, f, k)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	got, e := r.Fetch(syncCtx, "file")
	if e != nil || len(got.Failed) != 1 {
		t.Fatal(got, e)
	}
	dir, opts := r.dir, r.opts
	r.Close()
	r, e = Attach(syncCtx, c, f.ID, k, dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = os.Chmod(filepath.Join(dir, "file"), 0600); e != nil {
		t.Fatal(e)
	}
	// Move the stored retry deadline to the past without sleeping on real time.
	for pid := range r.retryAt {
		r.retryAt[pid] = time.Time{}
	}
	c.fail = false
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(filepath.Join(dir, "file"))
	if e != nil || string(data) != "content" || len(r.Status().Quarantined) != 0 {
		t.Fatal(string(data), e, r.Status())
	}
	var pending int
	if e = r.db.QueryRow("SELECT count(*) FROM pending").Scan(&pending); e != nil || pending != 0 {
		t.Fatal(pending, e)
	}
}
