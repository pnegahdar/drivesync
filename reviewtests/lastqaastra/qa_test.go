package lastqa

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
	e "github.com/pnegahdar/drivesync/internal/engine"
)

var owner = e.Principal{Tenant: "tenant", Subject: "owner"}

func TestCompletedUploadAckFailureDoesNotPinCapacityForever(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "meta.sqlite")
			meta, err := e.OpenSQLiteMetaStore(file)
			if err != nil {
				t.Fatal(err)
			}
			defer meta.Close()
			s := e.NewServer(meta, e.NewMemoryBlobStore())
			c := client(t, s, owner, httpMode)
			f, k := folder(t, c, e.Limits{MaxTotalBytes: 257, MaxFileBytes: 1, MaxRows: 1})
			pid, _ := e.PathID(k, f.ID, "file")
			ticket, err := c.Reserve(context.Background(), f.ID, e.UploadRequest{PathID: pid, SealedSize: 1})
			if err != nil {
				t.Fatal(err)
			}
			// Force a real SQLite rollback precisely at the uploaded=true write;
			// all stores, protocol handlers and stream authorization remain real.
			db, err := sql.Open("sqlite", file)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err = db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE ON tickets WHEN json_extract(NEW.data,'$.Uploaded')=1 BEGIN SELECT RAISE(ABORT,'transient acknowledgement failure'); END`); err != nil {
				t.Fatal(err)
			}
			if err = c.Upload(context.Background(), f.ID, ticket, strings.NewReader("x")); err == nil {
				t.Fatal("fault did not fire")
			}
			if _, err = db.Exec(`DROP TRIGGER fail_ack`); err != nil {
				t.Fatal(err)
			}
			// Upload has returned; nothing can still publish this blob. Cancellation
			// plus healthy ordinary GC should now make its capacity reusable.
			if err = c.CancelUpload(context.Background(), f.ID, ticket.ID); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if err = s.CollectGarbage(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			_, err = c.Reserve(context.Background(), f.ID, e.UploadRequest{PathID: pid, SealedSize: 1})
			if err != nil {
				got, _ := c.GetFolder(context.Background(), f.ID)
				t.Fatalf("completed failed upload pins capacity despite cancellation and healthy GC: %v; usage=%+v", err, got.Usage)
			}
		})
	}
}

func server(t *testing.T) (*e.Server, *e.SQLiteMetaStore) {
	t.Helper()
	m, err := e.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return e.NewServer(m, e.NewMemoryBlobStore()), m
}

type corruptOnce struct {
	ds.BlobStore
	opens atomic.Int64
}

func (b *corruptOnce) Open(ctx context.Context, folder, id string) (io.ReadCloser, error) {
	r, err := b.BlobStore.Open(ctx, folder, id)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		return nil, err
	}
	if b.opens.Add(1) == 1 {
		data[len(data)-1] ^= 1
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestPublicReplicaCanRetryQuarantinedDownload(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			ctx := context.Background()
			meta, err := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer meta.Close()
			blobs := &corruptOnce{BlobStore: ds.NewMemoryBlobStore()}
			s := ds.NewServer(meta, blobs, ds.ServerOptions{})
			p := ds.Principal{Tenant: "tenant", Subject: "owner"}
			c := s.Client(p)
			if httpMode {
				h := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) { return p, nil }))
				defer h.Close()
				c = ds.NewHTTPClient(h.URL, nil)
			}
			k := ds.NewFolderKey()
			f, err := c.CreateFolder(ctx, ds.FolderSpec{Name: "quarantine"}, k)
			if err != nil {
				t.Fatal(err)
			}
			a, ad := replica(t, c, f, k)
			if err := os.WriteFile(filepath.Join(ad, "file"), []byte("valid stored content"), 0600); err != nil {
				t.Fatal(err)
			}
			syncOK(t, a)
			dir, state := filepath.Join(t.TempDir(), "files"), filepath.Join(t.TempDir(), "state")
			b, err := ds.Attach(ctx, c, f.ID, k, dir, ds.Options{Manual: true, StateDir: state})
			if err != nil {
				t.Fatal(err)
			}
			if err = b.Sync(ctx); !errors.Is(err, ds.ErrIntegrity) {
				b.Close()
				t.Fatalf("wanted corruption error, got %v", err)
			}
			if len(b.Status().Quarantined) != 1 {
				b.Close()
				t.Fatal("expected quarantined row")
			}
			b.Close()
			b, err = ds.Attach(ctx, c, f.ID, k, dir, ds.Options{Manual: true, StateDir: state})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			syncOK(t, b)
			// Preserve the deliberate no-automatic-redownload rule, but require an
			// operator-triggered recovery path on the public handle.
			retry, ok := any(b).(interface{ Retry() })
			if !ok {
				t.Fatalf("valid stored row remains quarantined across restart; public Replica has no RetryRejected, opens=%d status=%+v", blobs.opens.Load(), b.Status())
			}
			retry.Retry()
			syncOK(t, b)
			got, err := os.ReadFile(filepath.Join(dir, "file"))
			if err != nil || string(got) != "valid stored content" {
				t.Fatalf("retry failed: %q %v", got, err)
			}
		})
	}
}

// Public-key usability check: the ordinary Go zero value must not silently
// select a globally known encryption key.
func TestPublicCreateRejectsUninitializedFolderKey(t *testing.T) {
	c, _, _ := publicFixture(t, false)
	var key ds.FolderKey
	f, err := c.CreateFolder(context.Background(), ds.FolderSpec{Name: "forgot-to-initialize-key"}, key)
	if err == nil {
		t.Fatalf("uninitialized all-zero FolderKey accepted for folder %s; authority/storage observers can derive every encryption key", f.ID)
	}
}

func publicFixture(t *testing.T, httpMode bool) (*ds.Client, ds.Folder, ds.FolderKey) {
	t.Helper()
	m, err := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s := ds.NewServer(m, ds.NewMemoryBlobStore(), ds.ServerOptions{})
	p := ds.Principal{Tenant: "owner", Subject: "owner"}
	c := s.Client(p)
	if httpMode {
		h := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) { return p, nil }))
		t.Cleanup(h.Close)
		c = ds.NewHTTPClient(h.URL, nil)
	}
	k := ds.NewFolderKey()
	f, err := c.CreateFolder(context.Background(), ds.FolderSpec{Name: "files"}, k)
	if err != nil {
		t.Fatal(err)
	}
	return c, f, k
}
func replica(t *testing.T, c *ds.Client, f ds.Folder, k ds.FolderKey) (*ds.Replica, string) {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, err := ds.Attach(context.Background(), c, f.ID, k, dir, ds.Options{Manual: true, StateDir: filepath.Join(base, "state")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, dir
}
func syncOK(t *testing.T, r *ds.Replica) {
	t.Helper()
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPermissionOnlyChangePropagates(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			c, f, k := publicFixture(t, httpMode)
			a, ad := replica(t, c, f, k)
			b, bd := replica(t, c, f, k)
			if err := os.WriteFile(filepath.Join(ad, "script"), []byte("#!/bin/sh\necho hello\n"), 0600); err != nil {
				t.Fatal(err)
			}
			syncOK(t, a)
			syncOK(t, b)
			if err := os.Chmod(filepath.Join(ad, "script"), 0700); err != nil {
				t.Fatal(err)
			}
			syncOK(t, a)
			syncOK(t, b)
			syncOK(t, b)
			info, err := os.Stat(filepath.Join(bd, "script"))
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0700 {
				t.Fatalf("remote chmod silently ignored forever: want 0700 got %04o; status=%+v", info.Mode().Perm(), b.Status())
			}
		})
	}
}

func TestSymlinkedTrackedDirectoryDoesNotDeleteRemoteChildren(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			c, f, k := publicFixture(t, httpMode)
			a, ad := replica(t, c, f, k)
			b, bd := replica(t, c, f, k)
			if err := os.Mkdir(filepath.Join(ad, "dir"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(ad, "dir", "precious"), []byte("keep this content"), 0600); err != nil {
				t.Fatal(err)
			}
			syncOK(t, a)
			syncOK(t, b)
			outside := filepath.Join(t.TempDir(), "moved")
			if err := os.Rename(filepath.Join(ad, "dir"), outside); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(ad, "dir")); err != nil {
				t.Fatal(err)
			}
			syncOK(t, a)
			syncOK(t, b)
			data, err := os.ReadFile(filepath.Join(bd, "dir", "precious"))
			if err != nil || string(data) != "keep this content" {
				t.Fatalf("skipping a symlinked tracked parent generated remote child deletion: data=%q err=%v source status=%+v", data, err, a.Status())
			}
		})
	}
}

func client(t *testing.T, s *e.Server, p e.Principal, httpMode bool) e.Client {
	t.Helper()
	if !httpMode {
		return s.Client(p)
	}
	h := httptest.NewServer(s.Handler(func(r *http.Request) (e.Principal, error) { return p, nil }))
	t.Cleanup(h.Close)
	return e.NewHTTPClient(h.URL, nil)
}

func folder(t *testing.T, c e.Client, limits e.Limits) (e.Folder, e.FolderKey) {
	t.Helper()
	k := e.NewFolderKey()
	f, err := e.CreateFolder(context.Background(), c, e.FolderSpec{Name: "shared", Limits: limits}, k)
	if err != nil {
		t.Fatal(err)
	}
	return f, k
}

// The extra-byte oversize probe is stored by both built-in stores. A rejected
// upload must not leave sealed bytes plus the fixed row cost above its charge.
func TestOversizedUploadRetainsActualCharge(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%v", disk), func(t *testing.T) {
			for _, httpMode := range []bool{false, true} {
				t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
					s, m := server(t)
					if disk {
						store, err := e.OpenDirectoryBlobStore(filepath.Join(t.TempDir(), "blobs"))
						if err != nil {
							t.Fatal(err)
						}
						defer store.Close()
						s.Blobs = store
					}
					c := client(t, s, owner, httpMode)
					f, k := folder(t, c, e.Limits{MaxTotalBytes: 256, MaxRows: 1, MaxFileBytes: 1})
					writer := e.Principal{Tenant: "other", Subject: "writer"}
					if err := c.Grant(context.Background(), f.ID, writer, e.Writer); err != nil {
						t.Fatal(err)
					}
					c = client(t, s, writer, httpMode)
					pid, _ := e.PathID(k, f.ID, "oversized")
					ticket, err := c.Reserve(context.Background(), f.ID, e.UploadRequest{PathID: pid, SealedSize: 0})
					if err != nil {
						t.Fatal(err)
					}
					if err = c.Upload(context.Background(), f.ID, ticket, strings.NewReader("x")); !errors.Is(err, e.ErrInvalid) {
						t.Fatalf("expected oversize failure, got %v", err)
					}
					actual, err := s.Blobs.Size(context.Background(), f.ID, ticket.BlobID)
					if errors.Is(err, os.ErrNotExist) {
						actual = 0
						err = nil
					}
					if err != nil {
						t.Fatal(err)
					}
					var charged int64
					if err = m.Transaction(context.Background(), func(m *e.Metadata) error {
						for _, a := range m.Accounts {
							charged += a.Bytes
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					got, err := c.GetFolder(context.Background(), f.ID)
					if err != nil {
						t.Fatal(err)
					}
					physicalCost := actual + e.RowCost*got.Usage.GarbageRows
					if charged < physicalCost {
						t.Fatalf("owner charged %d, stored sealed bytes %d plus %d garbage row(s) cost %d; folder cap %d, reported usage %d", charged, actual, got.Usage.GarbageRows, physicalCost, f.Limits.MaxTotalBytes, got.Usage.Bytes)
					}
				})
			}
		})
	}
}

// Exercise the public management facade too. A denied request must remain a
// denial while an unrelated tenant is executing a slow but legitimate policy.
func TestDeniedMutationDoesNotObserveUnrelatedWriter(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			meta, err := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer meta.Close()
			entered, release := make(chan struct{}), make(chan struct{})
			s := ds.NewServer(meta, ds.NewMemoryBlobStore(), ds.ServerOptions{Quotas: ds.QuotaFunc(func(ctx context.Context, p ds.Principal) (ds.Quota, error) {
				if p.Tenant == "victim" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ds.Quota{}, ctx.Err()
					}
				}
				return ds.Quota{}, nil
			})})
			attacker := s.Client(ds.Principal{Tenant: "attacker", Subject: "user"})
			if httpMode {
				h := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) {
					return ds.Principal{Tenant: "attacker", Subject: "user"}, nil
				}))
				defer h.Close()
				attacker = ds.NewHTTPClient(h.URL, nil)
			}
			target := strings.Repeat("0", 32) // no grant, and no knowledge of any folder ID
			if err = attacker.DeleteFolder(context.Background(), target); !errors.Is(err, ds.ErrDenied) {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := s.Client(ds.Principal{Tenant: "victim", Subject: "owner"}).CreateFolder(context.Background(), ds.FolderSpec{Name: "secret"}, ds.NewFolderKey())
				done <- err
			}()
			<-entered
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			start := time.Now()
			err = attacker.DeleteFolder(ctx, target)
			elapsed := time.Since(start)
			cancel()
			close(release)
			if createErr := <-done; createErr != nil {
				t.Fatal(createErr)
			}
			if !errors.Is(err, ds.ErrDenied) {
				t.Fatalf("denied nonexistent-folder mutation exposes unrelated writer activity: idle=ErrDenied, busy=%v after %s", err, elapsed)
			}
		})
	}
}
