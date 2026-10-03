package drivesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var owner = Principal{"tenant", "owner"}

func testServer(t testing.TB) (*Server, *SQLiteMetaStore) {
	t.Helper()
	m, e := OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	return NewServer(m, NewMemoryBlobStore()), m
}
func folderFor(t testing.TB, c Client, l Limits) (Folder, FolderKey) {
	t.Helper()
	k := NewFolderKey()
	f, e := c.CreateFolder(context.Background(), FolderSpec{Name: randomID(), Limits: l, KeyCheck: KeyCheck(k)})
	if e != nil {
		t.Fatal(e)
	}
	return f, k
}
func clients(t testing.TB, s *Server, httpMode bool) func(Principal) Client {
	if !httpMode {
		return func(p Principal) Client { return s.Client(p) }
	}
	server := httptest.NewServer(s.Handler(func(r *http.Request) (Principal, error) {
		return Principal{r.Header.Get("Tenant"), r.Header.Get("Subject")}, nil
	}))
	t.Cleanup(server.Close)
	return func(p Principal) Client {
		return NewHTTPClient(server.URL, http.Header{"Tenant": {p.Tenant}, "Subject": {p.Subject}})
	}
}
func put(t testing.TB, c Client, f Folder, k FolderKey, p string, base uint64, data []byte) Row {
	t.Helper()
	ctx := context.Background()
	pid, e := PathID(k, f.ID, p)
	if e != nil {
		t.Fatal(e)
	}
	ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, base, SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	var sealed bytes.Buffer
	if e = SealContent(&sealed, bytes.NewReader(data), k, f.ID, ticket.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, &sealed); e != nil {
		t.Fatal(e)
	}
	h := hashBytes(data)
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: p, BlobID: ticket.BlobID, Size: int64(len(data)), Mode: 0600, Hash: h})
	if e != nil {
		t.Fatal(e)
	}
	d, e := c.Commit(ctx, f.ID, []Mutation{{PathID: pid, BaseVersion: base, TicketID: ticket.ID, Metadata: meta}})
	if e != nil {
		t.Fatal(e)
	}
	return d.Rows[0]
}
func TestAccessMatrix(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			roles := []struct {
				name                string
				p                   Principal
				read, write, manage bool
			}{{"owner", owner, true, true, true}, {"delegated owner", Principal{"tenant", "delegated"}, true, true, true}, {"writer", Principal{"tenant", "writer"}, true, true, false}, {"reader", Principal{"tenant", "reader"}, true, false, false}, {"other tenant", Principal{"other", "owner"}, false, false, false}, {"ungranted", Principal{"tenant", "stranger"}, false, false, false}, {"revoked", Principal{"tenant", "revoked"}, false, false, false}, {"anonymous", Principal{}, false, false, false}}
			methods := []string{"get", "changes", "wait", "download", "reserve", "upload", "commit", "cancel", "grant", "revoke", "delete", "limits", "list", "create"}
			for _, role := range roles {
				for _, method := range methods {
					t.Run(role.name+"/"+method, func(t *testing.T) {
						s, _ := testServer(t)
						makeClient := clients(t, s, httpMode)
						admin := makeClient(owner)
						f, k := folderFor(t, admin, Limits{})
						ctx := context.Background()
						if role.name == "writer" || role.name == "reader" || role.name == "revoked" || role.name == "delegated owner" {
							r := Writer
							if role.name == "delegated owner" {
								r = Owner
							}
							if role.name == "reader" {
								r = Reader
							}
							if e := admin.Grant(ctx, f.ID, role.p, r); e != nil {
								t.Fatal(e)
							}
							if role.name == "revoked" {
								_ = admin.Revoke(ctx, f.ID, role.p)
							}
						}
						row := put(t, admin, f, k, "file", 0, []byte("secret"))
						c := makeClient(role.p)
						allowed := role.read
						var e error
						switch method {
						case "get":
							_, e = c.GetFolder(ctx, f.ID)
						case "changes":
							_, e = c.Changes(ctx, f.ID, 0)
						case "wait":
							_, e = c.Wait(ctx, f.ID, 0)
						case "download":
							var r io.ReadCloser
							r, e = c.Download(ctx, f.ID, row.BlobID)
							if e == nil {
								_, e = io.ReadAll(r)
								r.Close()
							}
						case "reserve":
							allowed = role.write
							_, e = c.Reserve(ctx, f.ID, UploadRequest{row.PathID, row.Version, 1})
						case "upload":
							allowed = role.write
							creator := admin
							if allowed {
								creator = c
							}
							pid := fmt.Sprintf("%064x", 99)
							ticket, ce := creator.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
							if ce != nil {
								t.Fatal(ce)
							}
							e = c.Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1}))
						case "commit":
							allowed = role.write
							_, e = c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}})
						case "cancel":
							allowed = role.write
							creator := admin
							if allowed {
								creator = c
							}
							pid := fmt.Sprintf("%064x", 99)
							ticket, ce := creator.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
							if ce != nil {
								t.Fatal(ce)
							}
							e = c.CancelUpload(ctx, f.ID, ticket.ID)
						case "grant":
							allowed = role.manage
							e = c.Grant(ctx, f.ID, Principal{"tenant", "new"}, Reader)
						case "revoke":
							allowed = role.manage
							e = c.Revoke(ctx, f.ID, Principal{"tenant", "new"})
						case "delete":
							allowed = role.manage
							e = c.DeleteFolder(ctx, f.ID)
						case "limits":
							allowed = role.manage
							e = c.SetLimits(ctx, f.ID, Limits{})
						case "list":
							allowed = role.p.valid()
							var folders []Folder
							folders, e = c.ListFolders(ctx)
							if e == nil {
								want := 0
								if role.read {
									want = 1
								}
								if len(folders) != want {
									t.Fatalf("listed %d, want %d", len(folders), want)
								}
							}
						case "create":
							allowed = role.p.valid()
							_, e = c.CreateFolder(ctx, FolderSpec{Name: "own", KeyCheck: KeyCheck(k)})
						}
						if allowed && e != nil {
							t.Fatalf("allowed: %v", e)
						}
						if !allowed && !errors.Is(e, ErrDenied) {
							t.Fatalf("denied: %v", e)
						}
					})
				}
			}
		})
	}
}
func TestOpaqueDenialAndBlobBinding(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			cc := clients(t, s, httpMode)
			admin := cc(owner)
			f, k := folderFor(t, admin, Limits{})
			g, _ := folderFor(t, admin, Limits{})
			row := put(t, admin, f, k, "secret", 0, []byte("bytes"))
			ctx := context.Background()
			stranger := cc(Principal{"tenant", "other"})
			for _, id := range []string{f.ID, randomID()} {
				if _, e := stranger.GetFolder(ctx, id); e != ErrDenied {
					t.Fatal(e)
				}
			}
			if _, e := admin.Download(ctx, g.ID, row.BlobID); e != ErrDenied {
				t.Fatal(e)
			}
			pid, _ := PathID(k, f.ID, "pending")
			ticket, e := admin.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
			if e != nil {
				t.Fatal(e)
			}
			if e = admin.Upload(ctx, g.ID, ticket, bytes.NewReader([]byte{1})); e != ErrDenied {
				t.Fatal(e)
			}
			_, e = admin.Commit(ctx, g.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: make([]byte, 32)}})
			if e != ErrDenied {
				t.Fatal(e)
			}
			writer := Principal{"tenant", "writer"}
			_ = admin.Grant(ctx, f.ID, writer, Writer)
			if e = cc(writer).Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != ErrDenied {
				t.Fatal(e)
			}
			_ = admin.DeleteFolder(ctx, f.ID)
			if _, e = admin.Download(ctx, f.ID, row.BlobID); e != ErrDenied {
				t.Fatal(e)
			}
			if _, e = admin.Changes(ctx, f.ID, 0); e != ErrDenied {
				t.Fatal(e)
			}
		})
	}
}
func TestImmediateRevocation(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			cc := clients(t, s, httpMode)
			admin := cc(owner)
			f, k := folderFor(t, admin, Limits{})
			p := Principal{"tenant", "writer"}
			_ = admin.Grant(context.Background(), f.ID, p, Writer)
			c := cc(p)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			row := put(t, admin, f, k, "file", 0, []byte("payload"))
			open, e := c.Download(ctx, f.ID, row.BlobID)
			if e != nil {
				t.Fatal(e)
			}
			defer open.Close()
			pid, _ := PathID(k, f.ID, "next")
			ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
			if e != nil {
				t.Fatal(e)
			}
			waited := make(chan error, 1)
			go func() { _, e := c.Wait(ctx, f.ID, row.Version); waited <- e }()
			if e = admin.Revoke(ctx, f.ID, p); e != nil {
				t.Fatal(e)
			}
			if e = <-waited; e != ErrDenied {
				t.Fatal(e)
			}
			if e = c.Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != ErrDenied {
				t.Fatal(e)
			}
			if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: make([]byte, 32)}}); e != ErrDenied {
				t.Fatal(e)
			}
			if !httpMode {
				if _, e = open.Read(make([]byte, 1)); e != ErrDenied {
					t.Fatal(e)
				}
			}
		})
	}
}
func TestSubscriptions(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	p := Principal{"t", "p"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = c.Grant(ctx, f.ID, p, Reader)
	ch, e := s.Subscribe(ctx, p, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	put(t, c, f, k, "x", 0, []byte("x"))
	select {
	case event := <-ch:
		if event.Version != 1 || event.Err != nil {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("no event")
	}
	_ = c.Revoke(ctx, f.ID, p)
	select {
	case event := <-ch:
		if event.Err != ErrDenied {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("no revocation event")
	}
}
func TestLimitsBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, httpMode := range []bool{false, true} {
		for _, which := range []string{"file", "bytes", "files"} {
			t.Run(fmt.Sprint(httpMode)+which, func(t *testing.T) {
				s, _ := testServer(t)
				c := clients(t, s, httpMode)(owner)
				l := Limits{}
				switch which {
				case "file":
					l.MaxFileBytes = SealedSize(3)
				case "bytes":
					l.MaxTotalBytes = SealedSize(3)
				case "files":
					l.MaxFiles = 1
				}
				f, k := folderFor(t, c, l)
				row := put(t, c, f, k, "one", 0, []byte("abc"))
				p, _ := PathID(k, f.ID, "two")
				size := SealedSize(1)
				base := uint64(0)
				if which == "file" {
					p = row.PathID
					base = row.Version
					size = SealedSize(4)
				}
				_, e := c.Reserve(ctx, f.ID, UploadRequest{p, base, size})
				var le *LimitError
				if !errors.As(e, &le) {
					t.Fatalf("expected limit: %v", e)
				}
				if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
					t.Fatal(e)
				}
				if _, e = c.Reserve(ctx, f.ID, UploadRequest{p, 0, SealedSize(1)}); e != nil && which != "file" {
					t.Fatal(e)
				}
			})
		}
	}
}
func TestReservationRacesExpiryAndLies(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, _ := folderFor(t, c, Limits{MaxTotalBytes: 10, MaxFiles: 10})
	ctx := context.Background()
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := fmt.Sprintf("%064x", i)
			if _, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1}); e == nil {
				count.Add(1)
			} else {
				var l *LimitError
				if !errors.As(e, &l) {
					t.Error(e)
				}
			}
		}()
	}
	wg.Wait()
	if count.Load() != 10 {
		t.Fatalf("%d reservations", count.Load())
	}
	got, _ := c.GetFolder(ctx, f.ID)
	if got.Usage.Reserved != 10 {
		t.Fatal(got.Usage)
	}
	s.Now = func() time.Time { return time.Now().Add(time.Hour) }
	got, _ = c.GetFolder(ctx, f.ID)
	if got.Usage.Reserved != 0 {
		t.Fatal(got.Usage)
	}
	pid := fmt.Sprintf("%064x", 200)
	ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 3})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, bytes.NewBufferString("1234")); e != ErrInvalid {
		t.Fatal(e)
	}
	ticket, e = c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 3})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, bytes.NewBufferString("12")); e != ErrInvalid {
		t.Fatal(e)
	}
}
func TestOwnerQuotaAndLowering(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	ctx := context.Background()
	var max atomic.Int64
	max.Store(SealedSize(3))
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) {
		return Quota{MaxTotalBytes: max.Load(), MaxFolders: 2, MaxFileBytes: SealedSize(3)}, nil
	})
	f, k := folderFor(t, c, Limits{MaxFileBytes: 1000})
	row := put(t, c, f, k, "x", 0, []byte("abc"))
	g, _ := folderFor(t, c, Limits{})
	pid, _ := PathID(k, g.ID, "x")
	if _, e := c.Reserve(ctx, g.ID, UploadRequest{pid, 0, 1}); e == nil {
		t.Fatal("owner quota exceeded")
	}
	if _, e := c.CreateFolder(ctx, FolderSpec{Name: "third", KeyCheck: KeyCheck(k)}); e == nil {
		t.Fatal("folder quota exceeded")
	}
	max.Store(1)
	if _, e := c.Reserve(ctx, f.ID, UploadRequest{row.PathID, row.Version, 1}); e == nil {
		t.Fatal("write below usage allowed")
	}
	if _, e := c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Reserve(ctx, g.ID, UploadRequest{pid, 0, 1}); e != nil {
		t.Fatal(e)
	}
}

func TestEveryOperationAfterDeletion(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			c := clients(t, s, httpMode)(owner)
			f, k := folderFor(t, c, Limits{})
			ctx := context.Background()
			row := put(t, c, f, k, "file", 0, []byte("file"))
			pid, _ := PathID(k, f.ID, "pending")
			ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
			if e != nil {
				t.Fatal(e)
			}
			if e = c.DeleteFolder(ctx, f.ID); e != nil {
				t.Fatal(e)
			}
			methods := map[string]func() error{
				"get": func() error { _, e := c.GetFolder(ctx, f.ID); return e }, "changes": func() error { _, e := c.Changes(ctx, f.ID, 0); return e }, "wait": func() error { _, e := c.Wait(ctx, f.ID, 0); return e }, "download": func() error { _, e := c.Download(ctx, f.ID, row.BlobID); return e }, "reserve": func() error { _, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1}); return e }, "upload": func() error { return c.Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})) }, "cancel": func() error { return c.CancelUpload(ctx, f.ID, ticket.ID) }, "commit": func() error {
					_, e := c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}})
					return e
				}, "grant": func() error { return c.Grant(ctx, f.ID, Principal{"t", "p"}, Reader) }, "revoke": func() error { return c.Revoke(ctx, f.ID, Principal{"t", "p"}) }, "delete": func() error { return c.DeleteFolder(ctx, f.ID) }, "limits": func() error { return c.SetLimits(ctx, f.ID, Limits{}) }, "subscribe": func() error { _, e := s.Subscribe(ctx, owner, f.ID, 0); return e }}
			for name, fn := range methods {
				if e := fn(); e != ErrDenied {
					t.Errorf("%s: %v", name, e)
				}
			}
			folders, e := c.ListFolders(ctx)
			if e != nil || len(folders) != 0 {
				t.Fatal(folders, e)
			}
		})
	}
}

func TestInvalidUTF8PrincipalsNeverAlias(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			cc := clients(t, s, httpMode)
			admin := cc(owner)
			f, k := folderFor(t, admin, Limits{})
			ctx := context.Background()
			replacement := Principal{"tenant", "\uFFFD"}
			if e := admin.Grant(ctx, f.ID, replacement, Reader); e != nil {
				t.Fatal(e)
			}
			if _, e := cc(replacement).GetFolder(ctx, f.ID); e != nil {
				t.Fatal(e)
			}
			for _, bad := range []Principal{{"tenant", "\xff"}, {"tenant", "\xfe"}, {"\xff", "owner"}} {
				if _, e := cc(bad).GetFolder(ctx, f.ID); e != ErrDenied {
					t.Fatal("invalid identity aliased", e)
				}
				if _, e := cc(bad).CreateFolder(ctx, FolderSpec{Name: "invalid", KeyCheck: KeyCheck(k)}); e != ErrDenied {
					t.Fatal(e)
				}
				if e := admin.Grant(ctx, f.ID, bad, Reader); e != ErrInvalid {
					t.Fatal(e)
				}
				if e := admin.Revoke(ctx, f.ID, bad); e != ErrInvalid {
					t.Fatal(e)
				}
			}
		})
	}
}
