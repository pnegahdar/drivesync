package drivesync

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pnegahdar/drivesync/internal/engine"
)

func apiServer(t testing.TB, opts ServerOptions) *Server {
	t.Helper()
	meta, e := OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { meta.Close() })
	return NewServer(meta, NewMemoryBlobStore(), opts)
}

type testReplica struct {
	*Replica
	dir string
}

func apiReplica(t testing.TB, c *Client, f Folder, k FolderKey, name string, manual bool) *testReplica {
	t.Helper()
	base := t.TempDir()
	r, e := Attach(context.Background(), c, f.ID, k, filepath.Join(base, "files"), Options{Name: name, StateDir: filepath.Join(base, "state"), Manual: manual})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return &testReplica{Replica: r, dir: filepath.Join(base, "files")}
}
func apiWrite(t testing.TB, r *testReplica, p, data string) {
	t.Helper()
	if e := os.WriteFile(filepath.Join(r.dir, p), []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
}

func TestFolderAndReplicaAPI(t *testing.T) {
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			s := apiServer(t, ServerOptions{})
			alice := Principal{"tenant", "alice"}
			bob := Principal{"tenant", "bob"}
			other := Principal{"elsewhere", "alice"}
			factory := s.Client
			if transport == "http" {
				host := httptest.NewServer(s.Handler(func(r *http.Request) (Principal, error) {
					switch r.Header.Get("Authorization") {
					case "alice":
						return alice, nil
					case "bob":
						return bob, nil
					case "other":
						return other, nil
					}
					return Principal{}, ErrDenied
				}))
				t.Cleanup(host.Close)
				factory = func(p Principal) *Client {
					token := "other"
					if p == alice {
						token = "alice"
					}
					if p == bob {
						token = "bob"
					}
					return NewHTTPClient(host.URL, http.Header{"Authorization": {token}})
				}
			}
			c := factory(alice)
			key := NewFolderKey()
			f, e := c.CreateFolder(ctx, FolderSpec{Name: "shared", Description: "example", Limits: Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 16, MaxRows: 100}}, key)
			if e != nil {
				t.Fatal(e)
			}
			if f.Owner != alice || f.Role != Owner || f.Name != "shared" {
				t.Fatal(f)
			}
			if e = c.Grant(ctx, f.ID, bob, Writer); e != nil {
				t.Fatal(e)
			}
			if _, e = factory(other).GetFolder(ctx, f.ID); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			if _, e = factory(other).GetFolder(ctx, strings.Repeat("0", 32)); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			a, b := apiReplica(t, c, f, key, "a", true), apiReplica(t, factory(bob), f, key, "b", true)
			apiWrite(t, a, "file.txt", "from a")
			if e = a.Sync(ctx); e != nil {
				t.Fatal(e)
			}
			if e = b.Sync(ctx); e != nil {
				t.Fatal(e)
			}
			apiWrite(t, b, "file.txt", "from b")
			if e = b.Sync(ctx); e != nil {
				t.Fatal(e)
			}
			if e = a.Sync(ctx); e != nil {
				t.Fatal(e)
			}
			content, e := os.ReadFile(filepath.Join(a.dir, "file.txt"))
			if e != nil || string(content) != "from b" {
				t.Fatal(string(content), e)
			}
			got, e := c.GetFolder(ctx, f.ID)
			if e != nil || got.Usage.Files != 1 || got.Usage.Bytes == 0 {
				t.Fatal(got, e)
			}
			list, e := factory(bob).ListFolders(ctx)
			if e != nil || len(list) != 1 || list[0].Role != Writer {
				t.Fatal(list, e)
			}
			if _, e = Attach(ctx, c, f.ID, NewFolderKey(), filepath.Join(t.TempDir(), "bad"), Options{Manual: true}); !errors.Is(e, ErrKey) {
				t.Fatal(e)
			}
			status := b.Status()
			if status.LastSync.IsZero() || status.PendingUpBytes != 0 || len(status.Rejected) != 0 {
				t.Fatal(status)
			}
			if e = c.SetLimits(ctx, f.ID, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 16, MaxRows: 100}); e != nil {
				t.Fatal(e)
			}
			if e = c.Revoke(ctx, f.ID, bob); e != nil {
				t.Fatal(e)
			}
			if _, e = factory(bob).GetFolder(ctx, f.ID); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			if e = c.DeleteFolder(ctx, f.ID); e != nil {
				t.Fatal(e)
			}
			if _, e = c.GetFolder(ctx, f.ID); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
		})
	}
}

func TestAPIHidesProtocolFields(t *testing.T) {
	want := map[reflect.Type][]string{
		reflect.TypeFor[Folder]():     {"ID", "Name", "Description", "Owner", "Role", "Limits", "Usage"},
		reflect.TypeFor[FolderSpec](): {"Name", "Description", "Limits"},
		reflect.TypeFor[Usage]():      {"Bytes", "Files", "Reserved"},
		reflect.TypeFor[Status]():     {"PendingUpBytes", "PendingDownBytes", "Conflicts", "Rejected", "Quarantined", "LastSync", "Errors"},
	}
	for typ, fields := range want {
		if typ.NumField() != len(fields) {
			t.Fatal(typ, typ.NumField())
		}
		for j, name := range fields {
			if typ.Field(j).Name != name || !typ.Field(j).IsExported() {
				t.Fatal(typ, name)
			}
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[*Server](), reflect.TypeFor[*Client](), reflect.TypeFor[*Replica](), reflect.TypeFor[*MetaStore]()} {
		for j := 0; j < typ.Elem().NumField(); j++ {
			if typ.Elem().Field(j).IsExported() {
				t.Fatal("exported implementation field", typ, typ.Elem().Field(j).Name)
			}
		}
	}
}

func TestCreateComputesKeyCheckLocally(t *testing.T) {
	s := apiServer(t, ServerOptions{})
	handler := s.Handler(func(*http.Request) (Principal, error) { return Principal{"t", "u"}, nil })
	key := NewFolderKey()
	capture := make(chan []byte, 1)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, e := io.ReadAll(r.Body)
		if e != nil {
			t.Error(e)
		}
		capture <- body
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler.ServeHTTP(w, r)
	}))
	defer host.Close()
	c := NewHTTPClient(host.URL, nil)
	if _, e := c.CreateFolder(context.Background(), FolderSpec{Name: "folder"}, key); e != nil {
		t.Fatal(e)
	}
	captured := <-capture
	var request engine.WireRequest
	if e := json.Unmarshal(captured, &request); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(request.Spec.KeyCheck, engine.KeyCheck(engine.FolderKey(key))) || bytes.Contains(captured, []byte(hex.EncodeToString(key[:]))) {
		t.Fatal("raw key sent or key check missing")
	}
}

func TestOptionsAndPublicLimitError(t *testing.T) {
	clock := func() time.Time { return time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC) }
	s := apiServer(t, ServerOptions{Quotas: QuotaFunc(func(ctx context.Context, p Principal) (Quota, error) {
		if p != (Principal{"t", "u"}) {
			t.Error(p)
		}
		return Quota{MaxFolders: 1}, nil
	}), Clock: clock, ReservationTTL: time.Minute, TombstoneTTL: -1, GCInterval: time.Millisecond})
	if s.server.Now() != clock() {
		t.Fatal("clock option lost")
	}
	c := s.Client(Principal{"t", "u"})
	key := NewFolderKey()
	if _, e := c.CreateFolder(context.Background(), FolderSpec{Name: "one"}, key); e != nil {
		t.Fatal(e)
	}
	_, e := c.CreateFolder(context.Background(), FolderSpec{Name: "two"}, key)
	var le *LimitError
	if !errors.As(e, &le) || le.Limit != "owner folders" || le.Maximum != 1 || le.Requested != 2 {
		t.Fatal(e)
	}
}
