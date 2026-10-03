package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Growing folders, owners, tickets and garbage together must grow accounting
// work roughly linearly, rather than recomputing every contribution per owner.
func TestFinishAccountingLinear(t *testing.T) {
	fixture := func(n int) *Metadata {
		m := newMetadata()
		for j := 0; j < n; j++ {
			id := fmt.Sprintf("%032x", j)
			p := Principal{"tenant", fmt.Sprint(j)}
			m.Folders[id] = FolderRecord{Folder: Folder{ID: id, Owner: p}, FileUsage: Usage{Bytes: 100, Rows: 1, Files: 1}}
			m.Tickets[id] = Ticket{FolderID: id, ReservedBytes: 100, ReservedRows: 1}
			m.Garbage[id] = Garbage{FolderID: id, Owner: p, Size: 100}
		}
		return m
	}
	measure := func(n int) (float64, time.Duration) {
		m := fixture(n)
		start := time.Now()
		allocs := testing.AllocsPerRun(3, func() { m.Prepare(false); m.Finish() })
		return allocs, time.Since(start)
	}
	small, st := measure(128)
	large, lt := measure(1024)
	t.Logf("8x records: allocations %.0f -> %.0f; time %v -> %v", small, large, st, lt)
	if large > 14*small+100 || lt > 24*st+100*time.Millisecond {
		t.Fatalf("accounting is superlinear: allocations %.0f / %.0f, time %v / %v", large, small, lt, st)
	}
}

type inspectedPaths struct {
	*SQLiteMetaStore
	observed int
	largest  int
}

func (m *inspectedPaths) Transaction(ctx context.Context, fn func(*Metadata) error) error {
	return m.SQLiteMetaStore.Transaction(ctx, func(v *Metadata) error {
		scope, _ := ScopeFromContext(ctx)
		if scope.Folder != "" && !scope.ReadOnly {
			m.observed++
			for _, rows := range v.Files {
				m.largest = max(m.largest, len(rows))
			}
		}
		return fn(v)
	})
}
func TestWritesLoadOnlyTouchedPaths(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx := context.Background()
	if e := m.Transaction(ctx, func(v *Metadata) error {
		record := v.Folders[f.ID]
		record.Folder.Version = 1
		v.Folders[f.ID] = record
		for j := 0; j < 10000; j++ {
			pid, _ := PathID(k, f.ID, fmt.Sprint(j))
			v.Files[f.ID][pid] = Row{FolderID: f.ID, PathID: pid, Version: 1, Deleted: true}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	wrapped := &inspectedPaths{SQLiteMetaStore: m}
	s.Meta = wrapped
	put(t, c, f, k, "fresh", 0, []byte("value"))
	if wrapped.observed < 4 || wrapped.largest > 1 {
		t.Fatalf("write transactions=%d loaded rows=%d", wrapped.observed, wrapped.largest)
	}
	got, e := c.GetFolder(ctx, f.ID)
	if e != nil || got.Usage.Rows != 10001 || got.Usage.Files != 1 {
		t.Fatal("partial cache lost full usage", got.Usage, e)
	}
}
func TestReadsDoNotTakeWriterLock(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	put(t, c, f, k, "live", 0, []byte("data"))
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- m.Transaction(context.Background(), func(*Metadata) error { close(held); <-release; return nil })
	}()
	<-held
	defer func() {
		close(release)
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := c.GetFolder(ctx, f.ID); e != nil {
		t.Fatal("GetFolder blocked", e)
	}
	if _, e := c.ListFolders(ctx); e != nil {
		t.Fatal("ListFolders blocked", e)
	}
	d, e := c.Changes(ctx, f.ID, 0)
	if e != nil || len(d.Rows) != 1 {
		t.Fatal("Changes blocked", e)
	}
	reader, de := c.Download(ctx, f.ID, d.Rows[0].BlobID)
	if de != nil {
		t.Fatal("authorization blocked", de)
	}
	if e = reader.Close(); e != nil {
		t.Fatal("authorization blocked", e)
	}
	if _, e = s.Client(Principal{"unknown", "u"}).GetFolder(ctx, strings.Repeat("a", 32)); e != ErrDenied {
		t.Fatal("denied check blocked", e)
	}
}

func TestMissingCompactedBaseAcceptsOnlyHorizon(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	now := time.Now()
	s.Now = func() time.Time { return now }
	s.TombstoneTTL = time.Hour
	row := put(t, c, f, k, "file", 0, []byte("data"))
	d, e := c.Commit(context.Background(), f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}})
	if e != nil {
		t.Fatal(e)
	}
	now = now.Add(2 * time.Hour)
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	for _, base := range []uint64{d.Rows[0].Version + 1, ^uint64(0)} {
		if _, e = c.Reserve(context.Background(), f.ID, UploadRequest{PathID: row.PathID, BaseVersion: base, SealedSize: 1}); e == nil {
			t.Fatal("accepted future compacted base", base)
		}
	}
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: row.PathID, BaseVersion: d.Rows[0].Version, SealedSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.CancelUpload(context.Background(), f.ID, ticket.ID); e != nil {
		t.Fatal(e)
	}
}
