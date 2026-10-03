package drivesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLiteRollbackAndReopen(t *testing.T) {
	name := filepath.Join(t.TempDir(), "meta.sqlite")
	m, e := OpenSQLiteMetaStore(name)
	if e != nil {
		t.Fatal(e)
	}
	s := NewServer(m, NewMemoryBlobStore())
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	row := put(t, c, f, k, "file", 0, []byte("persisted"))
	if e = m.Transaction(context.Background(), func(meta *Metadata) error { delete(meta.Folders, f.ID); return errNetwork }); e != errNetwork {
		t.Fatal(e)
	}
	if e = m.Close(); e != nil {
		t.Fatal(e)
	}
	mm, e := OpenSQLiteMetaStore(name)
	if e != nil {
		t.Fatal(e)
	}
	defer mm.Close()
	ss := NewServer(mm, s.Blobs)
	d, e := ss.Client(owner).Changes(context.Background(), f.ID, 0)
	if e != nil || len(d.Rows) != 1 || d.Rows[0].Version != row.Version {
		t.Fatal(d, e)
	}
}
func TestConcurrentAuthorities(t *testing.T) {
	name := filepath.Join(t.TempDir(), "meta.sqlite")
	a, e := OpenSQLiteMetaStore(name)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := OpenSQLiteMetaStore(name)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	sa, sb := NewServer(a, NewMemoryBlobStore()), NewServer(b, NewMemoryBlobStore())
	f, _ := folderFor(t, sa.Client(owner), Limits{MaxTotalBytes: 7})
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server := sa
			if i%2 == 1 {
				server = sb
			}
			_, e := server.Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: fmt.Sprintf("%064x", i), SealedSize: 1})
			if e == nil {
				accepted.Add(1)
			} else {
				var l *LimitError
				if !errors.As(e, &l) {
					t.Error(e)
				}
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 7 {
		t.Fatal(accepted.Load())
	}
}
func TestBlobStoresImmutableAndInterrupted(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprint(directory), func(t *testing.T) {
			var b BlobStore = NewMemoryBlobStore()
			dir := t.TempDir()
			if directory {
				d, e := OpenDirectoryBlobStore(dir)
				if e != nil {
					t.Fatal(e)
				}
				defer d.Close()
				b = d
			}
			ctx := context.Background()
			f, id := randomID(), randomID()
			if n, e := b.Put(ctx, f, id, bytes.NewBufferString("bytes")); e != nil || n != 5 {
				t.Fatal(n, e)
			}
			if _, e := b.Put(ctx, f, id, bytes.NewBufferString("overwrite")); e == nil {
				t.Fatal("overwrote immutable object")
			}
			r, e := b.Open(ctx, f, id)
			if e != nil {
				t.Fatal(e)
			}
			data, e := io.ReadAll(r)
			r.Close()
			if e != nil || string(data) != "bytes" {
				t.Fatal(string(data), e)
			}
			bad := randomID()
			if _, e = b.Put(ctx, f, bad, &failReader{}); e == nil {
				t.Fatal("accepted interrupted upload")
			}
			if _, e = b.Size(ctx, f, bad); !errors.Is(e, os.ErrNotExist) {
				t.Fatal(e)
			}
			if directory {
				items, _ := os.ReadDir(filepath.Join(dir, f))
				if len(items) != 1 {
					t.Fatal(items)
				}
			}
			if e = b.Delete(ctx, f, id); e != nil {
				t.Fatal(e)
			}
		})
	}
}

type failReader struct{ sent bool }

func (r *failReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, errNetwork
	}
	r.sent = true
	copy(p, "partial")
	return 7, nil
}
func TestStoredSizeRecheckedAndBatchAtomicity(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx := context.Background()
	pid, _ := PathID(k, f.ID, "new")
	ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != nil {
		t.Fatal(e)
	}
	b := s.Blobs.(*MemoryBlobStore)
	b.mu.Lock()
	b.blobs[f.ID+"/"+ticket.BlobID] = []byte{1, 2}
	b.mu.Unlock()
	meta, _ := SealMetadata(k, f.ID, pid, FileMetadata{Path: "new", BlobID: ticket.BlobID, Mode: 0600})
	if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != ErrInvalid {
		t.Fatal(e)
	}
	_ = c.CancelUpload(ctx, f.ID, ticket.ID)
	a := put(t, c, f, k, "a", 0, []byte("a"))
	bb := put(t, c, f, k, "b", 0, []byte("b"))
	if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: a.PathID, BaseVersion: a.Version, Deleted: true}, {PathID: bb.PathID, BaseVersion: 0, Deleted: true}}); e != ErrConflict {
		t.Fatal(e)
	}
	delta, _ := c.Changes(ctx, f.ID, 0)
	for _, row := range delta.Rows {
		if row.Deleted {
			t.Fatal("partial batch")
		}
	}
}
func TestExactReservationBoundaries(t *testing.T) {
	for _, name := range []string{"file", "folder bytes", "owner bytes"} {
		t.Run(name, func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			l := Limits{}
			switch name {
			case "file":
				l.MaxFileBytes = 10
			case "folder bytes":
				l.MaxTotalBytes = 10
			case "owner bytes":
				s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxTotalBytes: 10}, nil })
			}
			f, k := folderFor(t, c, l)
			pid, _ := PathID(k, f.ID, "x")
			ctx := context.Background()
			ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 10})
			if e != nil {
				t.Fatal("exact limit", e)
			}
			if e = c.CancelUpload(ctx, f.ID, ticket.ID); e != nil {
				t.Fatal(e)
			}
			if _, e = c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 11}); e == nil {
				t.Fatal("limit+1 accepted")
			}
		})
	}
}
func TestOutstandingTicketsAfterDeletionAndDowngrade(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	p := Principal{"tenant", "writer"}
	ctx := context.Background()
	_ = c.Grant(ctx, f.ID, p, Writer)
	pid, _ := PathID(k, f.ID, "pending")
	ticket, e := s.Client(p).Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
	if e != nil {
		t.Fatal(e)
	}
	_ = c.Grant(ctx, f.ID, p, Reader)
	if e = s.Client(p).Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != ErrDenied {
		t.Fatal(e)
	}
	f, _ = c.GetFolder(ctx, f.ID)
	if f.Usage.Reserved != 0 {
		t.Fatal(f.Usage)
	}
	_ = c.Grant(ctx, f.ID, p, Writer)
	ticket, e = s.Client(p).Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
	if e != nil {
		t.Fatal(e)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { _, e := s.Client(p).Wait(waitCtx, f.ID, f.Version); errCh <- e }()
	_ = c.DeleteFolder(ctx, f.ID)
	if e = <-errCh; e != ErrDenied {
		t.Fatal(e)
	}
	if e = s.Client(p).Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != ErrDenied {
		t.Fatal(e)
	}
}

func TestCommitExcludesExpiredReservations(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx := context.Background()
	clock := time.Now()
	s.Now = func() time.Time { return clock }
	s.ReservationTTL = time.Minute
	var maximum atomic.Int64
	maximum.Store(100)
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxTotalBytes: maximum.Load()}, nil })
	oldPID, _ := PathID(k, f.ID, "old")
	if _, e := c.Reserve(ctx, f.ID, UploadRequest{oldPID, 0, 1}); e != nil {
		t.Fatal(e)
	}
	clock = clock.Add(30 * time.Second)
	pid, _ := PathID(k, f.ID, "new")
	ticket, e := c.Reserve(ctx, f.ID, UploadRequest{pid, 0, 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, bytes.NewReader([]byte{1})); e != nil {
		t.Fatal(e)
	}
	clock = clock.Add(31 * time.Second)
	maximum.Store(1)
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: "new", BlobID: ticket.BlobID, Mode: 0600})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Commit(ctx, f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != nil {
		t.Fatal("expired reservation blocked commit", e)
	}
}
