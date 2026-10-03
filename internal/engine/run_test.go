package engine

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestRunRecoversAndCollects(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	pid, _ := PathID(k, f.ID, "interrupted")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Blobs.Put(context.Background(), f.ID, ticket.BlobID, strings.NewReader("x")); e != nil {
		t.Fatal(e)
	}
	if e = m.Transaction(context.Background(), func(v *Metadata) error {
		t := v.Tickets[ticket.ID]
		t.Writing = true
		v.Tickets[ticket.ID] = t
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, time.Millisecond) }()
	deadline := time.Now().Add(time.Second)
	for {
		got, e := c.GetFolder(context.Background(), f.ID)
		if e != nil {
			t.Fatal(e)
		}
		if got.Usage.Reserved == 0 && got.Usage.GarbageRows == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(got.Usage)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e = s.Blobs.Size(context.Background(), f.ID, ticket.BlobID); e == nil {
		t.Fatal("orphan retained")
	}
}
func TestRunDoesNotRetireLivePublications(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	pid, _ := PathID(k, f.ID, "flowing")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	reader, writer := io.Pipe()
	upload := make(chan error, 1)
	go func() { upload <- c.Upload(context.Background(), f.ID, ticket, reader) }()
	if _, e = writer.Write([]byte("x")); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if e = s.Run(ctx, time.Millisecond); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	if _, e = writer.Write([]byte("y")); e != nil {
		t.Fatal(e)
	}
	writer.Close()
	if e = <-upload; e != nil {
		t.Fatal("recovery cancelled active publication", e)
	}
}
func TestRunAdmissionSharedAcrossAuthorities(t *testing.T) {
	s, m := testServer(t)
	other := NewServer(m, s.Blobs)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, time.Second) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.wakes.mu.Lock()
		running := s.wakes.running
		s.wakes.mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not running")
		}
		time.Sleep(time.Millisecond)
	}
	if e := other.Run(context.Background(), time.Second); e != ErrBusy {
		t.Fatal(e)
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
