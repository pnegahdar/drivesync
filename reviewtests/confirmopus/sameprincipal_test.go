package confirmreview

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

// Tickets bind a principal, not a replica. Reserve's pre-transaction retires
// every ticket of the same principal on that path, so a second replica of the
// same user silently cancels the first replica's in-flight upload instead of
// getting ErrBusy (which a different principal gets).
func TestSamePrincipalReserveCancelsSiblingUpload(t *testing.T) {
	for _, same := range []bool{false, true} {
		s, _ := newServer(t)
		alice := ds.Principal{Tenant: "t", Subject: "alice"}
		owner := s.Client(alice)
		f, k := mkFolder(t, owner, ds.Limits{MaxTotalBytes: 1 << 30, MaxRows: 100, MaxFileBytes: 1 << 30})
		second := owner
		if !same {
			bob := ds.Principal{Tenant: "t", Subject: "bob"}
			if e := owner.Grant(bg, f.ID, bob, ds.Writer); e != nil {
				t.Fatal(e)
			}
			second = s.Client(bob)
		}
		pid, _ := ds.PathID(k, f.ID, "big.bin")
		const size = 1 << 20
		t1, e := owner.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: size})
		if e != nil {
			t.Fatal(e)
		}
		pr, pw := io.Pipe()
		uploadErr := make(chan error, 1)
		go func() { uploadErr <- owner.Upload(bg, f.ID, t1, pr) }()
		pw.Write(make([]byte, 64<<10)) // upload in flight
		_, e2 := second.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: size})
		go func() { pw.Write(make([]byte, size-64<<10)); pw.Close() }()
		e1 := <-uploadErr
		t.Logf("same principal=%v: second Reserve err=%v; first in-flight upload err=%v", same, e2, e1)
		if same && e1 != nil {
			t.Fatalf("a sibling replica's Reserve cancelled an in-flight upload (%v) instead of returning ErrBusy (%v)", e1, e2)
		}
		if !same && !errors.Is(e2, ds.ErrBusy) {
			t.Fatal("control: other principal should see ErrBusy", e2)
		}
	}
}

// coordinatedClient lets each replica's upload stream only after the sibling
// replica's next Reserve, modelling an upload that outlasts the sibling's next
// periodic sync (RescanInterval defaults to 30 s).
type coordinatedClient struct {
	ds.Client
	name, sibling string
	co            *coordinator
}
type coordinator struct {
	mu       sync.Mutex
	cond     *sync.Cond
	seq      int
	last     map[string]int
	released bool
}

func (c *coordinatedClient) Reserve(ctx context.Context, id string, r ds.UploadRequest) (ds.Ticket, error) {
	t, e := c.Client.Reserve(ctx, id, r)
	{
		c.co.mu.Lock()
		c.co.seq++
		c.co.last[c.name] = c.co.seq
		c.co.cond.Broadcast()
		c.co.mu.Unlock()
	}
	return t, e
}
func (c *coordinatedClient) Upload(ctx context.Context, id string, t ds.Ticket, body io.Reader) error {
	c.co.mu.Lock()
	mine := c.co.last[c.name]
	for !c.co.released && c.co.last[c.sibling] <= mine {
		c.co.cond.Wait()
	}
	c.co.mu.Unlock()
	return c.Client.Upload(ctx, id, t, body)
}

// Laptop and desktop of the same user both changed big.bin. While their
// uploads keep overlapping each other's next sync, every sync kills the other
// replica's upload: six syncs publish nothing.
func TestSamePrincipalReplicasLivelockOnLargeUpload(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "t", Subject: "alice"})
	f, k := mkFolder(t, c, ds.Limits{})
	co := &coordinator{last: map[string]int{}, released: true}
	co.cond = sync.NewCond(&co.mu)
	laptop := attach(t, &coordinatedClient{c, "laptop", "desktop", co}, f, k, "laptop")
	desktop := attach(t, &coordinatedClient{c, "desktop", "laptop", co}, f, k, "desktop")
	write(t, laptop, "big.bin", "v1")
	syncAll(t, laptop, desktop)
	co.mu.Lock()
	co.released = false
	co.mu.Unlock()
	before, _ := c.GetFolder(bg, f.ID)
	os.WriteFile(filepath.Join(laptop.dir, "big.bin"), []byte("laptop edit"), 0600)
	os.WriteFile(filepath.Join(desktop.dir, "big.bin"), []byte("desktop edit"), 0600)
	var wg sync.WaitGroup
	firstDone := make(chan struct{}, 2)
	for _, r := range []rep{laptop, desktop} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				_ = r.Sync(bg)
			}
			firstDone <- struct{}{}
		}()
	}
	<-firstDone
	during, _ := c.GetFolder(bg, f.ID)
	deadline := time.Now().Add(2 * time.Second)
	for during.Version == before.Version && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		during, _ = c.GetFolder(bg, f.ID)
	}
	co.mu.Lock()
	co.released = true
	co.cond.Broadcast()
	co.mu.Unlock()
	wg.Wait()
	if during.Version == before.Version {
		t.Fatalf("overlapping syncs of two same-principal replicas published neither edit; laptop errors=%v", laptop.Status().Errors)
	}
}
