package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiAuthority runs two authority processes against one Postgres schema.
// SQLite stays one process per file; TestConcurrentAuthorities covers that race.
func TestMultiAuthority(t *testing.T) {
	if !postgresMode() {
		t.Skip("multi-authority Postgres; SQLite is one process per file")
	}
	t.Run("quota", testMultiQuota)
	t.Run("grant", testMultiGrant)
	t.Run("wait", testMultiWait)
	t.Run("gc", testMultiGC)
	t.Run("recovery", testMultiRecovery)
	t.Run("isolation", testMultiIsolation)
}

func multiPair(t *testing.T) (*Server, *Server) {
	t.Helper()
	a, b := openMetaPair(t)
	blobs := NewMemoryBlobStore()
	sa, sb := NewServer(a, blobs), NewServer(b, blobs)
	return sa, sb
}

func testMultiQuota(t *testing.T) {
	const allow int64 = 7
	const racers = 50
	cases := []struct {
		name   string
		limits Limits
		quota  Quota
	}{
		{name: "folder bytes", limits: Limits{MaxTotalBytes: allow * (RowCost + 1), MaxFiles: 1000, MaxRows: 1000, MaxFileBytes: 1 << 20}, quota: Quota{MaxTotalBytes: 1 << 40, MaxFiles: 1 << 20}},
		{name: "folder files", limits: Limits{MaxFiles: allow, MaxTotalBytes: 1 << 40, MaxRows: 1000, MaxFileBytes: 1 << 20}, quota: Quota{MaxTotalBytes: 1 << 40, MaxFiles: 1 << 20}},
		{name: "owner bytes", limits: Limits{MaxTotalBytes: 1 << 40, MaxFiles: 1000, MaxRows: 1000, MaxFileBytes: 1 << 20}, quota: Quota{MaxTotalBytes: allow * (RowCost + 1), MaxFiles: 1 << 20}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sa, sb := multiPair(t)
			policy := QuotaFunc(func(context.Context, Principal) (Quota, error) { return tc.quota, nil })
			sa.Quotas, sb.Quotas = policy, policy
			f, _ := folderFor(t, sa.Client(owner), tc.limits)
			var accepted atomic.Int32
			var wg sync.WaitGroup
			var mu sync.Mutex
			var bad []error
			for i := 0; i < racers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					server := sa
					if i%2 == 1 {
						server = sb
					}
					_, e := server.Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: fmt.Sprintf("%064x", i), SealedSize: 1})
					if e == nil {
						accepted.Add(1)
						return
					}
					var l *LimitError
					if errors.As(e, &l) || errors.Is(e, ErrQuota) {
						return
					}
					mu.Lock()
					bad = append(bad, e)
					mu.Unlock()
				}(i)
			}
			wg.Wait()
			if len(bad) > 0 {
				t.Fatal(bad)
			}
			if int64(accepted.Load()) != allow {
				t.Fatalf("accepted %d, limit %d", accepted.Load(), allow)
			}
			for _, s := range []*Server{sa, sb} {
				got, e := s.GetFolder(context.Background(), owner, f.ID)
				if e != nil {
					t.Fatal(e)
				}
				if got.Usage.Reserved != allow*(RowCost+1) || got.Usage.ReservedFiles != allow {
					t.Fatalf("usage %+v after %d reservations", got.Usage, allow)
				}
			}
		})
	}
}

type gateReader struct {
	buf     []byte
	off     int
	started chan struct{}
	goOn    chan struct{}
	once    sync.Once
}

func (g *gateReader) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.started) })
	<-g.goOn
	if g.off >= len(g.buf) {
		return 0, io.EOF
	}
	n := copy(p, g.buf[g.off:])
	g.off += n
	return n, nil
}

func sealedBody(t *testing.T, k FolderKey, f Folder, pathID, blob string, data []byte) []byte {
	t.Helper()
	var sealed bytes.Buffer
	if e := SealContent(&sealed, bytes.NewReader(data), k, f.ID, blob, pathID); e != nil {
		t.Fatal(e)
	}
	return sealed.Bytes()
}

func holdUpload(t *testing.T, s *Server, p Principal, f Folder, k FolderKey, name string, data []byte) (Ticket, chan struct{}, <-chan error) {
	t.Helper()
	pid, e := PathID(k, f.ID, name)
	if e != nil {
		t.Fatal(e)
	}
	ticket, e := s.Reserve(context.Background(), p, f.ID, UploadRequest{PathID: pid, SealedSize: SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	body := sealedBody(t, k, f, pid, ticket.BlobID, data)
	if int64(len(body)) != ticket.SealedSize {
		t.Fatalf("sealed %d ticket %d", len(body), ticket.SealedSize)
	}
	started, goOn := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.Upload(context.Background(), p, f.ID, ticket, &gateReader{buf: body, started: started, goOn: goOn})
	}()
	select {
	case <-started:
	case e := <-done:
		t.Fatal(e)
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	return ticket, goOn, done
}

func testMultiGrant(t *testing.T) {
	sa, sb := multiPair(t)
	writer := Principal{Tenant: "tenant", Subject: "writer"}
	c := sa.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	if e := c.Grant(context.Background(), f.ID, writer, Writer); e != nil {
		t.Fatal(e)
	}
	if _, e := sb.GetFolder(context.Background(), writer, f.ID); e != nil {
		t.Fatalf("grant on A was not visible on B: %v", e)
	}
	_, goOn, done := holdUpload(t, sb, writer, f, k, "live", []byte("x"))
	if e := c.Revoke(context.Background(), f.ID, writer); e != nil {
		t.Fatal(e)
	}
	if _, e := sb.GetFolder(context.Background(), writer, f.ID); e != ErrDenied {
		t.Fatalf("revoke on A was not visible on B: %v", e)
	}
	close(goOn)
	if e := <-done; e != ErrDenied {
		t.Fatalf("in-flight upload after revoke: %v", e)
	}
}

func testMultiWait(t *testing.T) {
	sa, sb := multiPair(t)
	f, k := folderFor(t, sa.Client(owner), Limits{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	woke := make(chan error, 1)
	go func() {
		_, e := sa.Wait(ctx, owner, f.ID, 0)
		woke <- e
	}()
	// The listener connects after Open returns. Wait is blocked before the
	// commit, and that commit has to wake it within a second.
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	put(t, sb.Client(owner), f, k, "n", 0, []byte("x"))
	select {
	case e := <-woke:
		if e != nil {
			t.Fatal(e)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("wait woke after %s", time.Since(start))
		}
	case <-time.After(time.Second):
		t.Fatal("wait on A did not wake from a commit on B within 1s")
	}
}

func gcBoth(t *testing.T, sa, sb *Server) {
	t.Helper()
	var wg sync.WaitGroup
	for _, s := range []*Server{sa, sb} {
		wg.Add(1)
		go func(s *Server) {
			defer wg.Done()
			if e := s.CollectGarbage(context.Background()); e != nil {
				t.Error(e)
			}
		}(s)
	}
	wg.Wait()
}

func testMultiGC(t *testing.T) {
	sa, sb := multiPair(t)
	c := sa.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	live := put(t, c, f, k, "live", 0, []byte("keep"))
	before, e := sa.GetFolder(context.Background(), owner, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	doomed := put(t, c, f, k, "doomed", 0, []byte("drop"))
	charged, e := sa.GetFolder(context.Background(), owner, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	ticket, goOn, uploaded := holdUpload(t, sb, owner, f, k, "flight", []byte("z"))
	gcBoth(t, sa, sb)
	got, e := sa.Download(context.Background(), owner, f.ID, live.BlobID)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := io.ReadAll(got)
	got.Close()
	if e != nil || int64(len(raw)) != live.SealedSize {
		t.Fatalf("live blob %d bytes, want %d: %v", len(raw), live.SealedSize, e)
	}
	var writing bool
	if e = sa.Meta.Transaction(context.Background(), func(m *Metadata) error {
		tk, ok := m.Tickets[ticket.ID]
		writing = ok && tk.Writing && tk.Lease.After(time.Now())
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if !writing {
		t.Fatal("gc retired a leased upload")
	}
	if _, e = c.Commit(context.Background(), f.ID, []Mutation{{PathID: doomed.PathID, BaseVersion: doomed.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	var usage Usage
	for i := 0; i < 20; i++ {
		gcBoth(t, sa, sb)
		folder, e := sa.GetFolder(context.Background(), owner, f.ID)
		if e != nil {
			t.Fatal(e)
		}
		usage = folder.Usage
		if usage.GarbageRows == 0 && usage.Bytes < charged.Usage.Bytes {
			break
		}
	}
	// The tombstone row stays. The blob charge is released once, and the live
	// file's bytes stay. A second pass must not release that charge again.
	if usage.GarbageRows != 0 || usage.Bytes >= charged.Usage.Bytes || usage.Bytes <= before.Usage.Bytes {
		t.Fatalf("after gc usage %+v, live %d charged %d", usage, before.Usage.Bytes, charged.Usage.Bytes)
	}
	gcBoth(t, sa, sb)
	again, e := sb.GetFolder(context.Background(), owner, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	if again.Usage.Bytes != usage.Bytes || again.Usage.GarbageRows != 0 {
		t.Fatalf("second gc changed usage from %+v to %+v", usage, again.Usage)
	}
	onA, e := sa.GetFolder(context.Background(), owner, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	if onA.Usage != again.Usage {
		t.Fatalf("servers disagree: A %+v B %+v", onA.Usage, again.Usage)
	}
	if _, e = sa.Blobs.Size(context.Background(), f.ID, doomed.BlobID); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("doomed blob: %v", e)
	}
	got, e = sb.Download(context.Background(), owner, f.ID, live.BlobID)
	if e != nil {
		t.Fatal(e)
	}
	raw, e = io.ReadAll(got)
	got.Close()
	if e != nil || int64(len(raw)) != live.SealedSize {
		t.Fatalf("live blob after gc %d: %v", len(raw), e)
	}
	close(goOn)
	if e = <-uploaded; e != nil {
		t.Fatal(e)
	}
}

func testMultiRecovery(t *testing.T) {
	sa, sb := multiPair(t)
	c := sa.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	stale, e := sa.Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: fmt.Sprintf("%064x", 1), SealedSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = sa.Meta.Transaction(context.Background(), func(m *Metadata) error {
		tk, ok := m.Tickets[stale.ID]
		if !ok {
			return errors.New("missing stale ticket")
		}
		tk.Writing = true
		tk.Writer = "expired-process"
		tk.Lease = time.Now().Add(-time.Minute)
		m.Tickets[stale.ID] = tk
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	live, goOn, uploaded := holdUpload(t, sa, owner, f, k, "live", []byte("keep"))
	if e = sb.RecoverUploads(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = sb.Meta.Transaction(context.Background(), func(m *Metadata) error {
		if _, ok := m.Tickets[stale.ID]; ok {
			return errors.New("expired lease was not retired")
		}
		tk, ok := m.Tickets[live.ID]
		if !ok || !tk.Writing {
			return errors.New("live upload was retired")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	close(goOn)
	if e = <-uploaded; e != nil {
		t.Fatal(e)
	}
	pid, e := PathID(k, f.ID, "live")
	if e != nil {
		t.Fatal(e)
	}
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: "live", BlobID: live.BlobID, Size: 4, Mode: 0600, Hash: hashBytes([]byte("keep"))})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, BaseVersion: 0, TicketID: live.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
}

func testMultiIsolation(t *testing.T) {
	sa, sb := multiPair(t)
	f, _ := folderFor(t, sa.Client(owner), Limits{})
	other := Principal{Tenant: "other", Subject: "owner"}
	_, denied := sb.GetFolder(context.Background(), other, f.ID)
	_, missing := sa.GetFolder(context.Background(), owner, randomID())
	if denied != ErrDenied || missing != ErrDenied || denied != missing {
		t.Fatalf("denied %v missing %v", denied, missing)
	}
	list, e := sb.ListFolders(context.Background(), other)
	if e != nil {
		t.Fatal(e)
	}
	for _, folder := range list {
		if folder.ID == f.ID {
			t.Fatal("other tenant listed a foreign folder")
		}
	}
	list, e = sb.ListFolders(context.Background(), owner)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, folder := range list {
		if folder.ID == f.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("owner list on B missed the folder created on A")
	}
}
