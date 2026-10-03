package review_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	ds "github.com/pnegahdar/drivesync"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func transport(t *testing.T, s *ds.Server, p ds.Principal, overHTTP bool) ds.Client {
	t.Helper()
	if !overHTTP {
		return s.Client(p)
	}
	h := httptest.NewServer(s.Handler(func(*http.Request) (ds.Principal, error) { return p, nil }))
	t.Cleanup(h.Close)
	return ds.NewHTTPClient(h.URL, nil)
}
func TestReviewRevokeAfterPlanDecrease(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := setup(t)
			c := transport(t, s, owner, httpMode)
			var budget atomic.Int64
			budget.Store(10000)
			s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
				return ds.Quota{MaxTotalBytes: budget.Load()}, nil
			})
			k := ds.NewFolderKey()
			f := folder(t, c, "shared", k)
			if e := c.SetLimits(ctx, f.ID, ds.Limits{MaxTotalBytes: 5000, MaxFileBytes: 5000, MaxRows: 1000}); e != nil {
				t.Fatal(e)
			}
			a := ds.Principal{Tenant: "outside", Subject: "a"}
			b := ds.Principal{Tenant: "outside", Subject: "b"}
			for _, p := range []ds.Principal{a, b} {
				if e := c.Grant(ctx, f.ID, p, ds.Writer); e != nil {
					t.Fatal(e)
				}
			}
			budget.Store(4000)
			revoke := c.Revoke(ctx, f.ID, a)
			_, still := transport(t, s, a, httpMode).GetFolder(ctx, f.ID)
			downgrade := c.Grant(ctx, f.ID, b, ds.Reader)
			t.Logf("revoke=%v; revoked principal access=%v; downgrade=%v", revoke, still, downgrade)
			if revoke != nil || !errors.Is(still, ds.ErrDenied) || downgrade != nil {
				t.Fatal("grandfathered allocation blocks access revocation/downgrade")
			}
		})
	}
}
func TestReviewRenameCreditReusable(t *testing.T) {
	s, c := setup(t)
	k := ds.NewFolderKey()
	f := folder(t, c, "private", k)
	old := put(t, c, f, k, "old", 0, strings.Repeat("x", 1000))
	got, e := c.GetFolder(ctx, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	budget := got.Usage.Bytes
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) { return ds.Quota{MaxTotalBytes: budget}, nil })
	if e = c.SetLimits(ctx, f.ID, ds.Limits{MaxTotalBytes: budget, MaxFileBytes: budget, MaxRows: 20}); e != nil {
		t.Fatal(e)
	}
	physical := old.SealedSize
	var ids []string
	for n := 0; n < 8; n++ {
		pid, _ := ds.PathID(k, f.ID, fmt.Sprint("new", n))
		size := old.SealedSize - ds.RowCost
		ticket, e := c.Reserve(ctx, f.ID, renameRequest(pid, size, old.PathID, old.Version))
		if e != nil {
			t.Logf("blocked at %d: %v", n, e)
			break
		}
		if e = c.Upload(ctx, f.ID, ticket, bytes.NewReader(make([]byte, size))); e != nil {
			t.Fatal(e)
		}
		physical += size
		ids = append(ids, ticket.ID)
	}
	got, e = c.GetFolder(ctx, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("budget=%d, staged tickets=%d, actual blob bytes=%d, reported usage=%+v", budget, len(ids), physical, got.Usage)
	if len(ids) > 1 {
		t.Fatal("same CAS delete credit backed multiple simultaneous uploads beyond quota")
	}
}

type flakyDeletion struct {
	ds.BlobStore
	fail atomic.Bool
}

func (b *flakyDeletion) Delete(c context.Context, f, id string) error {
	if b.fail.Load() {
		return errors.New("transient delete failure")
	}
	return b.BlobStore.Delete(c, f, id)
}
func TestReviewSharedGarbageExceedsAllocation(t *testing.T) {
	s, c := setup(t)
	b := &flakyDeletion{BlobStore: s.Blobs}
	s.Blobs = b
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) { return ds.Quota{MaxTotalBytes: 2300}, nil })
	k := ds.NewFolderKey()
	shared := folder(t, c, "shared", k)
	private := folder(t, c, "private", k)
	if e := c.SetLimits(ctx, shared.ID, ds.Limits{MaxTotalBytes: 2000, MaxFileBytes: 2000, MaxRows: 1000}); e != nil {
		t.Fatal(e)
	}
	p := ds.Principal{Tenant: "outside", Subject: "writer"}
	if e := c.Grant(ctx, shared.ID, p, ds.Writer); e != nil {
		t.Fatal(e)
	}
	guest := s.Client(p)
	probe := func() error {
		pid, _ := ds.PathID(k, private.ID, "new")
		ticket, e := c.Reserve(ctx, private.ID, ds.UploadRequest{PathID: pid})
		if e == nil {
			e = c.CancelUpload(ctx, private.ID, ticket.ID)
		}
		return e
	}
	before := probe()
	if before != nil {
		t.Fatal(before)
	}
	row := put(t, guest, shared, k, "file", 0, strings.Repeat("a", 900))
	b.fail.Store(true)
	pid, _ := ds.PathID(k, shared.ID, "file")
	ticket, replaceErr := guest.Reserve(ctx, shared.ID, ds.UploadRequest{PathID: pid, BaseVersion: row.Version, SealedSize: ds.SealedSize(900)})
	if replaceErr == nil {
		var sealed bytes.Buffer
		ds.SealContent(&sealed, strings.NewReader(strings.Repeat("b", 900)), k, shared.ID, ticket.BlobID, pid)
		if e := guest.Upload(ctx, shared.ID, ticket, &sealed); e != nil {
			t.Fatal(e)
		}
		meta, _ := ds.SealMetadata(k, shared.ID, pid, ds.FileMetadata{Path: "file", BlobID: ticket.BlobID, Mode: 0600, Size: 900})
		_, replaceErr = guest.Commit(ctx, shared.ID, []ds.Mutation{{PathID: pid, BaseVersion: row.Version, TicketID: ticket.ID, Metadata: meta}})
	}
	t.Logf("replacement: %v", replaceErr)
	got, e := c.GetFolder(ctx, shared.ID)
	if e != nil {
		t.Fatal(e)
	}
	after := probe()
	t.Logf("cap=%d, shared charged bytes=%d, primary owner's private reservation before=%v after=%v", got.Limits.MaxTotalBytes, got.Usage.Bytes, before, after)
	if got.Usage.Bytes+got.Usage.Reserved > 2000 || after != nil {
		t.Fatal("cross-tenant writer exceeded its preallocation and blocked private owner writes")
	}
}

type pausedPublication struct {
	ds.BlobStore
	readDone, release chan struct{}
	fail              atomic.Bool
}

func (b *pausedPublication) Put(c context.Context, f, id string, r io.Reader) (int64, error) {
	data, e := io.ReadAll(r)
	if e != nil {
		return 0, e
	}
	close(b.readDone)
	select {
	case <-b.release:
	case <-c.Done():
		return 0, c.Err()
	}
	return b.BlobStore.Put(c, f, id, bytes.NewReader(data))
}
func (b *pausedPublication) Delete(c context.Context, f, id string) error {
	if b.fail.Load() {
		return errors.New("transient delete failure")
	}
	return b.BlobStore.Delete(c, f, id)
}
func TestReviewGCBeforePublicationDropsCharge(t *testing.T) {
	s, c := setup(t)
	b := &pausedPublication{BlobStore: s.Blobs, readDone: make(chan struct{}), release: make(chan struct{})}
	s.Blobs = b
	k := ds.NewFolderKey()
	f := folder(t, c, "private", k)
	pid, _ := ds.PathID(k, f.ID, "file")
	ticket, e := c.Reserve(ctx, f.ID, ds.UploadRequest{PathID: pid, SealedSize: 1000})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- c.Upload(ctx, f.ID, ticket, strings.NewReader(strings.Repeat("a", 1000))) }()
	select {
	case <-b.readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not reach publication pause")
	}
	if e = c.CancelUpload(ctx, f.ID, ticket.ID); e != nil {
		t.Fatal(e)
	}
	b.fail.Store(true)
	close(b.release)
	e = <-done
	if !errors.Is(e, ds.ErrDenied) {
		t.Fatal(e)
	}
	if e = s.CollectGarbage(ctx); e != nil {
		t.Log("GC error", e)
	}
	b.fail.Store(false)
	if e = s.CollectGarbage(ctx); e != nil {
		t.Fatal(e)
	}
	got, e := c.GetFolder(ctx, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	size, e := b.BlobStore.Size(ctx, f.ID, ticket.BlobID)
	t.Logf("actual bytes=%d, size error=%v, reported usage=%+v", size, e, got.Usage)
	if e == nil && size > 0 && got.Usage.GarbageRows == 0 {
		t.Fatal("published after cancellation GC; failed cleanup left permanent uncharged orphan")
	}
}
func TestReviewNULPrincipalsStayScoped(t *testing.T) {
	s, _ := setup(t)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) { return ds.Quota{MaxFolders: 1}, nil })
	p := ds.Principal{Tenant: "tenant\x00suffix", Subject: "subject\x00suffix"}
	c := s.Client(p)
	k := ds.NewFolderKey()
	f := folder(t, c, "first", k)
	fs, e := c.ListFolders(ctx)
	if e != nil || len(fs) != 1 || fs[0].ID != f.ID {
		t.Fatal("opaque principal scope mismatch", fs, e)
	}
	if _, e = c.CreateFolder(ctx, ds.FolderSpec{Name: "second", KeyCheck: ds.KeyCheck(k)}); e == nil {
		t.Fatal("NUL identity bypassed folder quota")
	}
}

var _ = os.ErrNotExist

func TestReviewSharedReservedRowsExceedAllocation(t *testing.T) {
	s, c := setup(t)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 10000, MaxFiles: 8}, nil
	})
	k := ds.NewFolderKey()
	f := folder(t, c, "shared", k)
	private := folder(t, c, "private", k)
	if e := c.SetLimits(ctx, f.ID, ds.Limits{MaxTotalBytes: 2000, MaxFileBytes: 2000, MaxRows: 100}); e != nil {
		t.Fatal(e)
	}
	p := ds.Principal{Tenant: "foreign", Subject: "writer"}
	if e := c.Grant(ctx, f.ID, p, ds.Writer); e != nil {
		t.Fatal(e)
	}
	guest := s.Client(p)
	old := put(t, guest, f, k, "old", 0, strings.Repeat("a", 1000))
	probe := func() error {
		ticket, e := c.Reserve(ctx, private.ID, ds.UploadRequest{PathID: strings.Repeat("f", 64)})
		if e == nil {
			e = c.CancelUpload(ctx, private.ID, ticket.ID)
		}
		return e
	}
	before := probe()
	if before != nil {
		t.Fatal(before)
	}
	for i := 0; i < 8; i++ {
		pid, _ := ds.PathID(k, f.ID, fmt.Sprint("new", i))
		_, e := guest.Reserve(ctx, f.ID, renameRequest(pid, old.SealedSize-ds.RowCost, old.PathID, old.Version))
		if e != nil {
			break
		}
	}
	after := probe()
	got, e := c.GetFolder(ctx, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("shared row allocation=7, usage=%+v, private reservation before=%v after=%v", got.Usage, before, after)
	if got.Usage.Rows+got.Usage.GarbageRows+got.Usage.ReservedRows > 7 || after != nil {
		t.Fatal("foreign writer exceeded row allocation using reservations alone and blocked owner")
	}
}
func TestReviewFIFOQuarantine(t *testing.T) {
	_, c := setup(t)
	k := ds.NewFolderKey()
	f := folder(t, c, "files", k)
	r, dir := newReplica(t, c, f, k)
	fifo := filepath.Join(dir, "z-pipe")
	if e := syscall.Mkfifo(fifo, 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "a-good"), []byte("ordinary file"), 0600); e != nil {
		t.Fatal(e)
	}
	runctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Sync(runctx) }()
	blocked := false
	select {
	case e := <-done:
		t.Logf("returned: %v", e)
	case <-time.After(500 * time.Millisecond):
		blocked = true
	}
	// Unblock the FIFO open so the test can shut down the replica cleanly.
	if blocked {
		w, e := os.OpenFile(fifo, os.O_WRONLY, 0)
		if e != nil {
			t.Fatal(e)
		}
		w.Close()
		<-done
	}
	delta, e := c.Changes(ctx, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("blocked past context deadline=%v; remote rows=%d", blocked, len(delta.Rows))
	if blocked {
		t.Fatal("FIFO blocked entire scan and prevented unrelated upload despite deadline")
	}
}
func TestReviewRawJSONVariants(t *testing.T) {
	s, c := setup(t)
	k := ds.NewFolderKey()
	f := folder(t, c, "raw", k)
	if e := c.SetLimits(ctx, f.ID, ds.Limits{MaxTotalBytes: 5000, MaxFileBytes: 5000, MaxRows: 1000}); e != nil {
		t.Fatal(e)
	}
	h := s.Handler(func(*http.Request) (ds.Principal, error) { return owner, nil })
	cases := []struct {
		subject string
		valid   bool
	}{{`\ud800a`, false}, {`\udc00`, false}, {`\ud800\u0061`, false}, {`\ud800\ud800`, false}, {string([]byte{0xed, 0xa0, 0x80}), false}, {`\ud83d\ude00`, true}, {`\\ud800`, true}, {`\ufffd`, true}}
	for _, v := range cases {
		body := fmt.Sprintf(`{"Op":"grant","Folder":%q,"Grantee":{"Tenant":"outside","Subject":"%s"},"Role":"reader"}`, f.ID, v.subject)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", "/rpc", strings.NewReader(body)))
		if (w.Code == 200) != v.valid {
			t.Errorf("subject %q valid=%v code=%d", v.subject, v.valid, w.Code)
		}
	}
}

type advancingReader struct {
	remaining int
	offset    *atomic.Int64
}

func (r *advancingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	r.offset.Add(int64(400 * time.Millisecond))
	p[0] = 'x'
	r.remaining--
	return 1, nil
}
func TestReviewStreamingRenewsBeyondOriginalExpiry(t *testing.T) {
	s, c := setup(t)
	var offset atomic.Int64
	base := time.Now()
	s.Now = func() time.Time { return base.Add(time.Duration(offset.Load())) }
	s.ReservationTTL = time.Second
	k := ds.NewFolderKey()
	f := folder(t, c, "renewal", k)
	pid, _ := ds.PathID(k, f.ID, "file")
	ticket, e := c.Reserve(ctx, f.ID, ds.UploadRequest{PathID: pid, SealedSize: 8, MetadataBytes: 28})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, &advancingReader{remaining: 8, offset: &offset}); e != nil {
		t.Fatal(e)
	}
	if !s.Now().After(ticket.Expires) {
		t.Fatal("test did not exceed original TTL")
	}
	if _, e = c.Commit(ctx, f.ID, []ds.Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: make([]byte, 28)}}); e != nil {
		t.Fatal(e)
	}
}

// The original attack supplies a removed credit field through JSON. On the old
// API it grants credit; the current request has no such field or mechanism.
func renameRequest(pid string, size int64, old string, version uint64) ds.UploadRequest {
	b, _ := json.Marshal(struct {
		PathID     string
		SealedSize int64
		Deletes    []ds.Mutation
	}{pid, size, []ds.Mutation{{PathID: old, BaseVersion: version, Deleted: true}}})
	var r ds.UploadRequest
	_ = json.Unmarshal(b, &r)
	return r
}
