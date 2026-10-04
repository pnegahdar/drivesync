package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAllocatedSharedFolderHasNoPrivateUsageOracle(t *testing.T) {
	for _, transport := range []bool{false, true} {
		t.Run(fmt.Sprint(transport), func(t *testing.T) {
			s, _ := testServer(t)
			const capacity int64 = 8192
			s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxTotalBytes: 2*capacity + 4096}, nil })
			admin := clients(t, s, transport)(owner)
			peer := Principal{"foreign", "writer"}
			guest := clients(t, s, transport)(peer)
			uncapped, _ := folderFor(t, admin, Limits{})
			if e := admin.Grant(context.Background(), uncapped.ID, peer, Reader); e != ErrInvalid {
				t.Fatal("uncapped cross-tenant grant", e)
			}
			shared, key := folderFor(t, admin, Limits{MaxTotalBytes: capacity})
			if e := admin.Grant(context.Background(), shared.ID, peer, Writer); e != nil {
				t.Fatal(e)
			}
			private, pk := sharedFor(t, admin)
			probe := func() []string {
				var result []string
				for _, n := range []int64{1, capacity - RowCost, capacity - RowCost + 1, capacity, MaxRequestBytes + 1} {
					pid, _ := PathID(key, shared.ID, fmt.Sprint(n))
					ticket, e := guest.Reserve(context.Background(), shared.ID, UploadRequest{PathID: pid, SealedSize: n})
					result = append(result, fmt.Sprint(e))
					if e == nil {
						if e = guest.CancelUpload(context.Background(), shared.ID, ticket.ID); e != nil {
							t.Fatal(e)
						}
					}
				}
				return result
			}
			before := probe()
			put(t, admin, private, pk, "private", 0, bytes.Repeat([]byte("s"), 3000))
			after := probe()
			if fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatalf("oracle: %v -> %v", before, after)
			}
			list, e := guest.GetFolder(context.Background(), shared.ID)
			if e != nil || list.Limits.MaxTotalBytes != capacity {
				t.Fatal(list, e)
			}
			if e = admin.SetLimits(context.Background(), shared.ID, Limits{MaxTotalBytes: 3 * capacity}); e == nil {
				t.Fatal("raising allocation ignored private usage")
			}
			if e = admin.SetLimits(context.Background(), shared.ID, Limits{}); e != ErrInvalid {
				t.Fatal("removed shared capacity cap", e)
			}
		})
	}
}
func TestOnlyPrimaryOwnerSeesOwnerQuotaNumbers(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxTotalBytes: 300}, nil })
			cc := clients(t, s, httpMode)
			admin := cc(owner)
			f, k := folderFor(t, admin, Limits{MaxTotalBytes: 300, MaxRows: 1})
			for _, role := range []Role{Writer, Owner} {
				p := Principal{owner.Tenant, "delegate-" + string(role)}
				if e := admin.Grant(context.Background(), f.ID, p, role); e != nil {
					t.Fatal(e)
				}
				pid, _ := PathID(k, f.ID, string(role))
				_, e := cc(p).Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 301})
				var le *LimitError
				if !errors.As(e, &le) || le.Maximum != 300 {
					t.Fatalf("%s quota leak: %v", role, e)
				}
				v, e := cc(p).GetFolder(context.Background(), f.ID)
				if e != nil || v.Limits.MaxTotalBytes != 300 {
					t.Fatal(v, e)
				}
			}
			private, pk := folderFor(t, admin, Limits{})
			pid, _ := PathID(pk, private.ID, "primary")
			_, e := admin.Reserve(context.Background(), private.ID, UploadRequest{PathID: pid, SealedSize: 301})
			var le *LimitError
			if !errors.As(e, &le) || le.Maximum != 300 {
				t.Fatal(e)
			}
		})
	}
}
func TestRowBudgetIncludesTombstonesAndOwnerFolders(t *testing.T) {
	s, _ := testServer(t)
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxFiles: 2}, nil })
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{MaxRows: 2})
	g, gk := folderFor(t, c, Limits{})
	for _, name := range []string{"one", "two"} {
		pid, _ := PathID(k, f.ID, name)
		if _, e := c.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, Deleted: true}}); e != nil {
			t.Fatal(e)
		}
	}
	v, e := c.GetFolder(context.Background(), f.ID)
	if e != nil || v.Usage.Rows != 2 || v.Usage.Bytes != 2*RowCost || v.Usage.Files != 0 {
		t.Fatal(v, e)
	}
	pid, _ := PathID(gk, g.ID, "third")
	if _, e = c.Reserve(context.Background(), g.ID, UploadRequest{PathID: pid}); e == nil {
		t.Fatal("owner row budget bypass")
	}
	pid, _ = PathID(k, f.ID, "third")
	if _, e = c.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, Deleted: true}}); e == nil {
		t.Fatal("tombstone row budget bypass")
	}
}
func TestGrantAndPrincipalBounds(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, _ := sharedFor(t, c)
	for i := 0; i < MaxGrants; i++ {
		if e := c.Grant(context.Background(), f.ID, Principal{owner.Tenant, fmt.Sprint(i)}, Reader); e != nil {
			t.Fatal(e)
		}
	}
	if e := c.Grant(context.Background(), f.ID, Principal{owner.Tenant, "overflow"}, Reader); e != ErrInvalid {
		t.Fatal(e)
	}
	if e := c.Grant(context.Background(), f.ID, Principal{owner.Tenant, strings.Repeat("a", 257)}, Reader); e != ErrInvalid {
		t.Fatal(e)
	}
	if sat(1<<62, 1<<62) != (1<<63)-1 {
		t.Fatal("overflow")
	}
}
func TestUnrelatedTenantRowsAreNotDecoded(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	f, _ := folderFor(t, c, Limits{})
	evil := Principal{"evil", "owner"}
	g, _ := folderFor(t, s.Client(evil), Limits{})
	data := fmt.Sprintf(`{"FolderID":%q,"PathID":%q,"SealedSize":"bad"}`, g.ID, strings.Repeat("a", 64))
	insertFileRow(t, m, g.ID, strings.Repeat("a", 64), data)
	if _, e := c.GetFolder(context.Background(), f.ID); e != nil {
		t.Fatal("unrelated row decoded", e)
	}
	if _, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: strings.Repeat("b", 64)}); e != nil {
		t.Fatal("unrelated row decoded", e)
	}
}

type failingDelete struct {
	BlobStore
	fail bool
}

func (b *failingDelete) Delete(ctx context.Context, f, id string) error {
	if b.fail {
		return errors.New("storage unavailable")
	}
	return b.BlobStore.Delete(ctx, f, id)
}
func TestGarbageRemainsChargedUntilCollected(t *testing.T) {
	s, _ := testServer(t)
	b := &failingDelete{BlobStore: s.Blobs}
	s.Blobs = b
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	old := put(t, c, f, k, "file", 0, []byte("old bytes"))
	b.fail = true
	newer := put(t, c, f, k, "file", old.Version, []byte("new bytes"))
	got, e := c.GetFolder(context.Background(), f.ID)
	if e != nil || got.Usage.Bytes != rowBytes(newer)+old.SealedSize+RowCost {
		t.Fatal(got, e)
	}
	if _, e = c.Download(context.Background(), f.ID, old.BlobID); e != ErrDenied {
		t.Fatal("retired blob downloadable", e)
	}
	if e = s.CollectGarbage(context.Background()); e == nil {
		t.Fatal("GC failure hidden")
	}
	b.fail = false
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	got, _ = c.GetFolder(context.Background(), f.ID)
	if got.Usage.Bytes != rowBytes(newer) {
		t.Fatal(got.Usage)
	}
	if _, e = s.Blobs.Size(context.Background(), f.ID, old.BlobID); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
	if e = c.DeleteFolder(context.Background(), f.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Blobs.Size(context.Background(), f.ID, newer.BlobID); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("folder deletion orphan", e)
	}
}
func TestUploadedReservationExpiryCollectsBlob(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	pid, _ := PathID(k, f.ID, "x")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 3})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(context.Background(), f.ID, ticket, strings.NewReader("abc")); e != nil {
		t.Fatal(e)
	}
	s.Now = func() time.Time { return ticket.Expires.Add(time.Second) }
	if e = c.Upload(context.Background(), f.ID, ticket, strings.NewReader("abc")); e != ErrExpired {
		t.Fatal("expired reported as authorization", e)
	}
	if _, e = c.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: make([]byte, 32)}}); e != ErrExpired {
		t.Fatal("expired commit reported as authorization", e)
	}
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Blobs.Size(context.Background(), f.ID, ticket.BlobID); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("expired orphan", e)
	}
}
func TestBusyAndOwnReservationReplacement(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := sharedFor(t, c)
	p := Principal{owner.Tenant, "other"}
	_ = c.Grant(context.Background(), f.ID, p, Writer)
	pid, _ := PathID(k, f.ID, "x")
	req := UploadRequest{SessionID: randomID(), PathID: pid, SealedSize: 1}
	first, e := c.Reserve(context.Background(), f.ID, req)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Client(p).Reserve(context.Background(), f.ID, req); e != ErrBusy {
		t.Fatal(e)
	}
	if e = c.Upload(context.Background(), f.ID, first, strings.NewReader("x")); e != nil {
		t.Fatal(e)
	}
	second, e := c.Reserve(context.Background(), f.ID, req)
	if e != nil || second.ID == first.ID {
		t.Fatal(second, e)
	}
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Blobs.Size(context.Background(), f.ID, first.BlobID); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("stale ticket orphan", e)
	}
}
func TestHTTPChangesPagination(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, _ := folderFor(t, c, Limits{})
	for start := 0; start < 1300; start += 256 {
		mut := []Mutation{}
		for i := start; i < min(start+256, 1300); i++ {
			mut = append(mut, Mutation{PathID: fmt.Sprintf("%064x", i), Deleted: true})
		}
		if _, e := c.Commit(context.Background(), f.ID, mut); e != nil {
			t.Fatal(e)
		}
	}
	h := httptest.NewServer(s.Handler(func(*http.Request) (Principal, error) { return owner, nil }))
	defer h.Close()
	remote := NewHTTPClient(h.URL, nil)
	remote.MaxResponseBytes = 200000
	got, e := remote.Changes(context.Background(), f.ID, 0)
	if e != nil || len(got.Rows) != 1300 {
		t.Fatalf("pagination: %d %v", len(got.Rows), e)
	}
	page, e := s.ChangesPage(context.Background(), owner, f.ID, 0, 0, "")
	if e != nil || len(page.Rows) != 512 || page.Next == "" {
		t.Fatal(page.Next, len(page.Rows), e)
	}
	if _, e = s.ChangesPage(context.Background(), Principal{}, f.ID, 0, 0, page.Next); e != ErrDenied {
		t.Fatal(e)
	}
}

type zeroBeforeEOF struct {
	*bytes.Reader
	zeros int
}

func (r *zeroBeforeEOF) Read(p []byte) (int, error) {
	if r.Len() == 0 && r.zeros > 0 {
		r.zeros--
		return 0, nil
	}
	return r.Reader.Read(p)
}
func TestOpenContentAllowsEmptyReadsBeforeEOF(t *testing.T) {
	k := NewFolderKey()
	f, b, p := randomID(), randomID(), strings.Repeat("a", 64)
	var encrypted bytes.Buffer
	if e := SealContent(&encrypted, strings.NewReader("data"), k, f, b, p); e != nil {
		t.Fatal(e)
	}
	r := &zeroBeforeEOF{bytes.NewReader(encrypted.Bytes()), 3}
	if e := OpenContent(io.Discard, r, k, f, b, p); e != nil {
		t.Fatal(e)
	}
	r = &zeroBeforeEOF{bytes.NewReader(encrypted.Bytes()), 1000}
	if e := OpenContent(io.Discard, r, k, f, b, p); e != io.ErrNoProgress {
		t.Fatal(e)
	}
}
func TestCanonicalIgnoreAndCollisions(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := replicaFor(t, c, f, k, "unicode", true)
	if !r.ignore("CAFE\u0301/hooks/run", false, []string{"caf\u00e9/"}) {
		t.Fatal("canonical/case ignore bypass")
	}
	for _, pair := range [][2]string{{"e\u0301", "\u00e9"}, {"\u1100\u1161\u11a8", "\uac01"}, {"A\u030a", "\u00c5"}} {
		if nfc(pair[0]) != pair[1] {
			t.Fatal(pair)
		}
	}
}
func TestNewDirectoryFsyncFailurePreventsAcknowledgement(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	put(t, c, f, k, "nested/child/file", 0, []byte("content"))
	r := replicaFor(t, c, f, k, "durability", true)
	original := syncDirectoryFile
	defer func() { syncDirectoryFile = original }()
	sentinel := errors.New("directory fsync failed")
	syncDirectoryFile = func(*os.File) error { return sentinel }
	if e := r.Sync(context.Background()); !errors.Is(e, sentinel) {
		t.Fatal("directory creation acknowledged", e)
	}
	if _, e := os.Stat(filepath.Join(r.dir, "nested/child/file")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("file published after failed parent sync", e)
	}
	if len(r.index) != 0 {
		t.Fatal("index advanced before durable directories")
	}
	syncDirectoryFile = original
	blob, e := OpenDirectoryBlobStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer blob.Close()
	syncDirectoryFile = func(*os.File) error { return sentinel }
	if _, e = blob.Put(context.Background(), randomID(), randomID(), strings.NewReader("blob")); !errors.Is(e, sentinel) {
		t.Fatal("blob directory creation acknowledged", e)
	}
}

func TestDelegatedOwnerCannotProbeAllocationChanges(t *testing.T) {
	s, _ := testServer(t)
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxTotalBytes: 16384}, nil })
	c := s.Client(owner)
	f, _ := folderFor(t, c, Limits{MaxTotalBytes: 4096})
	p := Principal{"outside", "delegate"}
	if e := c.Grant(context.Background(), f.ID, p, Owner); e != nil {
		t.Fatal(e)
	}
	delegate := s.Client(p)
	for _, cap := range []int64{1024, 8192, 16384, 0} {
		if e := delegate.SetLimits(context.Background(), f.ID, Limits{MaxTotalBytes: cap}); e != ErrDenied {
			t.Fatal("allocation oracle", e)
		}
	}
	if e := delegate.Revoke(context.Background(), f.ID, p); e != nil {
		t.Fatal("deallocate/reallocate oracle", e)
	}
	if e := c.Grant(context.Background(), f.ID, p, Owner); e != nil {
		t.Fatal(e)
	}
	peer := Principal{"another", "writer"}
	if e := delegate.Grant(context.Background(), f.ID, peer, Writer); e != nil {
		t.Fatal("grant within allocated capacity", e)
	}
	if e := delegate.Revoke(context.Background(), f.ID, peer); e != nil {
		t.Fatal(e)
	}
}
func TestExpiredInFlightUploadCollectsStagingFile(t *testing.T) {
	s, _ := testServer(t)
	dir := t.TempDir()
	b, e := OpenDirectoryBlobStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	s.Blobs = b
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	pid, _ := PathID(k, f.ID, "x")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 4})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Meta.Transaction(context.Background(), func(m *Metadata) error {
		v := m.Tickets[ticket.ID]
		v.Writing = true
		m.Tickets[ticket.ID] = v
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Join(dir, f.ID), 0700); e != nil {
		t.Fatal(e)
	}
	staging := filepath.Join(dir, f.ID, ".upload-"+ticket.BlobID)
	if e = os.WriteFile(staging, []byte("part"), 0600); e != nil {
		t.Fatal(e)
	}
	s.Now = func() time.Time { return ticket.Expires.Add(time.Second) }
	if e = s.RecoverUploads(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = s.CollectGarbage(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(staging); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("interrupted upload bytes uncollected", e)
	}
}
func TestUnignoreAfterRestart(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	put(t, c, f, k, "hidden", 0, []byte("retained"))
	r := replicaFor(t, c, f, k, "ignore", true)
	writeLocal(t, r, ".drivesyncignore", "hidden\n")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	dir, state := r.dir, r.opts.StateDir
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(filepath.Join(dir, ".drivesyncignore")); e != nil {
		t.Fatal(e)
	}
	next, e := Attach(context.Background(), c, f.ID, k, dir, Options{StateDir: state, Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	if e = next.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "hidden"))
	if e != nil || string(b) != "retained" {
		t.Fatal(string(b), e)
	}
}

type failFirstUpload struct {
	Client
	failed bool
}

func (c *failFirstUpload) Upload(ctx context.Context, id string, t Ticket, r io.Reader) error {
	if !c.failed {
		c.failed = true
		_, _ = io.Copy(io.Discard, r)
		return ErrInvalid
	}
	return c.Client.Upload(ctx, id, t, r)
}
func TestOneFailedUploadDoesNotBlockOtherTransfers(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	put(t, base, f, k, "download", 0, []byte("remote"))
	c := &failFirstUpload{Client: base}
	r := replicaFor(t, c, f, k, "failure", true)
	writeLocal(t, r, "a-fails", "kept")
	writeLocal(t, r, "b-succeeds", "sent")
	if e := r.Sync(context.Background()); e != ErrInvalid {
		t.Fatal(e)
	}
	if got := filesOn(t, r); got["download"] != "remote" || got["a-fails"] != "kept" {
		t.Fatal(got)
	}
	rows, e := base.Changes(context.Background(), f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, row := range rows.Rows {
		m, e := OpenMetadata(k, f.ID, row)
		if e != nil {
			t.Fatal(e)
		}
		if m.Path == "b-succeeds" {
			found = true
		}
	}
	if !found {
		t.Fatal("failed upload blocked another upload")
	}
}
func TestUnreadableLocalDoesNotBlockOtherPaths(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read mode 0000")
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := replicaFor(t, c, f, k, "unreadable", true)
	writeLocal(t, r, "locked", "keep")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	locked := filepath.Join(r.dir, "locked")
	if e := os.Chmod(locked, 0000); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(locked, 0600)
	put(t, c, f, k, "later", 0, []byte("remote"))
	writeLocal(t, r, "local", "work")
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(filepath.Join(r.dir, "later")); e != nil || string(b) != "remote" {
		t.Fatal(string(b), e)
	}
	if len(r.Status().Skipped) == 0 {
		t.Fatal("unreadable file not reported")
	}
	lockedPID, _ := PathID(k, f.ID, "locked")
	rows, _ := c.Changes(context.Background(), f.ID, 0)
	for _, row := range rows.Rows {
		if row.PathID == lockedPID && row.Deleted {
			t.Fatal("unreadable file deleted")
		}
	}
}

type slowBatchClient struct{ Client }
type slowBatchReader struct{ io.Reader }

func (r slowBatchReader) Read(p []byte) (int, error) {
	if len(p) > 4096 {
		p = p[:4096]
	}
	time.Sleep(5 * time.Millisecond)
	return r.Reader.Read(p)
}
func (c slowBatchClient) Upload(ctx context.Context, id string, t Ticket, r io.Reader) error {
	return c.Client.Upload(ctx, id, t, slowBatchReader{r})
}
func TestSlowSmallBatchDoesNotExpireEarlierTickets(t *testing.T) {
	s, _ := testServer(t)
	s.ReservationTTL = 300 * time.Millisecond
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	r := replicaFor(t, slowBatchClient{base}, f, k, "slow-batch", true)
	for i := 0; i < 6; i++ {
		writeLocal(t, r, fmt.Sprintf("%d", i), strings.Repeat("x", ChunkSize-1))
	}
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	got, e := base.GetFolder(context.Background(), f.ID)
	if e != nil || got.Usage.Files != 6 {
		t.Fatal(got.Usage, e)
	}
}
