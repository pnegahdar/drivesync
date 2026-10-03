package drivesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// Counting the transactions makes the trickle-upload regression deterministic.
type countedMetadata struct {
	*SQLiteMetaStore
	transactions int
}

func (m *countedMetadata) Transaction(ctx context.Context, fn func(*Metadata) error) error {
	m.transactions++
	return m.SQLiteMetaStore.Transaction(ctx, fn)
}

type singleByteReader struct{ io.Reader }

func (r singleByteReader) Read(p []byte) (int, error) { return r.Reader.Read(p[:1]) }
func TestTicketRenewOnlyAfterHalfTTL(t *testing.T) {
	s, m := testServer(t)
	count := &countedMetadata{SQLiteMetaStore: m}
	s.Meta = count
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	now := time.Now()
	s.Now = func() time.Time { return now }
	s.ReservationTTL = time.Minute
	pid, _ := PathID(k, f.ID, "stream")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 3000})
	if e != nil {
		t.Fatal(e)
	}
	count.transactions = 0
	if e = c.Upload(context.Background(), f.ID, ticket, singleByteReader{bytes.NewReader(make([]byte, 3000))}); e != nil {
		t.Fatal(e)
	}
	if count.transactions != 2 {
		t.Fatalf("3000 reads made %d write transactions, want start+finish only", count.transactions)
	}
	count.transactions = 0
	now = now.Add(30 * time.Second)
	if e = s.renew(context.Background(), owner, f.ID, ticket.ID); e != nil {
		t.Fatal(e)
	}
	if count.transactions != 0 {
		t.Fatal("renewed at exactly half TTL")
	}
	now = now.Add(time.Nanosecond)
	if e = s.renew(context.Background(), owner, f.ID, ticket.ID); e != nil || count.transactions != 1 {
		t.Fatal(e, count.transactions)
	}
}
func TestSameOwnerUnrelatedRowsAreNotDecoded(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	shared, k := folderFor(t, c, Limits{MaxTotalBytes: 8192, MaxRows: 16})
	private, _ := folderFor(t, c, Limits{})
	p := Principal{owner.Tenant, "same-tenant"}
	if e := c.Grant(context.Background(), shared.ID, p, Writer); e != nil {
		t.Fatal(e)
	}
	data := fmt.Sprintf(`{"FolderID":%q,"PathID":%q,"SealedSize":"bad"}`, private.ID, strings.Repeat("a", 64))
	if _, e := m.db.Exec("INSERT INTO files(folder,path,data) VALUES(?,?,?)", private.ID, strings.Repeat("a", 64), []byte(data)); e != nil {
		t.Fatal(e)
	}
	guest := s.Client(p)
	pid, _ := PathID(k, shared.ID, "new")
	ticket, e := guest.Reserve(context.Background(), shared.ID, UploadRequest{PathID: pid, SealedSize: 1})
	if e != nil {
		t.Fatal("decoded unrelated private row", e)
	}
	if e = guest.Upload(context.Background(), shared.ID, ticket, strings.NewReader("x")); e != nil {
		t.Fatal(e)
	}
	meta, _ := SealMetadata(k, shared.ID, pid, FileMetadata{Path: "new", BlobID: ticket.BlobID, Mode: 0600})
	if _, e = guest.Commit(context.Background(), shared.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ListFolders(context.Background()); e != nil {
		t.Fatal("listing decoded row payloads", e)
	}
}
func TestAuthorizationChangesDoNotCallQuota(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	ctx := context.Background()
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 8192, MaxRows: 16})
	admin := Principal{"same", "admin"}
	peer := Principal{"other", "writer"}
	if e := c.Grant(ctx, f.ID, admin, Owner); e != nil {
		t.Fatal(e)
	}
	if e := c.Grant(ctx, f.ID, peer, Writer); e != nil {
		t.Fatal(e)
	}
	row := put(t, c, f, k, "file", 0, []byte("value"))
	private, pk := folderFor(t, c, Limits{})
	pr := put(t, c, private, pk, "private", 0, []byte("secret"))
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) {
		t.Error("authorization/delete queried pricing policy")
		return Quota{}, errors.New("policy unavailable")
	})
	if e := c.Grant(ctx, f.ID, peer, Reader); e != nil {
		t.Fatal(e)
	}
	if e := s.Client(admin).Revoke(ctx, f.ID, peer); e != nil {
		t.Fatal(e)
	}
	if e := s.Client(admin).Revoke(ctx, f.ID, admin); e != nil {
		t.Fatal("last grant cannot be revoked", e)
	}
	if e := c.SetLimits(ctx, f.ID, Limits{MaxTotalBytes: 512, MaxRows: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Commit(ctx, private.ID, []Mutation{{PathID: pr.PathID, BaseVersion: pr.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	if e := c.DeleteFolder(ctx, f.ID); e != nil {
		t.Fatal(e)
	}
}
func TestCancelUnusedTicketChargesNothing(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	s.Blobs = &failingDelete{BlobStore: s.Blobs, fail: true}
	pid, _ := PathID(k, f.ID, "unused")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 5000})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.CancelUpload(context.Background(), f.ID, ticket.ID); e != nil {
		t.Fatal(e)
	}
	got, e := c.GetFolder(context.Background(), f.ID)
	if e != nil || got.Usage != (Usage{}) {
		t.Fatal(got.Usage, e)
	}
}

type downloadCounter struct {
	Client
	reads int
}

func (c *downloadCounter) Download(ctx context.Context, f, b string) (io.ReadCloser, error) {
	c.reads++
	return c.Client.Download(ctx, f, b)
}
func TestQuarantineDoesNotRedownloadUnchangedBlob(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	pid, _ := PathID(k, f.ID, "malformed")
	ticket, e := base.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = base.Upload(context.Background(), f.ID, ticket, strings.NewReader("x")); e != nil {
		t.Fatal(e)
	}
	meta, _ := SealMetadata(k, f.ID, pid, FileMetadata{Path: "malformed", BlobID: ticket.BlobID, Mode: 0600})
	if _, e = base.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	client := &downloadCounter{Client: base}
	r := replicaFor(t, client, f, k, "quarantine", true)
	if e = r.Sync(context.Background()); !errors.Is(e, ErrIntegrity) {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		if e = r.Sync(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	if client.reads != 1 || len(r.Status().Quarantined) != 1 {
		t.Fatal(client.reads, r.Status())
	}
	dir, state := r.dir, r.opts.StateDir
	r.Close()
	r, e = Attach(context.Background(), client, f.ID, k, dir, Options{StateDir: state, Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = r.Sync(context.Background()); e != nil || client.reads != 1 {
		t.Fatal(e, client.reads)
	}
	row := put(t, base, f, k, "malformed", 1, []byte("fixed"))
	if row.Version < 2 {
		t.Fatal(row)
	}
	if e = r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(r.dir + "/malformed")
	if e != nil || string(data) != "fixed" || len(r.Status().Quarantined) != 0 {
		t.Fatal(string(data), e, r.Status())
	}
}
func TestRenameAtCapacityKeepsOldRemotePath(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := replicaFor(t, c, f, k, "a", true)
	b := replicaFor(t, c, f, k, "b", true)
	writeLocal(t, a, "old", "retained")
	syncReplicas(t, a, b)
	got, _ := c.GetFolder(context.Background(), f.ID)
	if e := c.SetLimits(context.Background(), f.ID, Limits{MaxTotalBytes: got.Usage.Bytes, MaxRows: 4}); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(a.dir+"/old", a.dir+"/new"); e != nil {
		t.Fatal(e)
	}
	syncReplicas(t, a, b)
	if filesOn(t, a)["new"] != "retained" || filesOn(t, b)["old"] != "retained" || len(a.Status().Rejected) != 1 {
		t.Fatal(filesOn(t, a), filesOn(t, b), a.Status())
	}
	if e := c.SetLimits(context.Background(), f.ID, Limits{}); e != nil {
		t.Fatal(e)
	}
	a.RetryRejected()
	syncReplicas(t, a, b)
	if filesOn(t, b)["new"] != "retained" {
		t.Fatal(filesOn(t, b))
	}
	if _, exists := filesOn(t, b)["old"]; exists {
		t.Fatal("old path survived successful rename")
	}
}

func TestBackgroundGCCollectsCancelledPublication(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	pid, _ := PathID(k, f.ID, "cancelled")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 3})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(context.Background(), f.ID, ticket, strings.NewReader("abc")); e != nil {
		t.Fatal(e)
	}
	if e = c.CancelUpload(context.Background(), f.ID, ticket.ID); e != nil {
		t.Fatal(e)
	}
	gcctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.RunGC(gcctx, time.Millisecond) }()
	defer func() {
		cancel()
		if e := <-done; !errors.Is(e, context.Canceled) {
			t.Error(e)
		}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, e = s.Blobs.Size(context.Background(), f.ID, ticket.BlobID)
		if errors.Is(e, os.ErrNotExist) {
			break
		}
		if e != nil || time.Now().After(deadline) {
			t.Fatal("background GC did not collect", e)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestAllocatedFileCapCannotBeRemoved(t *testing.T) {
	s, _ := testServer(t)
	s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) { return Quota{MaxFileBytes: 100}, nil })
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 8192, MaxRows: 16})
	if e := c.Grant(context.Background(), f.ID, Principal{owner.Tenant, "guest"}, Writer); e != nil {
		t.Fatal(e)
	}
	if e := c.SetLimits(context.Background(), f.ID, Limits{MaxTotalBytes: 8192, MaxRows: 16}); e != nil {
		t.Fatal(e)
	}
	pid, _ := PathID(k, f.ID, "large")
	_, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: 101})
	var le *LimitError
	if !errors.As(e, &le) || le.Maximum != 100 {
		t.Fatal(e)
	}
}
