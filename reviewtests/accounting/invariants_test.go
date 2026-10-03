package accounting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

func transportClient(t *testing.T, s *ds.Server, http bool, p ds.Principal) ds.Client {
	if http {
		return httpClients(t, s)(p)
	}
	return s.Client(p)
}

type offlineDeleteStore struct{ ds.BlobStore }

func (b offlineDeleteStore) Delete(context.Context, string, string) error {
	return errors.New("blob storage unavailable")
}

func TestAllocatedDeleteRemainsFullyCharged(t *testing.T) {
	for _, h := range []bool{false, true} {
		t.Run(map[bool]string{false: "inprocess", true: "http"}[h], func(t *testing.T) {
			s, m := newServer(t)
			owner := ds.Principal{Tenant: "owner", Subject: "o"}
			writer := ds.Principal{Tenant: "other", Subject: "w"}
			c := transportClient(t, s, h, owner)
			w := transportClient(t, s, h, writer)
			f, _ := mkFolder(t, c, ds.Limits{MaxFileBytes: 1024, MaxTotalBytes: 4096, MaxRows: 8})
			pid := "0000000000000000000000000000000000000000000000000000000000000001"
			tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: 1024, MetadataBytes: 28})
			if e != nil {
				t.Fatal(e)
			}
			if e = c.Upload(bg, f.ID, tk, bytes.NewReader(make([]byte, 1024))); e != nil {
				t.Fatal(e)
			}
			d, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, TicketID: tk.ID, Metadata: make([]byte, 28)}})
			if e != nil {
				t.Fatal(e)
			}
			cap := int64(1024+28) + ds.RowCost
			s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
				return ds.Quota{MaxTotalBytes: cap, MaxFiles: 1}, nil
			})
			if e = c.SetLimits(bg, f.ID, ds.Limits{MaxFileBytes: 1024, MaxTotalBytes: cap, MaxRows: 1}); e != nil {
				t.Fatal(e)
			}
			if e = c.Grant(bg, f.ID, writer, ds.Writer); e != nil {
				t.Fatal(e)
			}
			if _, e = w.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: d.Version, Deleted: true}}); e != nil {
				t.Fatal(e)
			}
			s.Blobs = offlineDeleteStore{s.Blobs}
			if e = s.CollectGarbage(bg); e == nil {
				t.Fatal("expected GC outage")
			}
			f, e = c.GetFolder(bg, f.ID)
			if e != nil {
				t.Fatal(e)
			}
			var charged ds.Account
			e = m.Transaction(bg, func(md *ds.Metadata) error { k, _ := json.Marshal(owner); charged = md.Accounts[string(k)]; return nil })
			if e != nil {
				t.Fatal(e)
			}
			if f.Usage.Bytes > charged.Bytes || f.Usage.Rows+f.Usage.GarbageRows > charged.Rows {
				t.Fatalf("delete left usage bytes=%d rows=%d but owner charged bytes=%d rows=%d; allocated cap bytes=%d rows=%d", f.Usage.Bytes, f.Usage.Rows+f.Usage.GarbageRows, charged.Bytes, charged.Rows, cap, int64(1))
			}
		})
	}
}

type afterGet struct {
	ds.Client
	hook func()
}

func (c *afterGet) GetFolder(ctx context.Context, id string) (ds.Folder, error) {
	f, e := c.Client.GetFolder(ctx, id)
	if e == nil && c.hook != nil {
		h := c.hook
		c.hook = nil
		h()
	}
	return f, e
}
func TestCompactionBetweenFolderAndChanges(t *testing.T) {
	for _, h := range []bool{false, true} {
		t.Run(map[bool]string{false: "inprocess", true: "http"}[h], func(t *testing.T) {
			s, _ := newServer(t)
			now := time.Now()
			s.Now = func() time.Time { return now }
			c := transportClient(t, s, h, ds.Principal{Tenant: "t", Subject: "o"})
			f, k := mkFolder(t, c, ds.Limits{})
			row := putFile(t, c, f, k, "gone.txt", 0, []byte("original"))
			wrapped := &afterGet{Client: c}
			b := attach(t, wrapped, f, k, "offline")
			if e := b.Sync(bg); e != nil {
				t.Fatal(e)
			}
			if _, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
				t.Fatal(e)
			}
			now = now.Add(31 * 24 * time.Hour)
			wrapped.hook = func() {
				if e := s.CollectGarbage(bg); e != nil {
					t.Fatal(e)
				}
			}
			for i := 0; i < 4; i++ {
				if e := b.Sync(bg); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := os.Stat(filepath.Join(b.dir, "gone.txt")); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("deleted remote file survives compaction forever: files=%v status=%+v", files(t, b), b.Status())
			}
		})
	}
}

type failingDownload struct {
	ds.Client
	fail bool
}

func (c *failingDownload) Download(ctx context.Context, id, blob string) (io.ReadCloser, error) {
	if c.fail {
		return nil, io.ErrUnexpectedEOF
	}
	return c.Client.Download(ctx, id, blob)
}
func TestCompactionDropsObsoletePendingDownloads(t *testing.T) {
	for _, h := range []bool{false, true} {
		t.Run(map[bool]string{false: "inprocess", true: "http"}[h], func(t *testing.T) {
			s, _ := newServer(t)
			now := time.Now()
			s.Now = func() time.Time { return now }
			c := transportClient(t, s, h, ds.Principal{Tenant: "t", Subject: "o"})
			f, k := mkFolder(t, c, ds.Limits{})
			row := putFile(t, c, f, k, "gone.txt", 0, []byte("original"))
			wrapped := &failingDownload{Client: c, fail: true}
			b := attach(t, wrapped, f, k, "retry")
			if e := b.Sync(bg); !errors.Is(e, io.ErrUnexpectedEOF) {
				t.Fatal("expected failed initial transfer", e)
			}
			if _, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
				t.Fatal(e)
			}
			now = now.Add(31 * 24 * time.Hour)
			if e := s.CollectGarbage(bg); e != nil {
				t.Fatal(e)
			}
			wrapped.fail = false
			var e error
			for i := 0; i < 4; i++ {
				e = b.Sync(bg)
			}
			if e != nil {
				t.Fatalf("compacted remote row retried forever: %v; status=%+v", e, b.Status())
			}
		})
	}
}

type failAtEOF struct{ io.ReadCloser }

func (r *failAtEOF) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	if errors.Is(e, io.EOF) {
		e = io.ErrUnexpectedEOF
	}
	return n, e
}

type terminalFailureClient struct {
	ds.Client
	fail      bool
	downloads int
}

func (c *terminalFailureClient) Download(ctx context.Context, id, blob string) (io.ReadCloser, error) {
	r, e := c.Client.Download(ctx, id, blob)
	if e != nil {
		return r, e
	}
	c.downloads++
	if c.fail {
		return &failAtEOF{r}, nil
	}
	return r, nil
}
func TestTransportEOFErrorAfterFinalChunkIsRetried(t *testing.T) {
	for _, h := range []bool{false, true} {
		t.Run(map[bool]string{false: "inprocess", true: "http"}[h], func(t *testing.T) {
			s, _ := newServer(t)
			c := transportClient(t, s, h, ds.Principal{Tenant: "t", Subject: "o"})
			f, k := mkFolder(t, c, ds.Limits{})
			putFile(t, c, f, k, "file.txt", 0, []byte("original"))
			wrapped := &terminalFailureClient{Client: c, fail: true}
			b := attach(t, wrapped, f, k, "retry")
			if e := b.Sync(bg); e == nil {
				t.Fatal("expected interrupted transport error")
			}
			wrapped.fail = false
			for i := 0; i < 3; i++ {
				_ = b.Sync(bg)
			}
			if files(t, b)["file.txt"] != "original" {
				t.Fatalf("unchanged good row never retried after transport failure: downloads=%d quarantine=%v", wrapped.downloads, b.Status().Quarantined)
			}
		})
	}
}
