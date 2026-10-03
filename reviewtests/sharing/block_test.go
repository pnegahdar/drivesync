package sharing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// slowDelete models an object store whose deletes take ~100ms (S3-like).
type slowDelete struct {
	ds.BlobStore
	calls *atomic.Int64
}

func (s slowDelete) Delete(c context.Context, f, id string) error {
	if s.calls != nil {
		s.calls.Add(1)
	}
	time.Sleep(100 * time.Millisecond)
	return s.BlobStore.Delete(c, f, id)
}
func (s slowDelete) Put(c context.Context, f, id string, r io.Reader) (int64, error) {
	return s.BlobStore.Put(c, f, id, r)
}

// A cross-tenant writer turns reused rename credits into garbage far beyond the
// folder's allocation; until GC drains it, the owner's private writes fail.
func TestCrossTenantGarbageBlocksOwner(t *testing.T) {
	m, _ := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "m.sqlite"))
	defer m.Close()
	var deletions atomic.Int64
	s := ds.NewServer(m, slowDelete{BlobStore: ds.NewMemoryBlobStore(), calls: &deletions})
	now := time.Now()
	s.Now = func() time.Time { return now }
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 8 << 20}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "evil", Subject: "bob"}
	ac, bc := s.Client(alice), s.Client(bob)
	f, k := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 3 << 20, MaxRows: 1000})
	if e := ac.Grant(bg, f.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	priv, pk := mkFolder(t, ac, ds.Limits{})
	x := putFile(t, bc, f, k, "x.bin", 0, make([]byte, 1<<20))
	for i := 0; i < 200; i++ {
		tk, e := bc.Reserve(bg, f.ID, renameRequest(fmt.Sprintf("%064x", i+1), x.SealedSize, x.PathID, x.Version))
		if e != nil {
			break
		}
		if e = bc.Upload(bg, f.ID, tk, bytes.NewReader(make([]byte, x.SealedSize))); e != nil {
			t.Fatal(e)
		}
	}
	now = now.Add(10 * time.Minute) // bob's tickets expire together
	failures := 0
	var last error
	start := time.Now()
	for i := 0; i < 10; i++ {
		pid, _ := ds.PathID(pk, priv.ID, fmt.Sprintf("f%d", i))
		tk, e := ac.Reserve(bg, priv.ID, ds.UploadRequest{PathID: pid, SealedSize: 1000})
		if e != nil {
			failures++
			last = e
		} else {
			ac.CancelUpload(bg, priv.ID, tk.ID)
		}
	}
	got, _ := ac.GetFolder(bg, f.ID)
	t.Logf("shared allocation 3 MiB; shared usage now %+v; alice private reserve failures %d/10 over %v (last: %v)", got.Usage, failures, time.Since(start), last)
	if deletions.Load() != 0 {
		t.Fatal("request path ran garbage collection", deletions.Load())
	}
	var l *ds.LimitError
	if failures > 0 && errors.As(last, &l) {
		t.Errorf("BUG: cross-tenant writer blocked owner's private writes despite allocation")
	}
}
