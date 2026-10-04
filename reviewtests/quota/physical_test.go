package quota

import (
	"bytes"
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/pnegahdar/drivesync/internal/testkit"
)

// Reserve -> Upload -> CancelUpload frees the logical reservation but keeps the
// uploaded object, so physical storage grows without bound under a fixed quota.
func TestCancelLeavesBlobsOutsideQuota(t *testing.T) {
	m := testkit.OpenEngine(t)
	blobDir := filepath.Join(t.TempDir(), "blobs")
	bs, _ := ds.OpenDirectoryBlobStore(blobDir)
	defer bs.Close()
	s := ds.NewServer(m, bs)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 1 << 20}, nil
	})
	c := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, _ := mkFolder(t, c, ds.Limits{})
	chunk := make([]byte, (1<<20)-256)
	for i := 0; i < 20; i++ {
		tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: "00000000000000000000000000000000000000000000000000000000000000aa", SealedSize: (1 << 20) - 256})
		if e != nil {
			t.Fatal(e)
		}
		if e = c.Upload(bg, f.ID, tk, bytes.NewReader(chunk)); e != nil {
			t.Fatal(e)
		}
		if e = c.CancelUpload(bg, f.ID, tk.ID); e != nil {
			t.Fatal(e)
		}
		if e = s.CollectGarbage(bg); e != nil {
			t.Fatal(e)
		}
	}
	var total int64
	filepath.WalkDir(blobDir, func(p string, d fs.DirEntry, e error) error {
		if e == nil && !d.IsDir() {
			i, _ := d.Info()
			total += i.Size()
		}
		return nil
	})
	got, _ := c.GetFolder(bg, f.ID)
	t.Logf("quota 1 MiB; logical usage %+v; physical blob bytes %d", got.Usage, total)
	if total != 0 {
		t.Fatalf("cancel left %d orphan bytes", total)
	}
}
