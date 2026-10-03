package dsreview2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

func diskBytes(dir string) (total int64) {
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if e == nil && !d.IsDir() {
			i, _ := d.Info()
			total += i.Size()
		}
		return nil
	})
	return
}

// One live row offered as a rename credit to many reservations: each ticket is
// reserved at ~0 bytes, but each may upload a full copy.
func TestRenameCreditReusedAcrossTickets(t *testing.T) {
	m, _ := ds.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "m.sqlite"))
	defer m.Close()
	blobDir := filepath.Join(t.TempDir(), "blobs")
	bs, _ := ds.OpenDirectoryBlobStore(blobDir)
	defer bs.Close()
	s := ds.NewServer(m, bs)
	now := time.Now()
	s.Now = func() time.Time { return now }
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 8 << 20}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "evil", Subject: "bob"}
	ac, bc := s.Client(alice), s.Client(bob)
	const limit = 3 << 20
	f, k := mkFolder(t, ac, ds.Limits{MaxTotalBytes: limit, MaxRows: 1000})
	if e := ac.Grant(bg, f.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	x := putFile(t, bc, f, k, "x.bin", 0, make([]byte, 1<<20))
	size := x.SealedSize
	n := 0
	for i := 0; i < 40; i++ {
		tk, e := bc.Reserve(bg, f.ID, renameRequest(fmt.Sprintf("%064x", i+1), x.SealedSize, x.PathID, x.Version))
		if e != nil {
			t.Logf("reserve %d: %v", i, e)
			break
		}
		if e = bc.Upload(bg, f.ID, tk, bytes.NewReader(make([]byte, size))); e != nil {
			t.Fatal(e)
		}
		n++
	}
	got, _ := bc.GetFolder(bg, f.ID)
	t.Logf("folder limit %d; accepted %d uploads of %d bytes; usage %+v; physical blob bytes on disk %d", limit, n, size, got.Usage, diskBytes(blobDir))
	if diskBytes(blobDir) > limit || got.Usage.Bytes+got.Usage.Reserved > limit {
		t.Errorf("BUG: physical storage %dx the folder allocation", diskBytes(blobDir)/limit)
	}
	// When the tickets expire, garbage is charged; meanwhile the owner's private writes see it.
	now = now.Add(10 * time.Minute)
	priv, pk := mkFolder(t, ac, ds.Limits{})
	pid, _ := ds.PathID(pk, priv.ID, "a")
	_, e := ac.Reserve(bg, priv.ID, ds.UploadRequest{PathID: pid, SealedSize: 1000})
	got, _ = ac.GetFolder(bg, f.ID)
	t.Logf("after expiry: shared usage %+v; alice private reserve -> %v; disk %d", got.Usage, e, diskBytes(blobDir))
}

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
