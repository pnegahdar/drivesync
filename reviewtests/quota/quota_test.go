package quota

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// The owner's plan allows 4 KiB of sealed bytes, yet the owner stores ~4 MiB of
// arbitrary data in the server's metadata store, plus unbounded tombstone rows.
func TestMetadataAndTombstonesBypassQuota(t *testing.T) {
	s := newServer(t)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 4096, MaxFolders: 1}, nil
	})
	c := s.Client(ds.Principal{Tenant: "t", Subject: "cheap-plan"})
	f, _ := mkFolder(t, c, ds.Limits{}) // folder limits are the owner's own choice
	payload := make([]byte, 16384)
	var muts []ds.Mutation

	var b [32]byte
	rand.Read(b[:])
	pid := hex.EncodeToString(b[:])
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(bg, f.ID, tk, bytes.NewReader(nil)); e != nil {
		t.Fatal(e)
	}
	muts = append(muts, ds.Mutation{PathID: pid, TicketID: tk.ID, Metadata: payload})
	if _, e = c.Commit(bg, f.ID, muts); e == nil {
		t.Fatal("metadata bypassed byte quota")
	}
	_ = c.CancelUpload(bg, f.ID, tk.ID)
	var del []ds.Mutation
	for i := 0; i < 256; i++ {
		rand.Read(b[:])
		del = append(del, ds.Mutation{PathID: hex.EncodeToString(b[:]), Deleted: true})
	}
	if _, e = c.Commit(bg, f.ID, del); e == nil {
		t.Fatal("tombstones bypassed byte quota")
	}

	got, _ := c.GetFolder(bg, f.ID)
	d, _ := c.Changes(bg, f.ID, 0)
	var meta int
	for _, r := range d.Rows {
		meta += len(r.Metadata)
	}
	t.Logf("quota=4096 sealed bytes; usage=%+v; server rows=%d; stored metadata bytes=%d", got.Usage, len(d.Rows), meta)
	if meta > 4096 {
		t.Fatal("metadata bypass")
	}
}
