package dsreview2

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// A cross-tenant writer's reservation latency tracks the size of the owner's
// private folders, because Reserve/Commit/Cancel load the owner's whole account.
func TestCrossTenantTimingTracksPrivateRows(t *testing.T) {
	s := newServer(t)
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "evil", Subject: "bob"}
	ac, bc := s.Client(alice), s.Client(bob)
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 1000})
	if e := ac.Grant(bg, shared.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	private, _ := mkFolder(t, ac, ds.Limits{})
	probe := func() time.Duration {
		var ds_ []time.Duration
		for i := 0; i < 9; i++ {
			start := time.Now()
			tk, e := bc.Reserve(bg, shared.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", 1), SealedSize: 10})
			if e != nil {
				t.Fatal(e)
			}
			bc.CancelUpload(bg, shared.ID, tk.ID)
			ds_ = append(ds_, time.Since(start))
		}
		sort.Slice(ds_, func(i, j int) bool { return ds_[i] < ds_[j] })
		return ds_[4]
	}
	base := probe()
	// Admit the first batch through the public API; seed the remaining identical
	// legal tombstones in one storage transaction. Preserve all 20,480 rows.
	var first []ds.Mutation
	for i := 0; i < 256; i++ {
		var b [32]byte
		rand.Read(b[:])
		first = append(first, ds.Mutation{PathID: hex.EncodeToString(b[:]), Deleted: true})
	}
	if _, e := ac.Commit(bg, private.ID, first); e != nil {
		t.Fatal(e)
	}
	if e := s.Meta.Transaction(bg, func(m *ds.Metadata) error {
		f := m.Folders[private.ID]
		for round := 1; round < 80; round++ {
			f.Folder.Version++
			for i := 0; i < 256; i++ {
				var b [32]byte
				rand.Read(b[:])
				pid := hex.EncodeToString(b[:])
				m.Files[private.ID][pid] = ds.Row{FolderID: private.ID, PathID: pid, Version: f.Folder.Version, Deleted: true}
			}
		}
		m.Folders[private.ID] = f
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if got, e := ac.GetFolder(bg, private.ID); e != nil || got.Usage.Rows != 20480 {
		t.Fatal("scale fixture cardinality", got.Usage, e)
	}
	after := probe()
	t.Logf("bob's median reserve+cancel: %v before, %v after alice wrote 20,480 rows to a folder bob cannot see (x%.0f)", base, after, float64(after)/float64(base))
	if after > 5*base {
		t.Errorf("BUG: cross-tenant timing oracle on owner's private activity")
	}
}
