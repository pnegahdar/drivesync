package dsreview2

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
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
	for round := 0; round < 80; round++ { // alice's private activity: 20,480 rows
		var del []ds.Mutation
		for i := 0; i < 256; i++ {
			var b [32]byte
			rand.Read(b[:])
			del = append(del, ds.Mutation{PathID: hex.EncodeToString(b[:]), Deleted: true})
		}
		if _, e := ac.Commit(bg, private.ID, del); e != nil {
			t.Fatal(e)
		}
	}
	after := probe()
	t.Logf("bob's median reserve+cancel: %v before, %v after alice wrote 20,480 rows to a folder bob cannot see (x%.0f)", base, after, float64(after)/float64(base))
	if after > 5*base {
		t.Errorf("BUG: cross-tenant timing oracle on owner's private activity")
	}
}
