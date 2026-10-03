package dsreview

import (
	"errors"
	"fmt"
	"math"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// With default (unlimited) limits, a writer on one shared folder parks four huge
// reservations; every reservation in all of the owner's other folders now fails.
func TestHugeReservationsBlockOwnersOtherFolders(t *testing.T) {
	s := newServer(t)
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "other", Subject: "bob"}
	ac, bc := s.Client(alice), s.Client(bob)
	private, pk := mkFolder(t, ac, ds.Limits{})
	putFile(t, ac, private, pk, "a", 0, []byte("existing"))
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 1 << 40})
	if e := ac.Grant(bg, shared.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		if _, e := bc.Reserve(bg, shared.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", i+1), SealedSize: math.MaxInt64 / 4}); !errors.Is(e, ds.ErrInvalid) {
			t.Fatalf("request cap not applied: %v", e)
		}
	}
	pid, _ := ds.PathID(pk, private.ID, "b")
	_, e := ac.Reserve(bg, private.ID, ds.UploadRequest{PathID: pid, SealedSize: 100})
	t.Logf("alice reserve in her private folder: %v", e)
	if e != nil {
		t.Errorf("BUG: bob blocks alice's private folder uploads: %v", e)
	}
}
