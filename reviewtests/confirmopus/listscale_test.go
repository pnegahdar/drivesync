package confirmreview

import (
	"os"
	"strconv"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Grants need no consent and a grantee cannot leave a folder. N distinct owners
// that each share one folder with a principal make that principal's
// ListFolders O(N^2): Metadata.Finish recomputes contributions(m) for every
// owner key. The listing holds the global SQLite writer lock, so a third,
// unrelated tenant's request waits behind it.
func TestListFoldersQuadraticInSharingOwners(t *testing.T) {
	n := 3000
	if v := os.Getenv("LIST_OWNERS"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	s, m := newServer(t)
	victim := ds.Principal{Tenant: "victim", Subject: "bot"}
	bystander := s.Client(ds.Principal{Tenant: "bystander", Subject: "b"})
	bf, _ := mkFolder(t, bystander, ds.Limits{})
	seedOwners(t, s, m, n, &victim)
	vc := s.Client(victim)
	done := make(chan time.Duration, 1)
	var listErr error
	go func() {
		start := time.Now()
		_, listErr = vc.ListFolders(bg)
		done <- time.Since(start)
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	_, _ = bystander.GetFolder(bg, bf.ID)
	waited := time.Since(start)
	list := <-done
	t.Logf("sharers=%d victimList=%v err=%v bystanderGetFolder=%v", n, list, listErr, waited)
	if listErr != nil || list > time.Second || waited > time.Second {
		t.Fatalf("listing %d shared folders took %v (err %v); unrelated bystander waited %v", n, list, listErr, waited)
	}
}
