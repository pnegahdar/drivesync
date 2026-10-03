package convergence

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Same livelock without any injected hook: a 60k-row folder over HTTP (120
// pages per full listing) plus another node committing ~10 small changes per
// second. The fresh replica's reconcile never observes a quiescent version.
func TestFreshAttachBusyFolderHTTP(t *testing.T) {
	s, m := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "owner"}
	admin := s.Client(owner)
	f, k := mkFolder(t, admin, ds.Limits{})
	putFile(t, admin, f, k, "important.txt", 0, []byte("needed on the new node"))
	seedTombstones(t, m, admin, f, k, "old", 61440)
	stop := make(chan struct{})
	var commits atomic.Int64
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			pid, _ := ds.PathID(k, f.ID, fmt.Sprintf("activity/%d", i))
			if _, e := admin.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, Deleted: true}}); e == nil {
				commits.Add(1)
			}
		}
	}()
	defer close(stop)
	b := attach(t, httpClients(t, s)(owner), f, k, "b")
	var last error
	start := time.Now()
	for i := 0; i < 5; i++ {
		last = b.Sync(bg)
	}
	t.Logf("5 syncs in %v, %d concurrent commits, last error=%v", time.Since(start), commits.Load(), last)
	if files(t, b)["important.txt"] != "needed on the new node" {
		t.Fatalf("fresh HTTP replica made no progress on a busy 60k-row folder: %v", last)
	}
}
