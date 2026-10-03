package confirmreview

import (
	"context"
	"fmt"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// busyClient models a folder that receives one commit while a full listing is
// in flight (e.g. a multi-page HTTP Changes(0) over a large folder with an
// active writer). Every full listing therefore observes Version != GetFolder.
type busyClient struct {
	ds.Client
	writer ds.Client
	f      ds.Folder
	k      ds.FolderKey
	n      int
	t      testing.TB
}

func (c *busyClient) Changes(ctx context.Context, id string, after uint64) (ds.Delta, error) {
	d, e := c.Client.Changes(ctx, id, after)
	if after == 0 && e == nil {
		c.n++
		if _, we := tryPut(c.writer, c.f, c.k, fmt.Sprintf("log/%06d.txt", c.n), 0, []byte("tick")); we != nil {
			c.t.Fatal(we)
		}
	}
	return d, e
}

// A freshly attached replica (cursor 0) always runs reconcile, which demands
// three attempts at a globally quiescent folder before doing anything else.
// Nothing is deleted when the cursor is 0, so the stability check buys nothing,
// yet the replica never completes its first sync while the folder stays busy.
func TestFreshAttachNeverSyncsBusyFolder(t *testing.T) {
	s, _ := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "owner"}
	c := s.Client(owner)
	f, k := mkFolder(t, c, ds.Limits{})
	putFile(t, c, f, k, "important.txt", 0, []byte("needed on the new node"))
	b := attach(t, &busyClient{Client: c, writer: c, f: f, k: k, t: t}, f, k, "b")
	var last error
	for i := 0; i < 5; i++ {
		last = b.Sync(bg)
	}
	got := files(t, b)
	if got["important.txt"] != "needed on the new node" {
		t.Fatalf("fresh replica made no progress after 5 syncs: last=%v files=%d status.Version=%d", last, len(got), b.Status().Version)
	}
}
