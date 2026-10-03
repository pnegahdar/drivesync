package finalreview

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// Finding: GetFolder/ListFolders consult pricing policy for the primary owner of
// any private folder, so a policy outage stops all sync of private folders
// (downloads and deletes included) and the owner's whole folder listing.
func TestPolicyOutageWedgesPrivateFolderSync(t *testing.T) {
	s, _ := newServer(t)
	var down atomic.Bool
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		if down.Load() {
			return ds.Quota{}, errors.New("billing unavailable")
		}
		return ds.Quota{MaxTotalBytes: 1 << 30}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	c := s.Client(alice)
	f, k := mkFolder(t, c, ds.Limits{})
	shared, _ := mkFolder(t, c, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 100})
	if e := c.Grant(bg, shared.ID, ds.Principal{Tenant: "acme", Subject: "bob"}, ds.Reader); e != nil {
		t.Fatal(e)
	}
	laptop := attach(t, c, f, k, "laptop")
	desktop := attach(t, c, f, k, "desktop")
	write(t, laptop, "a.txt", "one")
	write(t, laptop, "b.txt", "two")
	syncAll(t, laptop, desktop)
	down.Store(true)
	if e := os.Remove(filepath.Join(laptop.dir, "a.txt")); e != nil {
		t.Fatal(e)
	}
	e1 := laptop.Sync(bg)
	e2 := desktop.Sync(bg)
	_, le := c.ListFolders(bg)
	t.Logf("laptop sync: %v; desktop sync: %v; ListFolders: %v", e1, e2, le)
	if _, ok := files(t, desktop)["a.txt"]; ok {
		t.Error("existing-row delete did not propagate during a pricing outage")
	}
	if le != nil {
		t.Error("owner cannot list any folder (including shared ones) during a pricing outage")
	}
}
