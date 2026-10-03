package finalreview

import (
	ds "github.com/pnegahdar/drivesync/internal/engine"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAttachExistingIdenticalContentAdopts(t *testing.T) {
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			s, _ := newServer(t)
			var c ds.Client = s.Client(ds.Principal{Tenant: "t", Subject: "owner"})
			if transport == "http" {
				c = httpClients(t, s)(ds.Principal{Tenant: "t", Subject: "owner"})
			}
			f, k := mkFolder(t, c, ds.Limits{})
			a := attach(t, c, f, k, "a")
			write(t, a, "pkg/file.txt", "identical")
			if e := a.Sync(bg); e != nil {
				t.Fatal(e)
			}
			before, _ := c.GetFolder(bg, f.ID)
			b := attach(t, c, f, k, "b")
			write(t, b, "pkg/file.txt", "identical")
			if e := b.Sync(bg); e != nil {
				t.Fatal(e)
			}
			if got := files(t, b); len(got) != 1 || got["pkg/file.txt"] != "identical" {
				t.Fatal("duplicate/conflict on identical attach", got)
			}
			after, _ := c.GetFolder(bg, f.ID)
			if after.Version != before.Version || len(b.Status().Conflicts) != 0 {
				t.Fatal("identical attach published a write", b.Status(), before, after)
			}
			write(t, b, "pkg/file.txt", "edit")
			syncAll(t, b, a)
			if files(t, a)["pkg/file.txt"] != "edit" {
				t.Fatal(files(t, a))
			}
		})
	}
}

func TestCompactedTombstoneReconcilesOfflineReplica(t *testing.T) {
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			s, _ := newServer(t)
			now := time.Now().UTC()
			s.Now = func() time.Time { return now }
			p := ds.Principal{Tenant: "t", Subject: "owner"}
			var c ds.Client = s.Client(p)
			if transport == "http" {
				c = httpClients(t, s)(p)
			}
			f, k := mkFolder(t, c, ds.Limits{})
			a, b, dirty := attach(t, c, f, k, "a"), attach(t, c, f, k, "offline"), attach(t, c, f, k, "dirty")
			write(t, a, "gone.txt", "original")
			syncAll(t, a, b, dirty)
			if e := os.Remove(filepath.Join(a.dir, "gone.txt")); e != nil {
				t.Fatal(e)
			}
			if e := a.Sync(bg); e != nil {
				t.Fatal(e)
			}
			now = now.Add(31 * 24 * time.Hour)
			if e := s.CollectGarbage(bg); e != nil {
				t.Fatal(e)
			}
			current, e := c.GetFolder(bg, f.ID)
			if e != nil || current.Usage.Rows != 0 || current.Usage.Bytes != 0 {
				t.Fatal("tombstone not compacted", current, e)
			}
			write(t, dirty, "gone.txt", "last offline write")
			if e = b.Sync(bg); e != nil {
				t.Fatal(e)
			}
			if len(files(t, b)) != 0 {
				t.Fatal("old replica resurrected deleted file", files(t, b))
			}
			if e = dirty.Sync(bg); e != nil {
				t.Fatal(e)
			}
			found := false
			for name, content := range files(t, dirty) {
				if strings.Contains(name, "conflict") && content == "last offline write" {
					found = true
				}
			}
			if !found {
				t.Fatal("offline write was lost", files(t, dirty))
			}
			write(t, b, "gone.txt", "recreated")
			if e = b.Sync(bg); e != nil {
				t.Fatal(e)
			}
			syncAll(t, b, a)
			if files(t, a)["gone.txt"] != "recreated" {
				t.Fatal("cannot recreate compacted path", files(t, a))
			}
		})
	}
}
