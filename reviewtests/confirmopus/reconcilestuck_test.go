package confirmreview

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// The ordinary pull isolates a failing local delete into retryRows, but the
// stale-replica reconcile returns on the first apply error, before uploads and
// downloads. One undeletable local file (here: inside a read-only directory)
// therefore stops every later sync of that replica after a compaction.
func TestReconcileDeleteFailureBlocksAllSync(t *testing.T) {
	s, _ := newServer(t)
	now := time.Now().UTC()
	s.Now = func() time.Time { return now }
	c := s.Client(ds.Principal{Tenant: "t", Subject: "owner"})
	f, k := mkFolder(t, c, ds.Limits{})
	a, b := attach(t, c, f, k, "a"), attach(t, c, f, k, "b")
	write(t, a, "ro/old.txt", "old")
	syncAll(t, a, b)
	if e := os.Remove(filepath.Join(a.dir, "ro", "old.txt")); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	now = now.Add(31 * 24 * time.Hour)
	if e := s.CollectGarbage(bg); e != nil {
		t.Fatal(e)
	}
	roDir := filepath.Join(b.dir, "ro")
	if e := os.Chmod(roDir, 0500); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.Chmod(roDir, 0700) })
	write(t, a, "fresh.txt", "new remote file")
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	write(t, b, "mine.txt", "new local file on b")
	var last error
	for i := 0; i < 3; i++ {
		last = b.Sync(bg)
	}
	if e := a.Sync(bg); e != nil {
		t.Fatal(e)
	}
	if files(t, b)["fresh.txt"] == "" || files(t, a)["mine.txt"] == "" {
		t.Fatalf("one undeletable path stalled all sync: last=%v b=%v a=%v", last, files(t, b), files(t, a))
	}
}
