package disk

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// apply() returns ErrInvalid for purely local obstacles (a FIFO/socket at the
// destination, a regular file where a parent directory must go). pull() files
// every ErrInvalid as peer corruption: the valid remote row is quarantined and
// never retried while unchanged — even after the local obstacle is gone. The
// public Replica exposes no RetryRejected, so the file never arrives.
func TestLocalObstacleQuarantinesValidRemoteRowForever(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	ad, bd := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	ra, e := drivesync.Attach(bg, a, f.ID, key, ad, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true, Name: "a"})
	if e != nil {
		t.Fatal(e)
	}
	defer ra.Close()
	rb, e := drivesync.Attach(bg, s.Client(alice), f.ID, key, bd, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true, Name: "b"})
	if e != nil {
		t.Fatal(e)
	}
	defer rb.Close()
	// b has a local named pipe where a's new file will land.
	if e = syscall.Mkfifo(filepath.Join(bd, "report.txt"), 0600); e != nil {
		t.Skip(e)
	}
	_ = os.WriteFile(filepath.Join(ad, "report.txt"), []byte("quarterly numbers"), 0600)
	if e = ra.Sync(bg); e != nil {
		t.Fatal(e)
	}
	_ = rb.Sync(bg)
	q := rb.Status().Quarantined
	// The user removes the obstacle; the valid remote file should now arrive.
	_ = os.Remove(filepath.Join(bd, "report.txt"))
	for i := 0; i < 3; i++ {
		_ = rb.Sync(bg)
	}
	if _, e := os.Stat(filepath.Join(bd, "report.txt")); e != nil {
		t.Fatalf("valid remote file never arrives after the local obstacle is removed; quarantined=%v now=%v", q, rb.Status().Quarantined)
	}
}
