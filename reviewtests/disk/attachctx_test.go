package disk

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pnegahdar/drivesync"
)

// Attach's ctx silently becomes the replica's lifetime. The common Go pattern
// of a bounded setup context stops automatic sync with no status signal.
func TestAttachContextSilentlyStopsReplica(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "w")
	setup, cancel := context.WithTimeout(bg, 10*time.Second)
	r, e := drivesync.Attach(setup, a, f.ID, key, dir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Debounce: 10 * time.Millisecond, RescanInterval: 50 * time.Millisecond})
	cancel() // setup finished
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = os.WriteFile(filepath.Join(dir, "note.txt"), []byte("hello"), 0600); e != nil {
		t.Fatal(e)
	}
	time.Sleep(time.Second)
	got, e := a.GetFolder(bg, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	st := r.Status()
	t.Logf("folder files=%d; status errors=%q pendingUp=%d", got.Usage.Files, st.Errors, st.PendingUpBytes)
	if got.Usage.Files == 0 {
		t.Fatalf("replica stopped syncing when the Attach context ended; Status reports no error")
	}
}
