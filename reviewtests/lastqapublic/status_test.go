package qapublic

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// Status.Errors only ever appends (last 20). A transient failure stays in the
// "actionable" status forever after the replica is healthy again.
func TestStatusErrorsNeverClearAfterRecovery(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "w")
	r, e := drivesync.Attach(bg, a, f.ID, key, dir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	_ = r.Sync(cancelled) // e.g. a network blip
	_ = os.WriteFile(filepath.Join(dir, "x.txt"), []byte("x"), 0600)
	for i := 0; i < 3; i++ {
		if e := r.Sync(bg); e != nil {
			t.Fatal(e)
		}
	}
	if st := r.Status(); len(st.Errors) != 0 {
		t.Fatalf("healthy replica still reports %d stale errors: %q", len(st.Errors), st.Errors)
	}
}
