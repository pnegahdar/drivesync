package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// A Linux node's data disk fails to mount at boot. The attachment path is now
// the empty mountpoint, while replica state (default: a sibling directory, or
// any StateDir on the root filesystem) still lists every tracked file. The
// replica treats the empty root as "user deleted everything" and propagates
// tombstones to every peer; GC then destroys the blobs.
func TestEmptyMountpointPropagatesMassDelete(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "data"}, key)
	if e != nil {
		t.Fatal(e)
	}
	base := t.TempDir()
	mnt := filepath.Join(base, "mnt", "data")
	state := filepath.Join(base, "var", "lib", "drivesync")
	opts := drivesync.Options{Manual: true, StateDir: state}
	r, e := drivesync.Attach(bg, a, f.ID, key, mnt, opts)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		_ = os.WriteFile(filepath.Join(mnt, fmt.Sprintf("record-%d.db", i)), []byte("irreplaceable"), 0600)
	}
	if e = r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	peerDir := filepath.Join(t.TempDir(), "peer")
	peer, e := drivesync.Attach(bg, s.Client(alice), f.ID, key, peerDir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer peer.Close()
	if e = peer.Sync(bg); e != nil {
		t.Fatal(e)
	}
	r.Close()

	// Reboot: the disk is not mounted; the mountpoint directory is empty.
	if e = os.Rename(mnt, mnt+".disk"); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(mnt, 0755); e != nil {
		t.Fatal(e)
	}
	r2, e := drivesync.Attach(bg, a, f.ID, key, mnt, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer r2.Close()
	_ = r2.Sync(bg)
	_ = peer.Sync(bg)
	got, _ := a.GetFolder(bg, f.ID)
	left, _ := os.ReadDir(peerDir)
	t.Logf("authority live files=%d; peer now holds %d entries", got.Usage.Files, len(left))
	if got.Usage.Files < 5 {
		t.Fatalf("an empty attachment root deleted %d remote files and they vanished from every peer", 5-got.Usage.Files)
	}
}
