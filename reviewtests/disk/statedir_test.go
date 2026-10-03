package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// StateDir is accepted even when it is an existing, shared directory. Attach
// then deletes every regular file there whose name starts with "upload-" and
// chmods the directory to 0700.
func TestAttachDeletesUnrelatedFilesInSharedStateDir(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	appState := t.TempDir() // e.g. the embedding application's own data directory
	mine := filepath.Join(appState, "upload-queue.json")
	if e = os.WriteFile(mine, []byte(`{"pending":["invoice-17.pdf"]}`), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(appState, 0755); e != nil {
		t.Fatal(e)
	}
	r, e := drivesync.Attach(bg, a, f.ID, key, filepath.Join(t.TempDir(), "w"), drivesync.Options{Manual: true, StateDir: appState})
	if e != nil {
		if _, statErr := os.Stat(mine); statErr != nil {
			t.Fatal(statErr)
		}
		return
	}
	defer r.Close()
	info, _ := os.Stat(appState)
	if _, e := os.Stat(mine); e != nil {
		t.Fatalf("Attach deleted the embedder's %s (state dir mode now %v)", filepath.Base(mine), info.Mode().Perm())
	}
}
