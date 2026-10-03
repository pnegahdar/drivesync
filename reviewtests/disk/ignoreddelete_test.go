package disk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// A peer deletes directory D. Locally D still holds an untracked new file, so
// the directory cannot be removed — yet every ignored entry inside it (default
// patterns include *.tmp, *.swp, *~, ~$*) is destroyed first. Nothing about
// those files was ever uploaded, so they are unrecoverable.
func TestBlockedRemoteDirectoryDeleteStillDestroysIgnoredFiles(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	ad := filepath.Join(t.TempDir(), "a")
	bd := filepath.Join(t.TempDir(), "b")
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
	_ = os.MkdirAll(filepath.Join(ad, "D"), 0700)
	_ = os.WriteFile(filepath.Join(ad, "D", "a.txt"), []byte("tracked"), 0600)
	for i := 0; i < 2; i++ {
		_ = ra.Sync(bg)
		_ = rb.Sync(bg)
	}
	// On b: unsaved-work artifacts that are ignored by default, plus a new file.
	precious := filepath.Join(bd, "D", "draft.docx~")
	_ = os.WriteFile(precious, []byte("autosave of 3 hours of edits"), 0600)
	_ = os.WriteFile(filepath.Join(bd, "D", "new.txt"), []byte("just created"), 0600)
	// On a: the user deletes D.
	_ = os.RemoveAll(filepath.Join(ad, "D"))
	_ = ra.Sync(bg)
	_ = rb.Sync(bg)
	_, newErr := os.Stat(filepath.Join(bd, "D", "new.txt"))
	if _, e := os.Stat(precious); e != nil {
		t.Fatalf("remote delete of D destroyed local-only ignored file (D kept for new.txt: %v)", newErr == nil)
	}
}

// The documented "ignored-only contents are removed" rule, applied to the
// common setup of syncing repo working trees while ignoring .git/: a peer
// deleting the project directory recursively destroys this node's local-only
// git history (unpushed commits, stashes), with no conflict copy or status.
func TestRemoteDirectoryDeleteDestroysIgnoredGitHistory(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "repos"}, key)
	if e != nil {
		t.Fatal(e)
	}
	ad := filepath.Join(t.TempDir(), "a")
	bd := filepath.Join(t.TempDir(), "b")
	opts := func(n string) drivesync.Options {
		return drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true, Name: n, Ignore: []string{".git/"}}
	}
	ra, e := drivesync.Attach(bg, a, f.ID, key, ad, opts("a"))
	if e != nil {
		t.Fatal(e)
	}
	defer ra.Close()
	rb, e := drivesync.Attach(bg, s.Client(alice), f.ID, key, bd, opts("b"))
	if e != nil {
		t.Fatal(e)
	}
	defer rb.Close()
	_ = os.MkdirAll(filepath.Join(ad, "projectX", "src"), 0700)
	_ = os.WriteFile(filepath.Join(ad, "projectX", "src", "main.go"), []byte("package main"), 0600)
	for i := 0; i < 2; i++ {
		_ = ra.Sync(bg)
		_ = rb.Sync(bg)
	}
	// Node b has local, unpushed git history for the project.
	obj := filepath.Join(bd, "projectX", ".git", "objects", "ab", "cdef0123")
	_ = os.MkdirAll(filepath.Dir(obj), 0700)
	_ = os.WriteFile(obj, []byte("unpushed commit"), 0600)
	// Another node cleans up the project directory.
	_ = os.RemoveAll(filepath.Join(ad, "projectX"))
	_ = ra.Sync(bg)
	_ = rb.Sync(bg)
	if _, e := os.Stat(obj); e != nil {
		t.Fatalf("peer's directory delete erased this node's ignored .git history (status errors: %q)", rb.Status().Errors)
	}
}
