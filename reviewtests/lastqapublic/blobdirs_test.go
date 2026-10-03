package qapublic

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pnegahdar/drivesync"
)

// Every folder that ever stored a blob leaves a directory in the
// DirectoryBlobStore forever, even after deletion and full collection.
func TestDirectoryBlobStoreLeaksPerFolderDirectories(t *testing.T) {
	meta, e := drivesync.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "m.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	defer meta.Close()
	root := filepath.Join(t.TempDir(), "blobs")
	blobs, e := drivesync.OpenDirectoryBlobStore(root)
	if e != nil {
		t.Fatal(e)
	}
	defer blobs.Close()
	s := drivesync.NewServer(meta, blobs, drivesync.ServerOptions{GCInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() { cancel(); <-done }()
	a := s.Client(alice)
	for i := 0; i < 20; i++ {
		key := drivesync.NewFolderKey()
		f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "tmp" + string(rune('a'+i))}, key)
		if e != nil {
			t.Fatal(e)
		}
		dir := filepath.Join(t.TempDir(), "w")
		r, e := drivesync.Attach(bg, a, f.ID, key, dir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true})
		if e != nil {
			t.Fatal(e)
		}
		_ = os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0600)
		_ = r.Sync(bg)
		r.Close()
		if e = a.DeleteFolder(bg, f.ID); e != nil {
			t.Fatal(e)
		}
	}
	time.Sleep(500 * time.Millisecond)
	entries, _ := os.ReadDir(root)
	list, _ := a.ListFolders(bg)
	if len(entries) > 0 {
		t.Fatalf("%d folders deleted and collected (%d remain), but %d per-folder directories remain in the blob store", 20, len(list), len(entries))
	}
}
