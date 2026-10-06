package drivesync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A crash after the state marker is rewritten, and before the binding update,
// must still reattach. Moved repairs the binding when the marker already
// names the directory.
func TestInterruptedMoveBetweenMarkerAndBinding(t *testing.T) {
	ctx := context.Background()
	client := moveServer(t, NewMemoryBlobStore(), ServerOptions{}).Client(Principal{"tenant", "owner"})
	key := NewFolderKey()
	folder, err := client.CreateFolder(ctx, FolderSpec{Name: "box"}, key)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	dir1 := filepath.Join(base, "one")
	state := filepath.Join(base, "state")
	if err = os.MkdirAll(dir1, 0o700); err != nil {
		t.Fatal(err)
	}
	replica, err := Attach(ctx, client, folder.ID, key, dir1, Options{StateDir: state, Manual: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir1, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err = replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = replica.Close(); err != nil {
		t.Fatal(err)
	}
	dir2 := filepath.Join(base, "two")
	if err = os.Rename(dir1, dir2); err != nil {
		t.Fatal(err)
	}
	real2, err := filepath.EvalSymlinks(dir2)
	if err != nil {
		t.Fatal(err)
	}
	realState, err := filepath.EvalSymlinks(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(realState, ".drivesync-state"), []byte(folder.ID+"/"+real2), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: state, Manual: true, Moved: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	body, err := os.ReadFile(filepath.Join(dir2, "a.txt"))
	if err != nil || string(body) != "hello" {
		t.Fatalf("recovered file: %q %v", body, err)
	}
}
