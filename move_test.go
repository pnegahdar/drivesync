package drivesync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type countingBlobs struct {
	BlobStore
	mu          sync.Mutex
	puts, opens int
}

func (c *countingBlobs) Put(ctx context.Context, folder, id string, r io.Reader) (int64, error) {
	n, err := c.BlobStore.Put(ctx, folder, id, r)
	if err == nil {
		c.mu.Lock()
		c.puts++
		c.mu.Unlock()
	}
	return n, err
}

func (c *countingBlobs) Open(ctx context.Context, folder, id string) (io.ReadCloser, error) {
	rc, err := c.BlobStore.Open(ctx, folder, id)
	if err == nil {
		c.mu.Lock()
		c.opens++
		c.mu.Unlock()
	}
	return rc, err
}

func (c *countingBlobs) counts() (puts, opens int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts, c.opens
}

func moveServer(t *testing.T, blobs BlobStore, opts ServerOptions) *Server {
	t.Helper()
	return apiServerWith(t, blobs, opts)
}

func TestMovedRootKeepsIndex(t *testing.T) {
	ctx := context.Background()
	blobs := &countingBlobs{BlobStore: NewMemoryBlobStore()}
	client := moveServer(t, blobs, ServerOptions{}).Client(Principal{"tenant", "owner"})
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
	if err = os.WriteFile(filepath.Join(dir1, "run.sh"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err = replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = replica.Close(); err != nil {
		t.Fatal(err)
	}
	puts, opens := blobs.counts()
	if puts < 2 {
		t.Fatalf("initial upload puts=%d", puts)
	}

	dir2 := filepath.Join(base, "two")
	if err = os.Rename(dir1, dir2); err != nil {
		t.Fatal(err)
	}
	if _, err = Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: state, Manual: true}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("attach without Moved: %v", err)
	}
	replica, err = Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: state, Manual: true, Moved: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	movedPuts, movedOpens := blobs.counts()
	if movedPuts != puts || movedOpens != opens {
		t.Fatalf("move transferred puts %d->%d opens %d->%d", puts, movedPuts, opens, movedOpens)
	}
	if err = replica.Close(); err != nil {
		t.Fatal(err)
	}

	copyDir := filepath.Join(base, "copy")
	if err = os.MkdirAll(copyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(dir2, ".drivesync-root"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(copyDir, ".drivesync-root"), marker, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = Attach(ctx, client, folder.ID, key, copyDir, Options{StateDir: state, Manual: true, Moved: true}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("copied root: %v", err)
	}

	var identity map[string]any
	if err = json.Unmarshal(marker, &identity); err != nil {
		t.Fatal(err)
	}
	identity["Token"] = "replaced-token"
	replaced, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir2, ".drivesync-root"), replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: state, Manual: true, Moved: true}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replaced token: %v", err)
	}
	if err = os.WriteFile(filepath.Join(dir2, ".drivesync-root"), marker, 0o600); err != nil {
		t.Fatal(err)
	}

	replica, err = Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: state, Manual: true, Moved: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir2, "b.txt"), []byte("more"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err = replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	editedPuts, editedOpens := blobs.counts()
	if editedPuts != movedPuts+1 || editedOpens != movedOpens {
		t.Fatalf("edit puts %d->%d opens %d->%d", movedPuts, editedPuts, movedOpens, editedOpens)
	}
	if err = replica.Close(); err != nil {
		t.Fatal(err)
	}

	fresh := filepath.Join(base, "fresh-state")
	if _, err = Attach(ctx, client, folder.ID, key, dir2, Options{StateDir: fresh, Manual: true, Moved: true}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("moved without state: %v", err)
	}
}
