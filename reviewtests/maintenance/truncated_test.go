package maintenance

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
	"github.com/pnegahdar/drivesync/internal/testkit"
)

type countingBlobs struct {
	ds.BlobStore
	opens atomic.Int64
}

func (c *countingBlobs) Open(ctx context.Context, f, id string) (io.ReadCloser, error) {
	c.opens.Add(1)
	return c.BlobStore.Open(ctx, f, id)
}

// A writer stores a sealed blob cut before its final chunk. Every reader treats
// the complete-but-truncated object as a transient EOF forever: it re-downloads
// the whole blob on every sync, and its own edits to that path are never sent.
func TestStoredTruncatedBlobRetriesForeverAndPinsLocalEdits(t *testing.T) {
	m := testkit.OpenEngine(t)
	blobs := &countingBlobs{BlobStore: ds.NewMemoryBlobStore()}
	s := ds.NewServer(m, blobs)
	a := s.Client(alice)
	f, k := mkFolder(t, a, ds.Limits{MaxTotalBytes: 1 << 30, MaxRows: 10000, MaxFileBytes: 1 << 28})
	if e := a.Grant(bg, f.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	v1 := put(t, a, f, k, "doc.txt", 0, []byte("version one"))
	r, dir := attach(t, s.Client(alice), f, k, "alice")
	if e := r.Sync(bg); e != nil {
		t.Fatal(e)
	}

	// Bob (buggy or hostile writer with the key) publishes a truncated object.
	b := s.Client(bob)
	pid, _ := ds.PathID(k, f.ID, "doc.txt")
	full := bytes.Repeat([]byte("B"), 200_000)
	var sealed bytes.Buffer
	probe := ds.SealedSize(int64(len(full)))
	cut := probe - 21 - 100 // drop the final empty chunk and part of the last data chunk
	tk, e := b.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: v1.Version, SealedSize: cut, MetadataBytes: 512})
	if e != nil {
		t.Fatal(e)
	}
	if e = ds.SealContent(&sealed, bytes.NewReader(full), k, f.ID, tk.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = b.Upload(bg, f.ID, tk, bytes.NewReader(sealed.Bytes()[:cut])); e != nil {
		t.Fatal(e)
	}
	meta, _ := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: "doc.txt", BlobID: tk.BlobID, Size: int64(len(full)), Mode: 0600, Hash: hash(full)})
	if _, e = b.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, BaseVersion: v1.Version, TicketID: tk.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}

	_ = r.Sync(bg)
	// Alice edits her copy; it should either publish or become a conflict copy.
	if e = os.WriteFile(filepath.Join(dir, "doc.txt"), []byte("alice's important edit"), 0600); e != nil {
		t.Fatal(e)
	}
	before := blobs.opens.Load()
	for i := 0; i < 10; i++ {
		_ = r.Sync(bg)
	}
	downloads := blobs.opens.Load() - before
	st := r.Status()
	t.Logf("downloads of the same unchanged truncated row over 10 syncs: %d; quarantined=%d; pendingUp=%d; errors[-1]=%q", downloads, len(st.Quarantined), st.PendingUpBytes, last(st.Errors))

	// Does Alice's edit ever reach the authority?
	d, e := a.Changes(bg, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	published := false
	for _, row := range d.Rows {
		if row.Deleted {
			continue
		}
		md, e := ds.OpenMetadata(k, f.ID, row)
		if e == nil && md.Hash == hash([]byte("alice's important edit")) {
			published = true
		}
	}
	if downloads > 1 {
		t.Errorf("unchanged truncated row was downloaded %d more times instead of being quarantined", downloads)
	}
	if !published {
		t.Errorf("local edit to the path is never uploaded while the remote row stays unreadable")
	}
}

func last(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[len(s)-1]
}
