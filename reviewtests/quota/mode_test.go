package quota

import (
	"bytes"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// A keyed peer publishes a file with mode 0000 (or a local user chmods one file).
// Every later scan fails, so this replica neither uploads nor downloads again.
func TestModeZeroFileWedgesReplica(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	r, dir := attach(t, oc, f, k, "r")
	data := []byte("x")
	pid, _ := ds.PathID(k, f.ID, "locked")
	tk, _ := oc.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: ds.SealedSize(1)})
	var sealed bytes.Buffer
	ds.SealContent(&sealed, bytes.NewReader(data), k, f.ID, tk.BlobID, pid)
	oc.Upload(bg, f.ID, tk, &sealed)
	meta, _ := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: "locked", BlobID: tk.BlobID, Size: 1, Mode: 0, Hash: hash(data)})
	if _, e := oc.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, TicketID: tk.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	t.Logf("first sync: %v", r.Sync(bg))
	putFile(t, oc, f, k, "later.txt", 0, []byte("later"))
	write(t, dir, "my-local-work.txt", "unsynced")
	var e error
	for i := 0; i < 3; i++ {
		e = r.Sync(bg)
	}
	got, _ := oc.GetFolder(bg, f.ID)
	t.Logf("sync: %v; remote files=%d", e, got.Usage.Files)
	if e != nil {
		t.Errorf("BUG: replica wedged by a mode-0000 file: %v", e)
	}
}
