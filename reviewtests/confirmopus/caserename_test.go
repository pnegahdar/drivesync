package confirmreview

import (
	"bytes"
	"os"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

func putDir(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, p string, base uint64, extra ...ds.Mutation) ds.Delta {
	t.Helper()
	pid, _ := ds.PathID(k, f.ID, p)
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, BaseVersion: base, SealedSize: ds.SealedSize(0), MetadataBytes: 512})
	if e != nil {
		t.Fatal(e)
	}
	var sealed bytes.Buffer
	if e = ds.SealContent(&sealed, bytes.NewReader(nil), k, f.ID, tk.BlobID, pid); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(bg, f.ID, tk, &sealed); e != nil {
		t.Fatal(e)
	}
	meta, _ := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Mode: 0700, Directory: true, Hash: "directory"})
	d, e := c.Commit(bg, f.ID, append([]ds.Mutation{{PathID: pid, BaseVersion: base, TicketID: tk.ID, Metadata: meta}}, extra...))
	if e != nil {
		t.Fatal(e)
	}
	return d
}

// A case-sensitive (Linux) replica renames Docs/ -> docs/. Its sync commits one
// rename pair per transaction, parent marker first. A macOS replica that pulls
// between those commits cannot remove the non-empty "Docs", aliases the new
// "docs" to "docs (case conflict ...)", keeps the alias forever, and re-uploads
// the stale "Docs" marker, resurrecting it for everyone.
func TestCaseOnlyDirectoryRenameObservedMidway(t *testing.T) {
	s, _ := newServer(t)
	c := s.Client(ds.Principal{Tenant: "t", Subject: "owner"})
	f, k := mkFolder(t, c, ds.Limits{})
	mac := attach(t, c, f, k, "mac")
	write(t, mac, "Docs/x.txt", "payload")
	if e := mac.Sync(bg); e != nil {
		t.Fatal(e)
	}
	d, _ := c.Changes(bg, f.ID, 0)
	versions := map[string]uint64{}
	for _, r := range d.Rows {
		m, e := ds.OpenMetadata(k, f.ID, r)
		if e != nil {
			t.Fatal(e)
		}
		versions[m.Path] = r.Version
	}
	oldDir, _ := ds.PathID(k, f.ID, "Docs")
	oldFile, _ := ds.PathID(k, f.ID, "Docs/x.txt")
	// Linux replica: first rename pair (directory marker).
	putDir(t, c, f, k, "docs", 0, ds.Mutation{PathID: oldDir, BaseVersion: versions["Docs"], Deleted: true})
	if e := mac.Sync(bg); e != nil {
		t.Log("mac sync:", e)
	}
	// Linux replica: second rename pair (the file).
	_, e := tryPutWithExtra(c, f, k, "docs/x.txt", []byte("payload"), ds.Mutation{PathID: oldFile, BaseVersion: versions["Docs/x.txt"], Deleted: true})
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if e := mac.Sync(bg); e != nil {
			t.Log("mac sync:", e)
		}
	}
	got := files(t, mac)
	entries, _ := os.ReadDir(mac.dir)
	var names []string
	for _, e := range entries {
		if e.Name() != ".drivesync-root" {
			names = append(names, e.Name())
		}
	}
	remote, _ := c.Changes(bg, f.ID, 0)
	var live []string
	for _, r := range remote.Rows {
		if !r.Deleted {
			m, _ := ds.OpenMetadata(k, f.ID, r)
			live = append(live, m.Path)
		}
	}
	if got["docs/x.txt"] != "payload" || len(names) != 1 || names[0] != "docs" || len(live) != 2 {
		t.Fatalf("case-only directory rename did not converge: mac top-level=%v files=%v remote live=%v", names, got, live)
	}
}

func tryPutWithExtra(c ds.Client, f ds.Folder, k ds.FolderKey, p string, data []byte, extra ds.Mutation) (ds.Row, error) {
	pid, _ := ds.PathID(k, f.ID, p)
	tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: ds.SealedSize(int64(len(data))), MetadataBytes: 512})
	if e != nil {
		return ds.Row{}, e
	}
	var sealed bytes.Buffer
	if e = ds.SealContent(&sealed, bytes.NewReader(data), k, f.ID, tk.BlobID, pid); e != nil {
		return ds.Row{}, e
	}
	if e = c.Upload(bg, f.ID, tk, &sealed); e != nil {
		return ds.Row{}, e
	}
	meta, _ := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hash(data)})
	d, e := c.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, TicketID: tk.ID, Metadata: meta}, extra})
	if e != nil {
		return ds.Row{}, e
	}
	return d.Rows[0], nil
}
