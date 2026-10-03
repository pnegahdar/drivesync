package dsreview

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// On a case-insensitive volume a peer writes into a locally ignored directory by
// changing case: ".GIT/hooks/pre-commit" lands in the ignored ".git/hooks/".
func TestPeerWritesIntoIgnoredDirViaCase(t *testing.T) {
	s := newServer(t)
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	r, dir := attach(t, oc, f, k, "victim")
	write(t, dir, ".drivesyncignore", ".git/\n")
	write(t, dir, ".git/hooks/pre-commit", "#!/bin/sh\necho original\n")
	if e := r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(dir, ".GIT")); e != nil {
		t.Skip("case-sensitive volume")
	}
	payload := []byte("#!/bin/sh\necho pwned\n")
	p := ".GIT/hooks/pre-commit"
	pid, _ := ds.PathID(k, f.ID, p)
	tk, _ := oc.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid, SealedSize: ds.SealedSize(int64(len(payload)))})
	var sealed bytes.Buffer
	ds.SealContent(&sealed, bytes.NewReader(payload), k, f.ID, tk.BlobID, pid)
	oc.Upload(bg, f.ID, tk, &sealed)
	meta, _ := ds.SealMetadata(k, f.ID, pid, ds.FileMetadata{Path: p, BlobID: tk.BlobID, Size: int64(len(payload)), Mode: 0755, Hash: hash(payload)})
	if _, e := oc.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, TicketID: tk.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	t.Logf("sync: %v", r.Sync(bg))
	b, _ := os.ReadFile(filepath.Join(dir, ".git/hooks/pre-commit"))
	st, _ := os.Stat(filepath.Join(dir, ".git/hooks/pre-commit"))
	t.Logf(".git/hooks/pre-commit now (%v): %q", st.Mode(), b)
	if bytes.Equal(b, payload) {
		t.Errorf("BUG: peer replaced a file inside a locally ignored directory")
	}
}
