package disk

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pnegahdar/drivesync"
	"github.com/pnegahdar/drivesync/internal/testkit"
)

var bg = context.Background()

func newServer(t *testing.T) *drivesync.Server {
	meta := testkit.OpenPublic(t)
	return drivesync.NewServer(meta, drivesync.NewMemoryBlobStore(), drivesync.ServerOptions{})
}

var (
	alice = drivesync.Principal{Tenant: "acme", Subject: "alice"}
	bob   = drivesync.Principal{Tenant: "acme", Subject: "bob"}
)

var shared = drivesync.Limits{MaxFileBytes: 1 << 20, MaxTotalBytes: 1 << 26, MaxFiles: 1000, MaxRows: 10000}

// Alice attaches her private "vault" folder inside her shared "repo" folder and
// even ignores vault/ in the outer attachment. The default StateDir is created
// beside the inner attachment, i.e. inside the outer synced tree, so the
// private folder's plaintext index (names, hashes) syncs to every repo grantee.
func TestDefaultStateDirLeaksNestedFolderToOuterGrantees(t *testing.T) {
	s := newServer(t)
	a := s.Client(alice)
	repoKey, vaultKey := drivesync.NewFolderKey(), drivesync.NewFolderKey()
	repo, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "repo", Limits: shared}, repoKey)
	if e != nil {
		t.Fatal(e)
	}
	vault, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "vault", Limits: drivesync.Limits{}}, vaultKey)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Grant(bg, repo.ID, bob, drivesync.Reader); e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	repoDir := filepath.Join(root, "repo")
	outer, e := drivesync.Attach(bg, a, repo.ID, repoKey, repoDir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true, Ignore: []string{"vault/"}})
	if e != nil {
		t.Fatal(e)
	}
	defer outer.Close()
	inner, e := drivesync.Attach(bg, a, vault.ID, vaultKey, filepath.Join(repoDir, "vault"), drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true})
	if e != nil {
		return // Nested attachments must be refused before creating private state.
	}
	defer inner.Close()
	if e = os.WriteFile(filepath.Join(repoDir, "vault", "acquisition-target-globex.txt"), []byte("offer: $40M"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = inner.Sync(bg); e != nil {
		t.Fatal(e)
	}
	if e = outer.Sync(bg); e != nil {
		t.Fatal(e)
	}
	// Bob has no grant on vault. He only reads repo.
	bobDir := filepath.Join(t.TempDir(), "bob")
	br, e := drivesync.Attach(bg, s.Client(bob), repo.ID, repoKey, bobDir, drivesync.Options{StateDir: filepath.Join(t.TempDir(), "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer br.Close()
	if e = br.Sync(bg); e != nil {
		t.Fatal(e)
	}
	var leaked []string
	_ = filepath.WalkDir(bobDir, func(p string, d fs.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return nil
		}
		b, _ := os.ReadFile(p)
		if bytes.Contains(b, []byte("acquisition-target-globex")) {
			rel, _ := filepath.Rel(bobDir, p)
			leaked = append(leaked, rel)
		}
		return nil
	})
	if _, e := os.Stat(filepath.Join(bobDir, "vault")); e == nil {
		t.Errorf("vault/ itself reached bob")
	}
	if len(leaked) > 0 {
		t.Fatalf("bob (no vault grant) received vault's plaintext file names via the default StateDir: %s", strings.Join(leaked, ", "))
	}
}
