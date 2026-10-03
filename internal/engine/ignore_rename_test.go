package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Rename detection (replica.go:827-833) takes every missing tracked
// entry as a source, including entries that are missing from the scan only
// because they are now ignored. The delete loop and deleteSafety both skip
// ignored entries; rename pairing does not. Adding an ignore rule for a tracked
// file and then copying it commits a delete of the ignored path; peers move
// their copy away. Every directory hashes to "directory", so ignoring a tracked
// directory and creating any unrelated directory tombstones the ignored
// directory while its children stay live: every peer then fails each sync
// with "directory not empty" (no backoff for local errors).
func TestIgnoredEntryUsedAsRenameSource(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		s, _ := testServer(t)
		c := s.Client(owner)
		f, k := folderFor(t, c, Limits{})
		a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
		b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
		writeLocal(t, a, "config/app.env", "TOKEN=peer-secret")
		syncTree(t, a, b)
		// a keeps app.env local from now on, then makes a backup copy.
		writeLocal(t, a, ".drivesyncignore", "config/app.env\n")
		syncTree(t, a)
		writeLocal(t, a, "config/app.env.bak", "TOKEN=peer-secret")
		syncTree(t, a, b)
		remote := remoteFiles(t, c, f, k)
		if _, ok := remote["config/app.env"]; !ok {
			t.Fatalf("an ignored, still-present file was deleted remotely via rename pairing; remote=%v peer b=%v", keys(remote), keys(filesOn(t, b)))
		}
	})
	t.Run("directory", func(t *testing.T) {
		s, _ := testServer(t)
		c := s.Client(owner)
		f, k := folderFor(t, c, Limits{})
		a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
		b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
		writeLocal(t, a, "build/out.bin", "artifact")
		writeLocal(t, a, "src/main.go", "package main")
		syncTree(t, a, b)
		writeLocal(t, a, ".drivesyncignore", "build/\n")
		syncTree(t, a)
		if e := os.Mkdir(filepath.Join(a.dir, "docs"), 0700); e != nil {
			t.Fatal(e)
		}
		syncTree(t, a)
		pid, _ := PathID(k, f.ID, "build")
		d, _ := c.Changes(syncCtx, f.ID, 0)
		for _, row := range d.Rows {
			if row.PathID == pid && row.Deleted {
				t.Errorf("ignored directory 'build' was tombstoned remotely while build/out.bin stays live")
			}
		}
		e1, e2 := b.Sync(syncCtx), b.Sync(syncCtx)
		if e2 != nil {
			t.Fatalf("peer b now fails every sync: %v / %v", e1, e2)
		}
	})
}

// A remote tombstone for an ignored path marks the entry Deleted but keeps
// its Hash (replica.go:1628-1631). The README promises ignored local contents
// are never touched. After a peer recreates the path and the user removes the
// ignore rule, the replayed row sees known && sameFile(local, deleted entry)
// and renames the download over the retained file with no conflict copy
// (replica.go:1843). The retained content - the only copy left anywhere - is gone.
func TestUnignoreOverwritesRetainedIgnoredFile(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "keys/deploy.pem", "ORIGINAL KEY")
	syncTree(t, a, b)
	writeLocal(t, a, ".drivesyncignore", "keys/\n")
	syncTree(t, a)
	os.Remove(filepath.Join(b.dir, "keys", "deploy.pem"))
	syncTree(t, b, a) // a retains its ignored copy, as documented
	if got, _ := os.ReadFile(filepath.Join(a.dir, "keys", "deploy.pem")); string(got) != "ORIGINAL KEY" {
		t.Fatalf("control: ignored file was not retained: %q", got)
	}
	writeLocal(t, b, "keys/deploy.pem", "ROTATED KEY")
	syncTree(t, b, a)
	os.Remove(filepath.Join(a.dir, ".drivesyncignore"))
	syncTree(t, a, b)
	for _, r := range []*Replica{a, b} {
		for _, v := range filesOn(t, r) {
			if v == "ORIGINAL KEY" {
				return
			}
		}
	}
	t.Fatalf("un-ignoring replaced the retained ignored file without a conflict copy; a=%v", filesOn(t, a))
}

// moveRemote marks the old entry Deleted but keeps its Hash
// (replica.go:2127). sync's upload filter is !sameFile(local, index[p]) with no
// Deleted check (replica.go:803), so a file later recreated at the old path
// with the same bytes (git checkout/stash of the moved file, cp back) is never
// uploaded and never counted as pending: silent permanent divergence.
func TestRecreatedFileAtMovedAwayPathNeverUploads(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "lib/util.go", "package lib")
	syncTree(t, a, b)
	if e := os.Rename(filepath.Join(b.dir, "lib", "util.go"), filepath.Join(b.dir, "lib", "helpers.go")); e != nil {
		t.Fatal(e)
	}
	syncTree(t, b, a)
	if _, e := os.Stat(filepath.Join(a.dir, "lib", "helpers.go")); e != nil {
		t.Fatalf("control: peer move not applied: %v", e)
	}
	writeLocal(t, a, "lib/util.go", "package lib") // restore the old file
	syncTree(t, a, b)
	if _, ok := remoteFiles(t, c, f, k)["lib/util.go"]; !ok {
		t.Fatalf("recreated lib/util.go never uploaded (pending up bytes %d); a=%v b=%v", a.Status().PendingUpBytes, keys(filesOn(t, a)), keys(filesOn(t, b)))
	}
}

var _ = strings.Contains
var _ = fmt.Sprint

// (local-only content shared with every grantee). patterns() silently
// drops .drivesyncignore unless it is a readable regular file
// (replica.go:1891-1896), and the file itself is ignored before any skip is
// reported. A symlinked ignore file (dotfile managers such as stow/chezmoi,
// or a shared rules file) or one that is temporarily unreadable disables
// every rule without a status entry: excluded secrets upload to all peers.
func TestUnreadableIgnoreFileFailsOpen(t *testing.T) {
	for _, mode := range []string{"symlink", "unreadable"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			f, k := folderFor(t, c, Limits{})
			a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
			rules := filepath.Join(t.TempDir(), "drivesync-ignore")
			writeTreeFile(t, rules, "secrets/\n*.key\n")
			switch mode {
			case "symlink":
				if e := os.Symlink(rules, filepath.Join(a.dir, ".drivesyncignore")); e != nil {
					t.Fatal(e)
				}
			case "unreadable":
				writeLocal(t, a, ".drivesyncignore", "secrets/\n*.key\n")
				os.Chmod(filepath.Join(a.dir, ".drivesyncignore"), 0)
				defer os.Chmod(filepath.Join(a.dir, ".drivesyncignore"), 0600)
			}
			writeLocal(t, a, "secrets/prod.env", "DB_PASSWORD=hunter2")
			writeLocal(t, a, "deploy.key", "-----BEGIN PRIVATE KEY-----")
			writeLocal(t, a, "README.md", "hello")
			e := a.Sync(syncCtx)
			remote := remoteFiles(t, c, f, k)
			if _, leaked := remote["secrets/prod.env"]; leaked {
				t.Fatalf("ignore rules silently disabled; excluded files shared: remote=%v err=%v skipped=%v", keys(remote), e, a.Status().Skipped)
			}
		})
	}
}
