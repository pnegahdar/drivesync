package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// T2. Rename detection (replica.go:827-833) does not skip entries with a pending
// remote row in retryRows, which the delete loop does (replica.go:1198). A
// local rename of a file a peer just edited is deferred by missingDeferred,
// then paired: the delete half loses CAS, the pair is cancelled and the
// renamed file is moved to "final (conflict from a ...).md" (replica.go:913).
// Nothing conflicted at final.md; the user's rename silently disappears.
func TestQA2RenameOfPeerEditedFileBecomesConflictCopy(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "draft.md", "my chapter")
	qaSync(t, a, b)
	writeLocal(t, b, "draft.md", "peer typo fix")
	if e := b.Sync(qaCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(a.dir, "draft.md"), filepath.Join(a.dir, "final.md")); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		_ = a.Sync(qaCtx)
		_ = b.Sync(qaCtx)
	}
	onA := filesOn(t, a)
	if onA["final.md"] != "my chapter" {
		t.Fatalf("local rename to final.md was turned into a conflict copy; a=%v conflicts=%v", keys(onA), a.Status().Conflicts)
	}
}

// T2 (documented-contract violation; arguably T1 for restores). README: after
// Retry "Existing local writes remain local ... No deletes can be derived".
// Fresh adoption still applies tombstones carrying metadata: a local file
// whose bytes match a deleted row is removed (replica.go:1607-1624 falls
// through to Remove). Restoring a backup to recover files a peer deleted,
// then calling Retry, deletes the restored files again.
func TestQA2RetryAdoptionDeletesRestoredFiles(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "shared")
	a := qaAttach(t, c, f, k, dir, "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	for i := 0; i < 3; i++ {
		writeLocal(t, a, fmt.Sprintf("thesis/ch%d.tex", i), fmt.Sprint("chapter ", i))
	}
	qaSync(t, a, b)
	backup := dir + "-backup"
	if out, e := exec.Command("cp", "-Rp", dir, backup).CombinedOutput(); e != nil {
		t.Fatalf("%v %s", e, out)
	}
	os.Remove(filepath.Join(b.dir, "thesis", "ch1.tex")) // the accident
	qaSync(t, b, a)
	if _, e := os.Stat(filepath.Join(dir, "thesis", "ch1.tex")); !os.IsNotExist(e) {
		t.Fatalf("control: delete did not propagate: %v", e)
	}
	// Restore the whole tree from backup (new inode), inspect it, Retry.
	os.RemoveAll(dir)
	if e := os.Rename(backup, dir); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(qaCtx); e == nil {
		t.Fatal("control: changed root should pause")
	}
	a.RetryRejected()
	for i := 0; i < 3; i++ {
		_ = a.Sync(qaCtx)
		_ = b.Sync(qaCtx)
	}
	if _, e := os.Stat(filepath.Join(dir, "thesis", "ch1.tex")); e != nil {
		t.Fatalf("Retry adoption deleted the restored file (remote=%v): %v", keys(qaRemote(t, c, f, k)), e)
	}
}

// T2. r.adoption is memory-only (root.go:403). rebindRoot persists the empty
// index and version 0, so a restart (crash, Close, reboot) after Retry but
// before the first clean sync resumes as an ordinary attach: differing local
// files are moved to conflict copies and remote bytes take the canonical
// names, the opposite of the documented adoption rule.
func TestQA2AdoptionModeLostAcrossRestart(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "shared")
	state := filepath.Join(t.TempDir(), "state")
	open := func() *Replica {
		r, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	a := open()
	writeLocal(t, a, "plan.md", "v1")
	qaSync(t, a)
	os.Remove(filepath.Join(dir, rootMarker)) // e.g. git clean -fd
	writeLocal(t, a, "plan.md", "local edit while paused")
	_ = a.Sync(qaCtx)
	a.RetryRejected() // fresh adoption: local writes should stay canonical
	a.Close()         // ...but the process restarts before syncing
	a = open()
	defer a.Close()
	qaSync(t, a)
	got, _ := os.ReadFile(filepath.Join(dir, "plan.md"))
	if string(got) != "local edit while paused" {
		t.Fatalf("adoption rule lost on restart: plan.md=%q conflicts=%v", got, a.Status().Conflicts)
	}
}

var _ = strings.Contains

// T2 (wedge until manual action, then lost deletes). A replica that starts
// before its volume mounts (login item vs. external disk/automount) creates an
// empty root on the boot volume (replica.go:140), writes a marker there
// (replica.go:219-221) and holds an os.Root on it. When the right volume
// mounts, identifyRoot matches the saved identity exactly, but validRoot also
// compares the stale held handle (root.go:244-245), so sync stays paused until
// restart or Retry. Retry then rebinds through rebindRoot, which always drops
// the index even though the identity matches: deletes made since are lost and
// the files are downloaded again (and adoption semantics apply).
func TestQA2CorrectVolumeMountingLateStaysPaused(t *testing.T) {
	img := qaImage(t, "qa2late")
	at := filepath.Join(t.TempDir(), "usb")
	qaMount(t, img, at)
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir, state := filepath.Join(at, "shared"), filepath.Join(t.TempDir(), "state")
	open := func() *Replica {
		r, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	a := open()
	for i := 0; i < 3; i++ {
		writeLocal(t, a, fmt.Sprintf("f%d.txt", i), "data")
	}
	qaSync(t, a)
	a.Close()
	qaUnmount(t, at)
	a = open() // the app starts before the disk is mounted
	defer a.Close()
	if e := a.Sync(qaCtx); e == nil {
		t.Fatal("control: missing volume must pause")
	}
	if out, e := exec.Command("hdiutil", "attach", "-nobrowse", "-mountpoint", at, img).CombinedOutput(); e != nil {
		t.Fatalf("%v %s", e, out)
	}
	e := a.Sync(qaCtx)
	if e != nil {
		// What the status tells the user/agent to do:
		os.Remove(filepath.Join(dir, "f0.txt")) // a deliberate delete on the real disk
		a.RetryRejected()
		_ = a.Sync(qaCtx)
		_, back := os.Stat(filepath.Join(dir, "f0.txt"))
		t.Fatalf("identical root back but sync stays paused (%v); after Retry the index was discarded: f0.txt deleted locally came back=%v, remote has f0=%v", e, back == nil, qaRemote(t, c, f, k)["f0.txt"] != "")
	}
}

// T2 (plaintext outside the intended volume). When the root is itself the
// mount point, unmounting leaves an empty directory on the parent volume.
// Retry (what the pause message invites, and what an agent will do) rebinds
// to it: rebindRoot checks only Lstat/IsDir, nesting and folder (root.go:317-345),
// writes a new marker and adoption downloads the whole folder onto the boot
// disk. Remounting then hides these plaintext copies under the mount point.
func TestQA2RetryOnEmptyMountPointDownloadsOntoParentVolume(t *testing.T) {
	img := qaImage(t, "qa2mnt")
	at := filepath.Join(t.TempDir(), "secure")
	qaMount(t, img, at)
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, at, "a")
	for i := 0; i < 3; i++ {
		writeLocal(t, a, fmt.Sprintf("tax/%d.pdf", i), "confidential")
	}
	qaSync(t, a)
	if out, e := exec.Command("hdiutil", "detach", "-force", at).CombinedOutput(); e != nil {
		t.Fatalf("%v %s", e, out)
	}
	if e := a.Sync(qaCtx); e == nil {
		t.Fatal("control: unmounted root must pause")
	}
	a.RetryRejected()
	_ = a.Sync(qaCtx)
	if leaked, _ := filepath.Glob(filepath.Join(at, "tax", "*.pdf")); len(leaked) > 0 {
		t.Fatalf("Retry adopted the empty mount point and wrote %d plaintext files onto the parent volume: %v", len(leaked), leaked)
	}
}

// T2. The lifetime flock is on the marker inode, but validRoot accepts any
// marker with the same JSON. A tool that removes and restores untracked files
// (git stash -u / pop, rsync from a copy, an editor) recreates the marker with
// a new inode; the running replica stays valid while holding a lock on the
// deleted inode, and a second replica (another state dir) attaches to the same
// root concurrently (replica.go:230 locks whatever marker file is there now;
// validRoot, root.go:241-248, never compares it with the held lock).
func TestQA2RecreatedMarkerAdmitsSecondReplica(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "repo")
	a := qaAttach(t, c, f, k, dir, "a")
	writeLocal(t, a, "x.txt", "x")
	qaSync(t, a)
	marker := filepath.Join(dir, rootMarker)
	b, _ := os.ReadFile(marker)
	os.Remove(marker)                                 // git stash -u
	if e := os.WriteFile(marker, b, 0600); e != nil { // git stash pop
		t.Fatal(e)
	}
	if e := a.Sync(qaCtx); e != nil {
		t.Fatalf("control: restored marker should be valid: %v", e)
	}
	second, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "second", Manual: true, StateDir: filepath.Join(t.TempDir(), "other-state")})
	if e == nil {
		second.Close()
		t.Fatal("a second replica attached to a root that a live replica still syncs")
	}
}

// T2 (only copy becomes local-only). Renaming a tracked file to a name the
// folder rejects ("?", ":", trailing space; all legal on macOS/Linux) removes
// it from the scan and rename candidates (replica.go:641-647), so the old path
// is deleted on every peer while the bytes now live only on this replica as a
// rejected file. A limit-rejected rename keeps the old remote path pending;
// a name-rejected rename should too.
func TestQA2RenameToUnsupportedNameDeletesPeerCopies(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "contract.pdf", "signed contract")
	qaSync(t, a, b)
	if e := os.Rename(filepath.Join(a.dir, "contract.pdf"), filepath.Join(a.dir, "contract (final?).pdf")); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		_ = a.Sync(qaCtx)
		_ = b.Sync(qaCtx)
	}
	if _, ok := filesOn(t, b)["contract.pdf"]; !ok {
		t.Fatalf("rename to a rejected name deleted every peer copy; a rejections=%v remote=%v", a.Status().Rejected, keys(qaRemote(t, c, f, k)))
	}
}
