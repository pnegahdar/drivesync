package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// T1-A. The root stays identifiable (marker readable through x permission) but
// cannot be listed. ScanLocal records blockLocal(".") but localBlocked never
// checks ".", and exactExists treats the EACCES as absence. Every tracked file
// is tombstoned and peers delete their copies. With >=5 entries the guard
// pauses, but its message tells the user Retry acknowledges the removal.
func TestQAUnlistableRootDeletesEverywhere(t *testing.T) {
	for _, n := range []int{3, 6} {
		t.Run(fmt.Sprint(n, "files"), func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			f, k := folderFor(t, c, Limits{})
			a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
			for i := 0; i < n; i++ {
				writeLocal(t, a, fmt.Sprintf("doc%d.txt", i), "irreplaceable")
			}
			b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
			qaSync(t, a, b)
			if e := os.Chmod(a.dir, 0300); e != nil { // e.g. ACL/mode change, flaky network listing
				t.Fatal(e)
			}
			e := a.Sync(qaCtx)
			t.Logf("first sync with unlistable root: %v", e)
			if e != nil && strings.Contains(e.Error(), "Retry acknowledges") {
				a.RetryRejected() // the status message invites this
				t.Logf("after Retry: %v", a.Sync(qaCtx))
			}
			os.Chmod(a.dir, 0700)
			_ = b.Sync(qaCtx)
			remote := qaRemote(t, c, f, k)
			onB := filesOn(t, b)
			if len(remote) != n || len(onB) != n {
				t.Fatalf("unlistable root propagated deletes: remote=%d files, peer b=%d files (want %d); a still has them on disk: %v", len(remote), len(onB), n, len(filesOn(t, a)))
			}
		})
	}
}

// T1-B. deleteSafety runs on the first scan; deletes use the second scan taken
// after pull. A removal that lands while pull is running (a download of a large
// file) is committed without the 80% guard.
func TestQAMassDeleteGuardBypassedDuringPull(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	hook := &qaHookClient{Client: base}
	a := qaAttach(t, hook, f, k, filepath.Join(t.TempDir(), "a"), "a")
	for i := 0; i < 10; i++ {
		writeLocal(t, a, fmt.Sprintf("src/file%d.go", i), fmt.Sprint("package x // ", i))
	}
	b := qaAttach(t, base, f, k, filepath.Join(t.TempDir(), "b"), "b")
	qaSync(t, a, b)

	// Control: the same removal between syncs is paused.
	away := filepath.Join(t.TempDir(), "src")
	if e := os.Rename(filepath.Join(a.dir, "src"), away); e != nil {
		t.Fatal(e)
	}
	if e := a.Sync(qaCtx); e == nil || !strings.Contains(e.Error(), "deletes paused") {
		t.Fatalf("control: expected pause, got %v", e)
	}
	// Restore, then remove the same files while pull is in flight.
	if e := os.Rename(away, filepath.Join(a.dir, "src")); e != nil {
		t.Fatal(e)
	}
	qaSync(t, a, b)
	if got := len(qaRemote(t, base, f, k)); got < 10 {
		t.Skipf("control restore failed (%d files)", got)
	}
	hook.beforeChanges = func() { os.RemoveAll(filepath.Join(a.dir, "src")) }
	e := a.Sync(qaCtx)
	t.Logf("sync with removal during pull: %v", e)
	if got := len(qaRemote(t, base, f, k)); got < 10 {
		t.Fatalf("mass removal during pull bypassed the guard: remote has %d of 11 entries", got)
	}
}

// T1-C. A tracked subtree that lives on another mounted volume. Unmounting it
// leaves an empty mountpoint; if it is under 80% of the folder, every file on
// it is deleted on all peers. Only the root's device/inode is checked.
func TestQANestedMountUnmountDeletesPeerCopies(t *testing.T) {
	if _, e := exec.LookPath("hdiutil"); e != nil {
		t.Skip("macOS hdiutil required")
	}
	img := filepath.Join(t.TempDir(), "nested.dmg")
	if out, e := exec.Command("hdiutil", "create", "-size", "4m", "-fs", "HFS+", "-volname", "dsqa", "-layout", "NONE", img).CombinedOutput(); e != nil {
		t.Skipf("hdiutil create: %v %s", e, out)
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	mount := filepath.Join(a.dir, "data")
	if e := os.Mkdir(mount, 0700); e != nil {
		t.Fatal(e)
	}
	if out, e := exec.Command("hdiutil", "attach", "-nobrowse", "-mountpoint", mount, img).CombinedOutput(); e != nil {
		t.Skipf("hdiutil attach: %v %s", e, out)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			exec.Command("hdiutil", "detach", "-force", mount).Run()
		}
	})
	for i := 0; i < 3; i++ {
		qaWrite(t, filepath.Join(mount, fmt.Sprintf("dataset%d.csv", i)), "only copy of field data")
	}
	for i := 0; i < 12; i++ {
		writeLocal(t, a, fmt.Sprintf("notes/n%d.md", i), "note")
	}
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	_ = a.Sync(qaCtx) // the volume's private .fseventsd/.Trashes are reported as skipped
	_ = b.Sync(qaCtx)
	_ = a.Sync(qaCtx)
	if got := filesOn(t, b); got["data/dataset0.csv"] != "" {
		t.Fatalf("cross-mount content escaped: %v", got)
	}
	if len(a.Status().Skipped) == 0 {
		t.Fatal("mount boundary not reported")
	}
	if out, e := exec.Command("hdiutil", "detach", mount).CombinedOutput(); e != nil {
		t.Fatalf("detach: %v %s", e, out)
	}
	mounted = false
	t.Logf("sync after unmount: %v", a.Sync(qaCtx))
	_ = b.Sync(qaCtx)
	if got := filesOn(t, b); len(got) != 12 {
		t.Fatalf("unmount changed unrelated peer files: %v", got)
	}
}

// T1-D. `git clean -fd` (or any cleanup of untracked dotfiles) in a root that
// is a repository removes .drivesync-root. Sync pauses forever: Retry cannot
// override it, re-Attach with the same state stays paused, and the default
// state path is derived from the directory, so there is no API recovery.
func TestQARootMarkerRemovalWedgesPermanently(t *testing.T) {
	if _, e := exec.LookPath("git"); e != nil {
		t.Skip("git required")
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "repo")
	state := filepath.Join(t.TempDir(), "state")
	git := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-c", "user.email=qa@example.com", "-c", "user.name=qa"}, args...)...)
		cmd.Dir = dir
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("git %v: %v %s", args, e, out)
		}
	}
	os.MkdirAll(dir, 0700)
	git("init", "-q")
	qaWrite(t, filepath.Join(dir, "README.md"), "v1")
	git("add", "README.md")
	git("commit", "-qm", "init")
	open := func() *Replica {
		r, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	a := open()
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	qaSync(t, a, b)
	git("clean", "-fdq") // a routine command; README.md is tracked and stays
	qaWrite(t, filepath.Join(a.dir, "README.md"), "v2 edited after git clean")
	t.Logf("sync: %v", a.Sync(qaCtx))
	a.RetryRejected()
	t.Logf("after Retry: %v", a.Sync(qaCtx))
	a.Close()
	a = open()
	defer a.Close()
	e := a.Sync(qaCtx)
	t.Logf("after re-Attach with the same state: %v", e)
	_ = b.Sync(qaCtx)
	if got := filesOn(t, b)["README.md"]; got != "v2 edited after git clean" {
		t.Fatalf("replica is permanently paused after its marker was removed from an unchanged root (same device/inode); peer has %q, err=%v", got, e)
	}
}

// T1-E. The root directory is replaced by a symlink to itself (same inode):
// `mv ~/shared ~/Sync/shared && ln -s ~/Sync/shared ~/shared`. identifyRoot
// follows the symlink and matches, but filepath.WalkDir does not descend a
// symlink root, so every scan is empty. Local edits never upload; with <5
// tracked entries nothing is reported at all.
func TestQASymlinkedRootSilentlyStopsUploads(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	parent := t.TempDir()
	a := qaAttach(t, c, f, k, filepath.Join(parent, "shared"), "a")
	writeLocal(t, a, "plan.md", "v1")
	writeLocal(t, a, "todo.md", "v1")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	qaSync(t, a, b)
	real := a.dir + "-moved"
	if e := os.Rename(a.dir, real); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(real, a.dir); e != nil {
		t.Fatal(e)
	}
	qaWrite(t, filepath.Join(a.dir, "plan.md"), "v2 edited through the symlink")
	e := a.Sync(qaCtx)
	st := a.Status()
	_ = b.Sync(qaCtx)
	if got := filesOn(t, b)["plan.md"]; got != "v2 edited through the symlink" && e == nil && len(st.Errors) == 0 {
		t.Fatalf("local edits silently stopped uploading after the root became a same-inode symlink: peer has %q, sync err=nil, status errors=%q skipped=%q", got, st.Errors, st.Skipped)
	}
}
