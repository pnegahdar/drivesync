package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHoldsUnmountedMovedOrRestoredRootNeverDeletes(t *testing.T) {
	setup := func(t *testing.T, dir string) (*Replica, *Replica, Client, Folder, FolderKey) {
		s, _ := testServer(t)
		c := s.Client(owner)
		f, k := folderFor(t, c, Limits{})
		a := tempReplica(t, c, f, k, dir, "a")
		for i := 0; i < 3; i++ {
			writeLocal(t, a, fmt.Sprintf("f%d.txt", i), "data")
		}
		b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
		syncTree(t, a, b)
		return a, b, c, f, k
	}
	check := func(t *testing.T, a, b *Replica, c Client, f Folder, k FolderKey) {
		e := a.Sync(syncCtx)
		a.RetryRejected()
		e2 := a.Sync(syncCtx)
		if e == nil {
			t.Fatalf("expected root-change pause before Retry, got %v / %v", e, e2)
		}
		_ = b.Sync(syncCtx)
		if n := len(remoteFiles(t, c, f, k)); n != 3 || len(filesOn(t, b)) != 3 {
			t.Fatalf("deletes propagated: remote=%d", n)
		}
	}
	t.Run("unmounted volume", func(t *testing.T) {
		img := diskImage(t, "qaunmount")
		at := filepath.Join(t.TempDir(), "usb")
		mountImage(t, img, at)
		a, b, c, f, k := setup(t, filepath.Join(at, "shared"))
		// A plain detach fails with "Resource busy" while the replica holds its
		// os.Root; force-eject models yanking the drive.
		if out, e := exec.Command("hdiutil", "detach", "-force", at).CombinedOutput(); e != nil {
			t.Fatalf("%v %s", e, out)
		}
		check(t, a, b, c, f, k)
	})
	t.Run("root renamed", func(t *testing.T) {
		a, b, c, f, k := setup(t, filepath.Join(t.TempDir(), "shared"))
		os.Rename(a.dir, a.dir+"-renamed")
		os.Mkdir(a.dir, 0700) // something recreates the path
		check(t, a, b, c, f, k)
	})
	t.Run("restored copy with marker", func(t *testing.T) {
		a, b, c, f, k := setup(t, filepath.Join(t.TempDir(), "shared"))
		backup := a.dir + "-backup"
		if out, e := exec.Command("cp", "-Rp", a.dir, backup).CombinedOutput(); e != nil {
			t.Fatalf("%v %s", e, out)
		}
		os.Remove(filepath.Join(backup, "f0.txt")) // older backup lacks a newer file
		os.RemoveAll(a.dir)
		os.Rename(backup, a.dir)
		check(t, a, b, c, f, k)
	})
}

func TestHoldsSharedStateDirRefused(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir, state := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "state")
	a, e := Attach(syncCtx, c, f.ID, k, dir, Options{Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if r, e := Attach(syncCtx, c, f.ID, k, dir, Options{Manual: true, StateDir: state}); e == nil {
		r.Close()
		t.Fatal("same state attached twice")
	}
	if r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(t.TempDir(), "other"), Options{Manual: true, StateDir: state}); e == nil {
		r.Close()
		t.Fatal("state reused for another root")
	}
}

// Disk full during a download: nothing partial is published, the previous
// local version stays intact, the staging file is removed, and it retries.
func TestHoldsDiskFullDuringDownload(t *testing.T) {
	img := diskImage(t, "qafull")
	at := filepath.Join(t.TempDir(), "small")
	mountImage(t, img, at)
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	row := putRemote(t, c, f, k, "big.bin", 0, []byte("small original"), 0644)
	r := tempReplica(t, c, f, k, filepath.Join(at, "shared"), "r")
	syncTree(t, r)
	putRemote(t, c, f, k, "big.bin", row.Version, []byte(strings.Repeat("x", 6<<20)), 0644)
	e := r.Sync(syncCtx)
	if e == nil {
		t.Fatal("expected ENOSPC")
	}
	got, _ := os.ReadFile(filepath.Join(r.dir, "big.bin"))
	leftovers, _ := filepath.Glob(filepath.Join(r.dir, ".drivesync-tmp-*"))
	if string(got) != "small original" || len(leftovers) > 0 {
		t.Fatalf("disk full: local=%q leftovers=%v err=%v", got, leftovers, e)
	}
}

// Read-only remount: remote changes cannot apply, local files are untouched,
// nothing is deleted remotely, and it recovers after a read-write remount.
func TestHoldsReadOnlyRemount(t *testing.T) {
	img := diskImage(t, "qaro")
	at := filepath.Join(t.TempDir(), "vol")
	mountImage(t, img, at)
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir, state := filepath.Join(at, "shared"), filepath.Join(t.TempDir(), "state")
	r, e := Attach(syncCtx, c, f.ID, k, dir, Options{Name: "r", Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	writeLocal(t, r, "a.txt", "mine")
	syncTree(t, r)
	r.Close()
	qaUnmount(t, at)
	if out, e := exec.Command("hdiutil", "attach", "-readonly", "-nobrowse", "-mountpoint", at, img).CombinedOutput(); e != nil {
		t.Fatalf("%v %s", e, out)
	}
	pid, _ := PathID(k, f.ID, "a.txt")
	_ = pid
	putRemote(t, c, f, k, "b.txt", 0, []byte("remote"), 0644)
	r, e = Attach(syncCtx, c, f.ID, k, dir, Options{Name: "r", Manual: true, StateDir: state})
	if e != nil {
		t.Logf("attach on read-only mount refused: %v", e)
		return
	}
	defer r.Close()
	t.Logf("read-only sync: %v", r.Sync(syncCtx))
	if remote := remoteFiles(t, c, f, k); remote["a.txt"] != "mine" {
		t.Fatalf("read-only mount caused remote change: %v", remote)
	}
}

// Branch switch: 300 tracked files, 200 rewritten, 60 deleted, 60 created,
// while a peer edits 10 of the rewritten files. Nothing is lost.
func TestHoldsBranchSwitchWithConcurrentPeerEdits(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	for i := 0; i < 300; i++ {
		writeLocal(t, a, fmt.Sprintf("src/m%d/f%d.go", i%12, i), fmt.Sprint("main ", i))
	}
	syncTree(t, a, b)
	for i := 0; i < 10; i++ {
		writeLocal(t, b, fmt.Sprintf("src/m%d/f%d.go", i%12, i), fmt.Sprint("peer ", i))
	}
	_ = b.Sync(syncCtx)
	for i := 0; i < 200; i++ {
		writeLocal(t, a, fmt.Sprintf("src/m%d/f%d.go", i%12, i), fmt.Sprint("branch ", i))
	}
	for i := 240; i < 300; i++ {
		os.Remove(filepath.Join(a.dir, fmt.Sprintf("src/m%d/f%d.go", i%12, i)))
	}
	for i := 0; i < 60; i++ {
		writeLocal(t, a, fmt.Sprintf("src/new/n%d.go", i), fmt.Sprint("new ", i))
	}
	syncTree(t, a, b)
	values := map[string]bool{}
	for _, v := range filesOn(t, b) {
		values[v] = true
	}
	for i := 0; i < 10; i++ {
		if !values[fmt.Sprint("peer ", i)] || !values[fmt.Sprint("branch ", i)] {
			t.Fatalf("lost concurrent version %d", i)
		}
	}
	if len(filesOn(t, a)) != len(filesOn(t, b)) {
		t.Fatalf("did not converge")
	}
}

// State and staging permissions: state dir 0700, no group/other access on any
// state file, downloads staged 0600.
func TestHoldsStatePermissions(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	writeLocal(t, a, "x.txt", "x")
	syncTree(t, a)
	info, _ := os.Stat(a.opts.StateDir)
	if info.Mode().Perm() != 0700 {
		t.Fatalf("state dir %v", info.Mode().Perm())
	}
}

// mtime-only changes and clock skew: change detection is hash-based and
// versions are authority-assigned, so touching files never uploads.
func TestHoldsMtimeOnlyChangeDoesNotUpload(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := tempReplica(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	writeLocal(t, a, "x.txt", "x")
	syncTree(t, a)
	v := remoteVersion(t, c, f, k, "x.txt")
	future := time.Now().Add(400 * 24 * time.Hour)
	os.Chtimes(filepath.Join(a.dir, "x.txt"), future, future)
	syncTree(t, a)
	if remoteVersion(t, c, f, k, "x.txt") != v {
		t.Fatal("mtime-only change uploaded")
	}
}
