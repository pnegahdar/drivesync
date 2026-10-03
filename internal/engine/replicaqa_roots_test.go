package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func qaImage(t *testing.T, name string) string {
	t.Helper()
	if _, e := exec.LookPath("hdiutil"); e != nil {
		t.Skip("macOS hdiutil required")
	}
	img := filepath.Join(t.TempDir(), name+".dmg")
	if out, e := exec.Command("hdiutil", "create", "-size", "4m", "-fs", "HFS+", "-volname", name, "-layout", "NONE", img).CombinedOutput(); e != nil {
		t.Skipf("hdiutil create: %v %s", e, out)
	}
	return img
}
func qaMount(t *testing.T, img, at string) {
	t.Helper()
	os.MkdirAll(at, 0700)
	if out, e := exec.Command("hdiutil", "attach", "-nobrowse", "-mountpoint", at, img).CombinedOutput(); e != nil {
		t.Fatalf("hdiutil attach: %v %s", e, out)
	}
	t.Cleanup(func() { exec.Command("hdiutil", "detach", "-force", at).Run() })
}
func qaUnmount(t *testing.T, at string) {
	t.Helper()
	if out, e := exec.Command("hdiutil", "detach", at).CombinedOutput(); e != nil {
		t.Fatalf("hdiutil detach: %v %s", e, out)
	}
}

// T1-F. macOS assigns st_dev from the disk number, which depends on attach
// order (the same is true of USB disks on Linux, and of NFS/btrfs anonymous
// devices across reboots). The same volume, same path, same marker and same
// inode remounts with a different st_dev and the replica is paused forever.
func TestQADeviceNumberChangeOnRemountWedges(t *testing.T) {
	other, shared := qaImage(t, "qaother"), qaImage(t, "qashared")
	mounts := t.TempDir()
	mOther, mShared := filepath.Join(mounts, "other"), filepath.Join(mounts, "usb")
	qaMount(t, other, mOther)
	qaMount(t, shared, mShared)
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(mShared, "shared")
	state := filepath.Join(t.TempDir(), "state")
	a, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	writeLocal(t, a, "notes.md", "v1")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	qaSync(t, a, b)
	before, _ := os.Stat(dir)
	a.Close()
	qaUnmount(t, mOther)
	qaUnmount(t, mShared)
	qaMount(t, shared, mShared) // plugged in first this time
	after, _ := os.Stat(dir)
	t.Logf("st_dev before=%v after=%v", before.Sys(), after.Sys())
	a, e = Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	qaWrite(t, filepath.Join(dir, "notes.md"), "v2 after replug")
	e = a.Sync(qaCtx)
	a.RetryRejected()
	if e2 := a.Sync(qaCtx); e2 != nil {
		e = e2
	}
	_ = b.Sync(qaCtx)
	if got := filesOn(t, b)["notes.md"]; got != "v2 after replug" {
		t.Fatalf("same volume remounted with a new st_dev; replica paused permanently (Retry does not help): peer=%q err=%v", got, e)
	}
}

// T2. checkAttachments aborts on the first unreadable directory, while
// ScanLocal tolerates and protects it. One root-owned 0700 directory (docker
// bind mount, sudo, .Trashes/.fseventsd on a volume root) prevents Attach and
// therefore every restart of an existing replica.
func TestQAUnreadableSubdirectoryPreventsAttach(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "a")
	state := filepath.Join(t.TempDir(), "state")
	a, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	writeLocal(t, a, "doc.txt", "x")
	qaSync(t, a)
	a.Close()
	locked := filepath.Join(dir, "cache")
	os.Mkdir(locked, 0700)
	os.Chmod(locked, 0)
	defer os.Chmod(locked, 0700)
	a, e = Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true, StateDir: state})
	if e != nil {
		t.Fatalf("restart of an existing replica refused because one subdirectory is unreadable: %v", e)
	}
	a.Close()
}

// T2. Nothing locks the root itself: two replicas of the same folder can run on
// one directory with different state directories (default state for one
// process, explicit state for another). Each sees the other's work as local
// edits; conflict handling renames files out from under the other.
func TestQATwoReplicasCanShareOneRoot(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "shared")
	a := qaAttach(t, c, f, k, dir, "a")
	a2, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a2", Manual: true, StateDir: filepath.Join(t.TempDir(), "other-state")})
	if e == nil {
		a2.Close()
		t.Fatalf("second replica attached to %s while %s is active on it", dir, a.opts.StateDir)
	}
}

// T1-G. An explicit StateDir is only checked against attachments that already
// exist. A later attachment of an enclosing directory (for another folder)
// syncs the live index.sqlite, with every plaintext path of the first folder,
// to the second folder's grantees, and can overwrite it from peers.
func TestQALaterAttachmentEnclosingStateLeaksPlaintextPaths(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f1, k1 := folderFor(t, c, Limits{})
	f2, k2 := folderFor(t, c, Limits{})
	work := filepath.Join(t.TempDir(), "work")
	r1, e := Attach(qaCtx, c, f1.ID, k1, filepath.Join(t.TempDir(), "private"), Options{Name: "r1", Manual: true, StateDir: filepath.Join(work, ".state", "private")})
	if e != nil {
		t.Fatal(e)
	}
	defer r1.Close()
	writeLocal(t, r1, "hr/2026-layoffs-list.xlsx", "secret")
	qaSync(t, r1)
	r2, e := Attach(qaCtx, c, f2.ID, k2, work, Options{Name: "r2", Manual: true, StateDir: filepath.Join(t.TempDir(), "state2")})
	if e != nil {
		t.Logf("refused (expected): %v", e)
		return
	}
	defer r2.Close()
	_ = r2.Sync(qaCtx)
	for p, content := range qaRemote(t, c, f2, k2) {
		if strings.Contains(content, "2026-layoffs-list") {
			t.Fatalf("folder 2 received %s containing folder 1's plaintext path names (%d bytes)", p, len(content))
		}
	}
	t.Fatalf("attachment enclosing another replica's state was allowed: %v", fmt.Sprint(qaRemote(t, c, f2, k2) != nil))
}

// T2. checkAttachments and the marker write are not atomic. Two concurrent
// Attach calls (a station attaching its folders in parallel at boot) for a
// parent and a child directory can both succeed; the parent then uploads the
// child folder's plaintext files into the parent folder, and every later
// restart refuses both attachments.
func TestQAConcurrentNestedAttachRace(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f1, k1 := folderFor(t, c, Limits{})
	f2, k2 := folderFor(t, c, Limits{})
	for i := 0; i < 20; i++ {
		parent := filepath.Join(t.TempDir(), "work")
		child := filepath.Join(parent, "vault")
		os.MkdirAll(child, 0700)
		type res struct {
			r *Replica
			e error
		}
		out := make(chan res, 2)
		go func() {
			r, e := Attach(qaCtx, c, f1.ID, k1, parent, Options{Manual: true, StateDir: filepath.Join(t.TempDir(), "s1")})
			out <- res{r, e}
		}()
		go func() {
			r, e := Attach(qaCtx, c, f2.ID, k2, child, Options{Manual: true, StateDir: filepath.Join(t.TempDir(), "s2")})
			out <- res{r, e}
		}()
		a, b := <-out, <-out
		if a.e == nil && b.e == nil {
			os.WriteFile(filepath.Join(child, "root-token.txt"), []byte("vault secret"), 0600)
			_ = a.r.Sync(qaCtx)
			_ = b.r.Sync(qaCtx)
			leaked := false
			for _, kv := range []struct {
				f Folder
				k FolderKey
			}{{f1, k1}, {f2, k2}} {
				for p := range qaRemote(t, c, kv.f, kv.k) {
					if strings.Contains(p, "root-token") && kv.f.ID == f1.ID {
						leaked = true
					}
				}
			}
			a.r.Close()
			b.r.Close()
			t.Fatalf("iteration %d: nested attachments both attached; child folder's file uploaded into parent folder: %v", i, leaked)
		}
		for _, r := range []res{a, b} {
			if r.e == nil {
				r.r.Close()
			}
		}
	}
}

// T1-I. Nesting is only refused at Attach. Moving one synced folder into
// another (`mv ~/vault ~/work/`) is an ordinary reorganisation: the vault
// replica pauses, but the work replica's ScanLocal descends past the nested
// .drivesync-root and uploads the vault's plaintext into the work folder,
// for the work folder's grantees.
func TestQAMovingAnAttachmentIntoAnotherLeaksAcrossFolders(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	work, kw := folderFor(t, c, Limits{})
	vault, kv := folderFor(t, c, Limits{})
	base := t.TempDir()
	w := qaAttach(t, c, work, kw, filepath.Join(base, "work"), "w")
	v := qaAttach(t, c, vault, kv, filepath.Join(base, "vault"), "v")
	writeLocal(t, w, "readme.md", "work")
	writeLocal(t, v, "prod/db-password.txt", "hunter2")
	qaSync(t, w, v)
	if e := os.Rename(v.dir, filepath.Join(w.dir, "vault")); e != nil {
		t.Fatal(e)
	}
	t.Logf("vault replica: %v", v.Sync(qaCtx))
	t.Logf("work replica: %v", w.Sync(qaCtx))
	if got, ok := qaRemote(t, c, work, kw)["vault/prod/db-password.txt"]; ok {
		t.Fatalf("vault content %q was uploaded into the work folder", got)
	}
}
