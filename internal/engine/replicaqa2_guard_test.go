package engine

// Second replica/local-disk QA pass against 6557aa0. Run:
//   go test ./internal/engine -run '^TestQA2' -count=1 -v
// Every test uses explicit temporary state (qaAttach) unless stated otherwise.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// T1. Retry's mass-delete acknowledgement is cleared only by a sync with no
// error at all (replica.go:1246). Any unrelated persistent error - here a
// symlink where a peer created a regular file, a retryable local obstacle that
// fails every pull without backoff - keeps allowMassDelete armed for the life
// of the process. A later accidental wipe then propagates without the guard.
func TestQA2MassDeleteAckOutlivesAcknowledgedDeletes(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	for i := 0; i < 10; i++ {
		writeLocal(t, a, fmt.Sprintf("old/f%d.txt", i), fmt.Sprint("old ", i))
	}
	qaSync(t, a, b)
	// Unrelated, persistent obstacle on a: a symlink at a path b creates.
	if e := os.Symlink("/nonexistent", filepath.Join(a.dir, "notes.txt")); e != nil {
		t.Fatal(e)
	}
	writeLocal(t, b, "notes.txt", "peer notes")
	if e := b.Sync(qaCtx); e != nil {
		t.Fatal(e)
	}
	// Deliberate cleanup, paused, acknowledged with Retry.
	os.RemoveAll(filepath.Join(a.dir, "old"))
	if e := a.Sync(qaCtx); e == nil || !strings.Contains(strings.Join(a.Status().Errors, "\n"), "deletes paused") {
		t.Fatalf("control: expected pause, got %v %v", e, a.Status().Errors)
	}
	a.RetryRejected()
	t.Logf("acknowledged sync: %v", a.Sync(qaCtx))
	t.Logf("next sync: %v", a.Sync(qaCtx))
	for p := range qaRemote(t, c, f, k) {
		if strings.HasPrefix(p, "old/") {
			t.Fatalf("control: acknowledged delete did not commit: %s", p)
		}
	}
	// Days later: new work, then an accidental wipe of all of it.
	for i := 0; i < 10; i++ {
		writeLocal(t, a, fmt.Sprintf("new/g%d.txt", i), fmt.Sprint("new ", i))
	}
	_ = a.Sync(qaCtx)
	_ = a.Sync(qaCtx)
	if n := len(qaRemote(t, c, f, k)); n < 10 {
		t.Fatalf("control: new work not uploaded (%d rows)", n)
	}
	os.RemoveAll(filepath.Join(a.dir, "new"))
	e := a.Sync(qaCtx)
	_ = b.Sync(qaCtx)
	left := 0
	for p := range qaRemote(t, c, f, k) {
		if strings.HasPrefix(p, "new/") {
			left++
		}
	}
	if left == 0 {
		t.Fatalf("100%% removal propagated without the guard; a stale Retry acknowledgement stayed armed (allowMassDelete=%v, sync err=%v)", a.allowMassDelete, e)
	}
}

// T2. The guard counts index entries, and every directory is an entry with
// hash "directory" (root.go:250-279). Directories hold no data, yet they
// dilute the ratio: deleting every file while the directory skeleton remains
// (find -type f -delete, a tool that empties a tree) is 50% of entries for one
// file per directory, so all content is deleted on every peer without a pause.
// The guard is also relative to the current index only, so a wipe that spans
// several syncs (slow rm -rf crossing a rescan, progressive volume failure)
// passes in sub-threshold steps.
func TestQA2MassDeleteGuardMissesWipes(t *testing.T) {
	t.Run("files only, directories remain", func(t *testing.T) {
		s, _ := testServer(t)
		c := s.Client(owner)
		f, k := folderFor(t, c, Limits{})
		a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
		for i := 0; i < 20; i++ {
			writeLocal(t, a, fmt.Sprintf("com/example/m%d/Main%d.java", i, i), fmt.Sprint("class ", i))
		}
		qaSync(t, a)
		filepath.WalkDir(a.dir, func(p string, d os.DirEntry, e error) error {
			if e == nil && !d.IsDir() && strings.HasSuffix(p, ".java") {
				os.Remove(p)
			}
			return nil
		})
		e := a.Sync(qaCtx)
		files := 0
		for p := range qaRemote(t, c, f, k) {
			if !strings.HasSuffix(p, "/") {
				files++
			}
		}
		if files == 0 {
			t.Fatalf("every file (100%% of content) deleted remotely without a pause: err=%v", e)
		}
	})
	t.Run("progressive wipe", func(t *testing.T) {
		s, _ := testServer(t)
		c := s.Client(owner)
		f, k := folderFor(t, c, Limits{})
		a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
		for i := 0; i < 100; i++ {
			writeLocal(t, a, fmt.Sprintf("f%03d.txt", i), fmt.Sprint(i))
		}
		qaSync(t, a)
		paused := false
		for next, step := 0, 0; next < 100; step++ {
			remaining := 100 - next
			n := remaining * 3 / 4 // 75% of what is left, below the 80% guard
			if remaining < 5 {
				n = remaining
			}
			for i := next; i < next+n; i++ {
				os.Remove(filepath.Join(a.dir, fmt.Sprintf("f%03d.txt", i)))
			}
			next += n
			if e := a.Sync(qaCtx); e != nil && strings.Contains(e.Error(), "paused") {
				paused = true
				break
			}
		}
		if n := len(qaRemote(t, c, f, k)); n == 0 && !paused {
			t.Fatalf("100 of 100 files deleted on every peer over a few syncs without a single pause")
		}
	})
}
