package engine

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// T2 (design default; a stated use case is syncing repos). .git is synced by
// default (only .git/**/*.lock is ignored, replica.go:553). Two nodes that
// commit on the same branch get file-level conflict copies of refs/heads/main
// and logs/HEAD named "main (conflict from b ...)", which git ignores as broken
// ref names. The losing commit is reachable from no ref or reflog, so the next
// gc prunes it - and its deletes then propagate to every peer.
func TestQA2ConcurrentGitCommitsLoseACommit(t *testing.T) {
	if _, e := exec.LookPath("git"); e != nil {
		t.Skip("git required")
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	git := func(dir string, args ...string) string {
		cmd := exec.Command("git", append([]string{"-c", "user.email=qa@example.com", "-c", "user.name=qa", "-c", "gc.auto=0"}, args...)...)
		cmd.Dir = dir
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("git %v: %v %s", args, e, out)
		}
		return string(out)
	}
	repoA := filepath.Join(a.dir, "repo")
	qaWrite(t, filepath.Join(repoA, "README.md"), "v0")
	git(repoA, "init", "-q", "-b", "main")
	git(repoA, "add", ".")
	git(repoA, "commit", "-qm", "base")
	qaSync(t, a, b)
	repoB := filepath.Join(b.dir, "repo")
	if _, e := os.Stat(filepath.Join(repoB, ".git")); errors.Is(e, os.ErrNotExist) {
		// .git is not drive payload. The working tree still syncs, and A's
		// repository stays a repository across that sync.
		qaWrite(t, filepath.Join(repoA, "a.txt"), "from a")
		git(repoA, "add", ".")
		git(repoA, "commit", "-qm", "work on a")
		qaWrite(t, filepath.Join(repoB, "b.txt"), "from b")
		qaSync(t, a, b)
		log := git(repoA, "log", "--all", "--reflog", "--format=%s")
		if !strings.Contains(log, "base") || !strings.Contains(log, "work on a") {
			t.Fatalf("ignoring .git still broke the repo: %s", log)
		}
		if _, e := os.Stat(filepath.Join(repoB, ".git")); e == nil {
			t.Fatal(".git was synced")
		}
		if _, e := os.Stat(filepath.Join(repoA, "b.txt")); e != nil {
			t.Fatal("repo files did not sync back")
		}
		return
	}
	qaWrite(t, filepath.Join(repoA, "a.txt"), "from a")
	git(repoA, "add", ".")
	git(repoA, "commit", "-qm", "work on a")
	qaWrite(t, filepath.Join(repoB, "b.txt"), "from b")
	git(repoB, "add", ".")
	git(repoB, "commit", "-qm", "work on b")
	for i := 0; i < 3; i++ {
		_ = a.Sync(qaCtx)
		_ = b.Sync(qaCtx)
	}
	broken := []string{}
	for _, name := range []string{"a", "b"} {
		repo := map[string]string{"a": repoA, "b": repoB}[name]
		cmd := exec.Command("git", "log", "--all", "--reflog", "--format=%s")
		cmd.Dir = repo
		out, e := cmd.CombinedOutput()
		log := string(out)
		if e != nil || !strings.Contains(log, "work on a") || !strings.Contains(log, "work on b") {
			broken = append(broken, name+": "+strings.TrimSpace(log))
		}
	}
	if len(broken) > 0 {
		t.Fatalf("after syncing two concurrent commits: %q (conflict copies a=%d b=%d)", broken, len(a.Status().Conflicts), len(b.Status().Conflicts))
	}
}
