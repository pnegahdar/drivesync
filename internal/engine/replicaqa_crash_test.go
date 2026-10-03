package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Child half of TestQAKillNineDuringLargeSync. Runs only when re-executed.
func TestQAKillChild(t *testing.T) {
	if os.Getenv("QA_KILL_CHILD") == "" {
		t.Skip("child process only")
	}
	key, _ := hex.DecodeString(os.Getenv("QA_KEY"))
	var k FolderKey
	copy(k[:], key)
	c := NewHTTPClient(os.Getenv("QA_URL"), http.Header{"Tenant": {owner.Tenant}, "Subject": {owner.Subject}})
	r, e := Attach(context.Background(), c, os.Getenv("QA_FOLDER"), k, os.Getenv("QA_DIR"), Options{Name: "a", Manual: true, StateDir: os.Getenv("QA_STATE"), RetryInterval: 10 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	for i := 0; ; i++ {
		e = r.Sync(context.Background())
		if os.Getenv("QA_FINAL") != "" && e == nil && i > 2 {
			return
		}
		if os.Getenv("QA_FINAL") != "" && i > 40 {
			t.Fatalf("final child did not converge: %v", e)
		}
	}
}

// Verified-holds control: kill -9 at seeded random points during a two-way
// sync with conflicts, peer edits, deletes and renames; then converge.
func TestQAKillNineDuringLargeSync(t *testing.T) {
	if os.Getenv("QA_KILL_CHILD") != "" {
		t.Skip()
	}
	seed := int64(20261003)
	rng := rand.New(rand.NewSource(seed))
	s, _ := testServer(t)
	hs := httptest.NewServer(s.Handler(func(r *http.Request) (Principal, error) {
		return Principal{r.Header.Get("Tenant"), r.Header.Get("Subject")}, nil
	}))
	defer hs.Close()
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	dirA, state := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "state")
	content := func(tag string, i int) string {
		n := 1024 + rng.Intn(200*1024)
		return fmt.Sprintf("%s-%d:", tag, i) + strings.Repeat(string(rune('a'+i%26)), n)
	}
	for i := 0; i < 160; i++ {
		writeLocal(t, b, fmt.Sprintf("b/%02d/f%d.bin", i%8, i), content("b", i))
	}
	qaSync(t, b)
	aOriginal := map[string]string{}
	for i := 0; i < 80; i++ {
		p := fmt.Sprintf("a/%02d/g%d.bin", i%5, i)
		if i < 20 {
			p = fmt.Sprintf("b/%02d/f%d.bin", i%8, i) // concurrent creation: must become a conflict, not a loss
		}
		aOriginal[p] = content("a", i)
		qaWrite(t, filepath.Join(dirA, filepath.FromSlash(p)), aOriginal[p])
	}
	run := func(final bool) (*exec.Cmd, error) {
		cmd := exec.Command(os.Args[0], "-test.run", "^TestQAKillChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), "QA_KILL_CHILD=1", "QA_URL="+hs.URL, "QA_FOLDER="+f.ID, "QA_KEY="+hex.EncodeToString(k[:]), "QA_DIR="+dirA, "QA_STATE="+state)
		if final {
			cmd.Env = append(cmd.Env, "QA_FINAL=1")
		}
		return cmd, cmd.Start()
	}
	kills := 0
	for round := 0; round < 14; round++ {
		cmd, e := run(false)
		if e != nil {
			t.Fatal(e)
		}
		time.Sleep(time.Duration(40+rng.Intn(700)) * time.Millisecond)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		kills++
		// Peer churn between kills: edit, delete, rename.
		i := rng.Intn(160)
		p := filepath.Join(b.dir, fmt.Sprintf("b/%02d/f%d.bin", i%8, i))
		switch round % 3 {
		case 0:
			os.WriteFile(p, []byte(content("b-edit", i)), 0600)
		case 1:
			os.Remove(p)
		case 2:
			os.Rename(p, p+".renamed")
		}
		if e := b.Sync(qaCtx); e != nil && !errors.Is(e, ErrConflict) {
			t.Logf("b sync: %v", e)
		}
	}
	cmd, e := run(true)
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e != nil {
		t.Fatalf("final child: %v (seed %d)", e, seed)
	}
	qaSync(t, b)
	onA, onB := map[string]string{}, filesOn(t, b)
	filepath.WalkDir(dirA, func(p string, d os.DirEntry, e error) error {
		if e == nil && !d.IsDir() && d.Name() != ".drivesync-root" {
			rel, _ := filepath.Rel(dirA, p)
			if strings.Contains(d.Name(), ".drivesync-tmp-") {
				t.Errorf("staging file left behind: %s", rel)
			}
			bs, _ := os.ReadFile(p)
			onA[filepath.ToSlash(rel)] = string(bs)
		}
		return nil
	})
	if len(onA) != len(onB) {
		t.Errorf("did not converge after %d kills: a=%d files b=%d files (seed %d)", kills, len(onA), len(onB), seed)
	}
	for p, v := range onB {
		if onA[p] != v {
			t.Errorf("diverged at %s (seed %d)", p, seed)
		}
	}
	values := map[string]bool{}
	for _, v := range onB {
		values[v] = true
	}
	for p, v := range aOriginal {
		if !values[v] {
			t.Errorf("a's original %s was lost after kill -9 (seed %d)", p, seed)
		}
	}
	t.Logf("kills=%d files=%d conflicts=%d", kills, len(onB), strings.Count(fmt.Sprint(keys(onB)), "conflict from"))
}

func keys(m map[string]string) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}

// T2. Retry acknowledges a mass removal for exactly one sync. Deletes are
// committed one RPC at a time, so any interruption part-way (network blip,
// Close, ctx timeout) re-arms the pause and the user must find and Retry again.
func TestQARetryAcknowledgementLostOnInterruptedDelete(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	flaky := &qaFailingCommits{Client: base, failAfter: -1}
	a := qaAttach(t, flaky, f, k, filepath.Join(t.TempDir(), "a"), "a")
	for i := 0; i < 20; i++ {
		writeLocal(t, a, fmt.Sprintf("old/f%02d.txt", i), fmt.Sprint(i))
	}
	writeLocal(t, a, "keep.txt", "keep")
	qaSync(t, a)
	os.RemoveAll(filepath.Join(a.dir, "old"))
	if e := a.Sync(qaCtx); e == nil {
		t.Fatal("expected pause")
	}
	a.RetryRejected() // user inspected the disk and acknowledged
	flaky.failAfter = 5
	t.Logf("acknowledged sync with a network blip: %v", a.Sync(qaCtx))
	flaky.failAfter = -1
	e := a.Sync(qaCtx)
	if e != nil && strings.Contains(e.Error(), "deletes paused") {
		t.Fatalf("acknowledged mass removal paused again after a transient failure: %v; remote still has %d entries", e, len(qaRemote(t, base, f, k)))
	}
}

type qaFailingCommits struct {
	Client
	failAfter int
}

func (c *qaFailingCommits) Commit(ctx context.Context, id string, m []Mutation) (Delta, error) {
	if c.failAfter == 0 {
		return Delta{}, errors.New("connection reset by peer")
	}
	if c.failAfter > 0 {
		c.failAfter--
	}
	return c.Client.Commit(ctx, id, m)
}
