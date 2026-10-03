package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplicaQAIgnoreAnchoringAndUnsupportedRules(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "root"), "r")
	writeLocal(t, r, ".drivesyncignore", "/secret\n**/.env\n!unsupported\n")
	writeLocal(t, r, "secret", "root secret")
	writeLocal(t, r, "sub/secret", "shared")
	writeLocal(t, r, ".env", "secret")
	writeLocal(t, r, "sub/deep/.env", "secret")
	if e := r.Sync(qaCtx); e != nil {
		t.Fatal(e)
	}
	got := qaRemote(t, c, f, k)
	if got["sub/secret"] != "shared" || got["secret"] != "" || got[".env"] != "" || got["sub/deep/.env"] != "" {
		t.Fatal(got)
	}
	reported := false
	for _, line := range r.Status().Skipped {
		if strings.Contains(line, "unsupported ignore rule") {
			reported = true
		}
	}
	if !reported {
		t.Fatal("unsupported rule was silent")
	}
}

func TestReplicaQARetrySurvivesFailedDeleteBatch(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	client := &qaFailingCommits{Client: base, failAfter: -1}
	r := qaAttach(t, client, f, k, filepath.Join(t.TempDir(), "root"), "r")
	for i := 0; i < 6; i++ {
		writeLocal(t, r, string(rune('a'+i)), "content")
	}
	qaSync(t, r)
	for i := 0; i < 6; i++ {
		os.Remove(filepath.Join(r.dir, string(rune('a'+i))))
	}
	if r.Sync(qaCtx) == nil {
		t.Fatal("expected mass-removal guard")
	}
	r.RetryRejected()
	client.failAfter = 0
	if r.Sync(qaCtx) == nil {
		t.Fatal("expected failed commit")
	}
	client.failAfter = -1
	if e := r.Sync(qaCtx); e != nil {
		t.Fatal("acknowledgement lost", e)
	}
	got, e := base.GetFolder(qaCtx, f.ID)
	if e != nil || got.Usage.Files != 0 {
		t.Fatal(got, e)
	}
}
