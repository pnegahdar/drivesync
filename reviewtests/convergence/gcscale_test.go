package convergence

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// CollectGarbage's global transaction calls Metadata.Finish, which recomputes
// contributions(m) (every folder x every ticket/garbage record) once per owner
// key: O(owners x folders x (tickets+garbage)), plus a json.Marshal per folder
// per owner. It holds the single SQLite writer lock the whole time, so every
// tenant's requests wait on it, and once it exceeds the 5 s transaction
// deadline GC never completes: garbage stays charged and deleted folders stay.
func TestGCScalesQuadraticallyWithOwners(t *testing.T) {
	n := 4000
	if v := os.Getenv("GC_OWNERS"); v != "" {
		n, _ = strconv.Atoi(v)
	}
	s, m := newServer(t)
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, vk := mkFolder(t, victim, ds.Limits{})
	seedOwners(t, s, m, n, nil)
	g := 0
	if v := os.Getenv("GC_GARBAGE"); v != "" {
		g, _ = strconv.Atoi(v)
	}
	for i := 0; i < g; i++ {
		pid, _ := ds.PathID(vk, vf.ID, fmt.Sprintf("junk/%d", i))
		tk, e := victim.Reserve(bg, vf.ID, ds.UploadRequest{PathID: pid, SealedSize: 1})
		if e != nil {
			t.Fatal(e)
		}
		if e = victim.Upload(bg, vf.ID, tk, bytesReader([]byte{1})); e != nil {
			t.Fatal(e)
		}
		if e = victim.CancelUpload(bg, vf.ID, tk.ID); e != nil {
			t.Fatal(e)
		}
	}
	// The victim overwrites a file, leaving one garbage blob that should be freed.
	row := putFile(t, victim, vf, vk, "a.txt", 0, []byte("one"))
	putFile(t, victim, vf, vk, "a.txt", row.Version, []byte("two"))
	before, _ := victim.GetFolder(bg, vf.ID)

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_ = s.CollectGarbage(bg)
		done <- time.Since(start)
	}()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	if _, e := victim.GetFolder(bg, vf.ID); e != nil {
		t.Log("victim GetFolder during GC:", e)
	}
	blocked := time.Since(start)
	gc := <-done
	after, _ := victim.GetFolder(bg, vf.ID)
	t.Logf("garbage=%d owners=%d gc=%v victimGetFolderDuringGC=%v garbageRows before=%d after=%d ", g, n, gc, blocked, before.Usage.GarbageRows, after.Usage.GarbageRows)
	if after.Usage.GarbageRows != 0 {
		t.Fatalf("GC could not collect the victim's garbage with %d unrelated owners (gc took %v)", n, gc)
	}
	if blocked > time.Second {
		t.Fatalf("unrelated tenants' folder count made a victim request wait %v", blocked)
	}
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
