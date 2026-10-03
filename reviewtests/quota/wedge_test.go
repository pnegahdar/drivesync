package quota

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// A writer that never received the folder key commits one row with garbage
// metadata. Every replica's Sync now fails before applying any later row, forever.
func TestKeylessWriterWedgesAllReplicas(t *testing.T) {
	s := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "o"}
	w := ds.Principal{Tenant: "t", Subject: "api-bot"} // writer by grant, no key
	oc := s.Client(owner)
	f, k := mkFolder(t, oc, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 1000})
	if e := oc.Grant(bg, f.ID, w, ds.Writer); e != nil {
		t.Fatal(e)
	}
	r, dir := attach(t, oc, f, k, "r")
	wc := s.Client(w)
	var b [32]byte
	rand.Read(b[:])
	pid := hex.EncodeToString(b[:])
	tk, e := wc.Reserve(bg, f.ID, ds.UploadRequest{PathID: pid})
	if e != nil {
		t.Fatal(e)
	}
	wc.Upload(bg, f.ID, tk, bytes.NewReader(nil))
	junk := make([]byte, 64)
	rand.Read(junk)
	if _, e = wc.Commit(bg, f.ID, []ds.Mutation{{PathID: pid, TicketID: tk.ID, Metadata: junk}}); e != nil {
		t.Fatal(e)
	}
	putFile(t, oc, f, k, "later-legit-file.txt", 0, []byte("hello"))
	for i := 0; i < 5; i++ {
		e = r.Sync(bg)
	}
	t.Logf("sync error after 5 attempts: %v; files=%v", e, files(t, dir))
	if _, ok := files(t, dir)["later-legit-file.txt"]; !ok {
		t.Errorf("BUG: one undecryptable row from a keyless writer blocks all later downloads")
	}
}

// A keyed peer (or a benign race) produces a regular file "x" and a file "x/y".
// Every other replica fails at "x/y" on every sync and never reaches later rows.
func TestFileAndChildWedges(t *testing.T) {
	s := newServer(t)
	owner := ds.Principal{Tenant: "t", Subject: "o"}
	oc := s.Client(owner)
	f, k := mkFolder(t, oc, ds.Limits{})
	r, dir := attach(t, oc, f, k, "r")
	putFile(t, oc, f, k, "x", 0, []byte("file"))
	putFile(t, oc, f, k, "x/y", 0, []byte("child"))
	putFile(t, oc, f, k, "zz/later.txt", 0, []byte("later"))
	var e error
	for i := 0; i < 5; i++ {
		e = r.Sync(bg)
	}
	t.Logf("sync error: %v; files=%v", e, files(t, dir))
	if _, ok := files(t, dir)["zz/later.txt"]; !ok {
		t.Errorf("BUG: replica permanently wedged by file/child shape")
	}
}
