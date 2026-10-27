package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRescanReadsOnlyChangedFiles(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := replicaFor(t, c, f, k, "r", true)
	writeLocal(t, r, "kept", "same content")
	writeLocal(t, r, "edited", "before edit!")
	writeLocal(t, r, "removed", "gone soon")
	defer func(v time.Duration) { scanSettle = v }(scanSettle)
	scanSettle = -time.Hour // every file counts as settled

	first, e := r.ScanLocal()
	if e != nil {
		t.Fatal(e)
	}
	// A sentinel in the cache shows which files the next scan didn't read.
	for local, cached := range r.scanHashes {
		cached.hash = "cached"
		r.scanHashes[local] = cached
	}
	full := filepath.Join(r.dir, "edited")
	info, e := os.Stat(full)
	if e != nil {
		t.Fatal(e)
	}
	writeLocal(t, r, "edited", "after edit!!") // same size, same mtime below
	if e := os.Chtimes(full, info.ModTime(), info.ModTime()); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(filepath.Join(r.dir, "removed")); e != nil {
		t.Fatal(e)
	}

	second, e := r.ScanLocal()
	if e != nil {
		t.Fatal(e)
	}
	if second["kept"].Hash != "cached" {
		t.Fatalf("unchanged file was read again: %q", second["kept"].Hash)
	}
	if got := second["edited"].Hash; got == "cached" || got == first["edited"].Hash {
		t.Fatalf("edited file kept its old hash: %q", got)
	}
	if _, ok := r.scanHashes["removed"]; ok {
		t.Fatal("removed file stayed in the scan cache")
	}
}

func TestRescanReadsRecentlyWrittenFiles(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := replicaFor(t, c, f, k, "r", true)
	writeLocal(t, r, "fresh", "just written")
	if _, e := r.ScanLocal(); e != nil {
		t.Fatal(e)
	}
	if _, ok := r.scanHashes["fresh"]; ok {
		t.Fatal("a file written within the settle window was trusted from its signature")
	}
}
