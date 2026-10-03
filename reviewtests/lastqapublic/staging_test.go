package qapublic

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// Every upload stages a full plaintext copy in StateDir. If that copy fails
// (ENOSPC/EDQUOT/EFBIG for one large file), sync() returns immediately: every
// later upload and every local delete is skipped, on every sync, forever.
// RLIMIT_FSIZE stands in for "StateDir's filesystem has less free space than
// this file". Run this test alone (it changes a process-wide limit).
func TestOneUnstageableFileBlocksAllLaterUploadsAndDeletes(t *testing.T) {
	if os.Getenv("QA_RLIMIT") == "" {
		t.Skip("set QA_RLIMIT=1 and run alone: changes a process-wide rlimit")
	}
	s := newServer(t)
	a := s.Client(alice)
	key := drivesync.NewFolderKey()
	f, e := a.CreateFolder(bg, drivesync.FolderSpec{Name: "w"}, key)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "w")
	r, e := drivesync.Attach(bg, a, f.ID, key, dir, drivesync.Options{Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	_ = os.WriteFile(filepath.Join(dir, "c-old.txt"), []byte("to be deleted"), 0600)
	if e = r.Sync(bg); e != nil {
		t.Fatal(e)
	}
	_ = os.WriteFile(filepath.Join(dir, "a-video.mov"), bytes.Repeat([]byte("v"), 8<<20), 0600)
	_ = os.WriteFile(filepath.Join(dir, "b-notes.txt"), []byte("small and important"), 0600)
	_ = os.Remove(filepath.Join(dir, "c-old.txt"))

	var old syscall.Rlimit
	_ = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &old)
	lim := old
	lim.Cur = 4 << 20
	if e = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &lim); e != nil {
		t.Skip(e)
	}
	for i := 0; i < 3; i++ {
		e = r.Sync(bg)
	}
	_ = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &old)
	peerDir := filepath.Join(t.TempDir(), "peer")
	peer, pe := drivesync.Attach(bg, s.Client(alice), f.ID, key, peerDir, drivesync.Options{Manual: true})
	if pe != nil {
		t.Fatal(pe)
	}
	defer peer.Close()
	_ = peer.Sync(bg)
	_, hasNotes := os.Stat(filepath.Join(peerDir, "b-notes.txt"))
	_, hasOld := os.Stat(filepath.Join(peerDir, "c-old.txt"))
	t.Logf("last sync error: %v; peer has b-notes.txt=%v, deleted c-old.txt still present=%v", e, hasNotes == nil, hasOld == nil)
	// Expect b-notes.txt uploaded and c-old.txt deleted despite a-video.mov failing.
	if hasNotes != nil || hasOld == nil {
		t.Fatalf("one unstageable file blocked unrelated upload/delete on every sync")
	}
}
