package qa

import (
	"bytes"
	"crypto/sha256"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Regression for deterministic checks linking folders. Salt removes that link.
// A correct candidate must still verify: checks and encrypted files inherently
// permit offline guessing. The public API requires random keys, not passphrases.
func TestKeyCheckDoesNotLinkFolders(t *testing.T) {
	s, _, _ := newServer(t)
	a := s.Client(alice)
	key := ds.FolderKey(sha256.Sum256([]byte("correct horse battery staple")))
	var checks [][]byte
	for i := 0; i < 2; i++ {
		f, e := ds.CreateFolder(bg, a, ds.FolderSpec{Name: string(rune('a' + i))}, key)
		if e != nil {
			t.Fatal(e)
		}
		g, _ := a.GetFolder(bg, f.ID)
		checks = append(checks, g.KeyCheck)
	}
	// What the authority (or a DB thief) can do: test candidate passphrases.
	guess := ds.FolderKey(sha256.Sum256([]byte("correct horse battery staple")))
	if ds.CheckKey(guess, checks[0]) != nil {
		t.Fatal("correct key rejected")
	}
	if bytes.Equal(checks[0], checks[1]) {
		t.Fatalf("authority can link folders sharing a key (equal=%v)", bytes.Equal(checks[0], checks[1]))
	}
}
