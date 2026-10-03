package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/zeebo/blake3"
	"io"
	"strings"
	"testing"
)

func hashBytes(b []byte) string { s := blake3.Sum256(b); return fmt.Sprintf("%x", s[:]) }
func TestCryptoIntegrity(t *testing.T) {
	k := NewFolderKey()
	f, b := randomID(), randomID()
	pid, _ := PathID(k, f, "file")
	data := bytes.Repeat([]byte("x"), ChunkSize*2+19)
	var sealed bytes.Buffer
	if e := SealContent(&sealed, bytes.NewReader(data), k, f, b, pid); e != nil {
		t.Fatal(e)
	}
	orig := sealed.Bytes()
	if int64(len(orig)) != SealedSize(int64(len(data))) {
		t.Fatal("size")
	}
	var out bytes.Buffer
	if e := OpenContent(&out, bytes.NewReader(orig), k, f, b, pid); e != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatal(e)
	}
	firstEnd := 20 + 5 + int(binary.BigEndian.Uint32(orig[20:24]))
	secondEnd := firstEnd + 5 + int(binary.BigEndian.Uint32(orig[firstEnd:firstEnd+4]))
	reordered := append([]byte(nil), orig[:20]...)
	reordered = append(reordered, orig[firstEnd:secondEnd]...)
	reordered = append(reordered, orig[20:firstEnd]...)
	reordered = append(reordered, orig[secondEnd:]...)
	tamper := append([]byte(nil), orig...)
	tamper[30] ^= 1
	for name, blob := range map[string][]byte{"tampered": tamper, "truncated": orig[:len(orig)-21], "reordered": reordered, "extended": append(append([]byte(nil), orig...), 0)} {
		t.Run(name, func(t *testing.T) {
			expected := ErrIntegrity
			if name == "truncated" {
				expected = io.ErrUnexpectedEOF
			}
			if e := OpenContent(&bytes.Buffer{}, bytes.NewReader(blob), k, f, b, pid); e != expected {
				t.Fatal(e)
			}
		})
	}
	for _, args := range []struct {
		k       FolderKey
		f, b, p string
	}{{NewFolderKey(), f, b, pid}, {k, randomID(), b, pid}, {k, f, randomID(), pid}, {k, f, b, strings.Repeat("a", 64)}} {
		if e := OpenContent(&bytes.Buffer{}, bytes.NewReader(orig), args.k, args.f, args.b, args.p); e != ErrIntegrity {
			t.Fatal(e)
		}
	}
	if CheckKey(NewFolderKey(), KeyCheck(k)) != ErrKey {
		t.Fatal("key check")
	}
	m := FileMetadata{Path: "file", BlobID: b, Size: int64(len(data)), Mode: 0600, Hash: hashBytes(data)}
	meta, e := SealMetadata(k, f, pid, m)
	if e != nil {
		t.Fatal(e)
	}
	row := Row{FolderID: f, PathID: pid, BlobID: b, Metadata: meta}
	if _, e = OpenMetadata(k, f, row); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []Row{{FolderID: randomID(), PathID: pid, BlobID: b, Metadata: meta}, {FolderID: f, PathID: strings.Repeat("b", 64), BlobID: b, Metadata: meta}, {FolderID: f, PathID: pid, BlobID: randomID(), Metadata: meta}, {FolderID: f, PathID: pid, BlobID: b, Metadata: meta[:len(meta)-1]}} {
		if _, e = OpenMetadata(k, f, bad); e != ErrIntegrity {
			t.Fatal(e)
		}
	}
	if _, e = OpenMetadata(NewFolderKey(), f, row); e != ErrIntegrity {
		t.Fatal(e)
	}
}
func TestPaths(t *testing.T) {
	for _, p := range []string{"../x", "a/../x", "/abs", "a//b", "C:\\x", "x\x00y", "CON.txt", "a/NUL", "foo.", "a/./b", ".drivesyncignore", "a\\b", "a:stream", "<bad>"} {
		if _, e := NormalizePath(p); e == nil {
			t.Errorf("accepted %q", p)
		}
	}
	for _, p := range []string{"a", "a/b.txt", "é/你好.md"} {
		if n, e := NormalizePath(p); e != nil || n != p {
			t.Errorf("%q: %v", p, e)
		}
	}
}

func TestMetadataAEADCrossFolder(t *testing.T) {
	k := NewFolderKey()
	a, b := randomID(), randomID()
	pid, _ := PathID(k, a, "file")
	blob := randomID()
	meta, e := SealMetadata(k, a, pid, FileMetadata{Path: "file", BlobID: blob, Mode: 0600})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = OpenMetadata(k, b, Row{FolderID: b, PathID: pid, BlobID: blob, Metadata: meta}); e != ErrIntegrity {
		t.Fatal(e)
	}
}

func TestRepeatedBlobIDsUseIndependentStreamKeys(t *testing.T) {
	key := NewFolderKey()
	folder, blob := randomID(), randomID()
	pid, _ := PathID(key, folder, "file")
	var a, b bytes.Buffer
	if e := SealContent(&a, bytes.NewBufferString("one"), key, folder, blob, pid); e != nil {
		t.Fatal(e)
	}
	if e := SealContent(&b, bytes.NewBufferString("two"), key, folder, blob, pid); e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(a.Bytes()[:20], b.Bytes()[:20]) {
		t.Fatal("reused stream salt")
	}
	swapped := append([]byte(nil), a.Bytes()...)
	copy(swapped[:20], b.Bytes()[:20])
	if e := OpenContent(&bytes.Buffer{}, bytes.NewReader(swapped), key, folder, blob, pid); e != ErrIntegrity {
		t.Fatal(e)
	}
}
