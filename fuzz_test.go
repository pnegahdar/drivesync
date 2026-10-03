package drivesync

import (
	"bytes"
	"github.com/pnegahdar/drivesync/internal/engine"
	"strings"
	"testing"
)

func FuzzNormalizePath(f *testing.F) {
	for _, p := range []string{"a/b.txt", "../x", "a\\b", "/abs", "a\x00b", "NUL"} {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, p string) {
		n, e := engine.NormalizePath(p)
		if e == nil {
			again, e := engine.NormalizePath(n)
			if e != nil || again != n {
				t.Fatal("not idempotent")
			}
		}
	})
}
func FuzzWireDecode(f *testing.F) {
	f.Add([]byte(`{"Op":"get"}`))
	f.Add([]byte(`{} {}`))
	f.Fuzz(func(t *testing.T, b []byte) { var r engine.WireRequest; _ = engine.DecodeWire(bytes.NewReader(b), &r) })
}
func FuzzOpenContent(f *testing.F) {
	f.Add([]byte("DSC\x01bad"))
	k := engine.FolderKey{}
	id := strings.Repeat("a", 32)
	pid := strings.Repeat("b", 64)
	var seed bytes.Buffer
	_ = engine.SealContent(&seed, strings.NewReader("hello"), k, id, id, pid)
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		_ = engine.OpenContent(&bytes.Buffer{}, bytes.NewReader(b), k, id, id, pid)
	})
}
