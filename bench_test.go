package drivesync

import (
	"context"
	"fmt"
	"github.com/pnegahdar/drivesync/internal/engine"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkSmallFilePropagation(b *testing.B) {
	s := apiServer(b, ServerOptions{})
	c := s.Client(Principal{"tenant", "owner"})
	k := NewFolderKey()
	f, e := c.CreateFolder(context.Background(), FolderSpec{Name: "bench"}, k)
	if e != nil {
		b.Fatal(e)
	}
	a, r := apiReplica(b, c, f, k, "a", false), apiReplica(b, c, f, k, "b", false)
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		token := fmt.Sprint(j)
		apiWrite(b, a, "file", token)
		deadline := time.Now().Add(5 * time.Second)
		for {
			data, _ := os.ReadFile(filepath.Join(r.dir, "file"))
			if string(data) == token {
				break
			}
			if time.Now().After(deadline) {
				b.Fatal("timeout")
			}
			time.Sleep(time.Millisecond)
		}
	}
}
func scanBenchmark(b *testing.B, indexed bool) {
	s := apiServer(b, ServerOptions{})
	c := s.Client(Principal{"tenant", "owner"})
	k := NewFolderKey()
	f, e := c.CreateFolder(context.Background(), FolderSpec{Name: "bench"}, k)
	if e != nil {
		b.Fatal(e)
	}
	r := apiReplica(b, c, f, k, "r", true)
	for j := 0; j < 10000; j++ {
		dir := fmt.Sprintf("dir%d", j/100)
		if e = os.MkdirAll(filepath.Join(r.dir, dir), 0700); e != nil {
			b.Fatal(e)
		}
		apiWrite(b, r, fmt.Sprintf("%s/file%d", dir, j), "small file")
	}
	if indexed {
		files, e := r.replica.ScanLocal()
		if e != nil {
			b.Fatal(e)
		}
		for _, v := range files {
			r.replica.Remember(engine.IndexEntry{Path: v.Path, Local: v.Local, Hash: v.Hash, Directory: v.Directory, Mode: v.Mode, Version: 1})
		}
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		files, e := r.replica.ScanLocal()
		if e != nil || len(files) != 10100 {
			b.Fatal(len(files), e)
		}
	}
}
func BenchmarkScan10K(b *testing.B)        { scanBenchmark(b, false) }
func BenchmarkIndexedScan10K(b *testing.B) { scanBenchmark(b, true) }
