package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRootFsyncRetry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "new-blob-root")
	saved := syncDirectoryFile
	defer func() { syncDirectoryFile = saved }()
	failure := errors.New("persistent parent fsync failure")
	calls := 0
	syncDirectoryFile = func(f *os.File) error {
		if f.Name() == filepath.Dir(root) {
			calls++
			return failure
		}
		return f.Sync()
	}
	first, e1 := OpenDirectoryBlobStore(root)
	if first != nil {
		first.Close()
	}
	if !errors.Is(e1, failure) {
		t.Fatal(e1)
	}
	if _, e := os.Stat(root); e != nil {
		t.Fatal("first attempt did not leave root for retry", e)
	}
	second, e2 := OpenDirectoryBlobStore(root)
	if second != nil {
		second.Close()
	}
	t.Logf("first=%v second=%v barrier calls=%d", e1, e2, calls)
	if calls != 2 {
		t.Fatal("parent barrier was not retried", calls)
	}
	if !errors.Is(e2, failure) {
		t.Fatal("retry acknowledged root whose parent barrier still fails")
	}
}
