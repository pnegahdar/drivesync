//go:build darwin || linux

package engine

import (
	"os"
	"path/filepath"
	"syscall"
)

func lockState(path string) (*os.File, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}

func openLocal(root *os.Root, p string) (*os.File, error) {
	return root.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
}

// Exclusive root/shared ancestors serialize overlapping attachment admission
// across processes. Disjoint roots can still attach concurrently.
func attachmentAdmission(dir string) (func(), error) {
	var held []*os.File
	release := func() {
		for _, f := range held {
			f.Close()
		}
	}
	for p := dir; ; p = filepath.Dir(p) {
		f, e := os.Open(p)
		if e != nil {
			release()
			return nil, e
		}
		mode := syscall.LOCK_SH
		if p == dir {
			mode = syscall.LOCK_EX
		}
		if e = syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB); e != nil {
			f.Close()
			release()
			return nil, e
		}
		held = append(held, f)
		if p == filepath.Dir(p) {
			break
		}
	}
	return release, nil
}

func flockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
