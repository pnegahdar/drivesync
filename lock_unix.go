//go:build darwin || linux

package drivesync

import (
	"os"
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
	return root.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}
