//go:build darwin || linux

package engine

import (
	"os"
	"syscall"
)

func statSignature(info os.FileInfo) (fileSignature, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileSignature{}, false
	}
	mtime, ctime := statTimes(s)
	return fileSignature{dev: uint64(s.Dev), ino: uint64(s.Ino), size: info.Size(), mtime: mtime, ctime: ctime}, true
}
