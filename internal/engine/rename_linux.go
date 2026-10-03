//go:build linux

package engine

import "golang.org/x/sys/unix"

func renameExclusiveAt(oldfd int, oldname string, newfd int, newname string) error {
	return unix.Renameat2(oldfd, oldname, newfd, newname, unix.RENAME_NOREPLACE)
}
