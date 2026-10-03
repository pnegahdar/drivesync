//go:build darwin

package engine

import "golang.org/x/sys/unix"

func renameExclusiveAt(oldfd int, oldname string, newfd int, newname string) error {
	return unix.RenameatxNp(oldfd, oldname, newfd, newname, unix.RENAME_EXCL)
}
