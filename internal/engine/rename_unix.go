//go:build darwin || linux

package engine

import (
	"os"
	"path"
	"strings"
)

// renameIfAbsent publishes old onto new only when new is not already there.
// The last check and the rename are one syscall, so a file that appears in
// between is left in place.
func renameIfAbsent(root *os.Root, oldPath, newPath string) error {
	oldDir, oldName := splitRoot(oldPath)
	newDir, newName := splitRoot(newPath)
	of, e := root.Open(oldDir)
	if e != nil {
		return e
	}
	defer of.Close()
	nf := of
	if newDir != oldDir {
		nf, e = root.Open(newDir)
		if e != nil {
			return e
		}
		defer nf.Close()
	}
	return renameExclusiveAt(int(of.Fd()), oldName, int(nf.Fd()), newName)
}

func splitRoot(p string) (string, string) {
	dir, name := path.Split(p)
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		dir = "."
	}
	return dir, name
}
