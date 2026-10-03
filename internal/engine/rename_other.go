//go:build !darwin && !linux

package engine

import (
	"errors"
	"os"
)

func renameIfAbsent(root *os.Root, oldPath, newPath string) error {
	return errors.New("exclusive rename requires Linux or macOS")
}
