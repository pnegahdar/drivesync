//go:build !darwin && !linux

package drivesync

import (
	"errors"
	"os"
)

func lockState(path string) (*os.File, error) {
	return nil, errors.New("replica file locking requires Linux or macOS")
}

func openLocal(root *os.Root, p string) (*os.File, error) { return root.Open(p) }
