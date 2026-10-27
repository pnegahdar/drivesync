//go:build !darwin && !linux

package engine

import "os"

// Without a change time a signature can't prove a file is unchanged, so every
// scan reads it.
func statSignature(os.FileInfo) (fileSignature, bool) { return fileSignature{}, false }
