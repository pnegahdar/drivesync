package engine

import "time"

// scanSettle is how long a file must sit unchanged before its hash is
// trusted from its stat signature. A write in the same timestamp tick as the
// read could otherwise keep the old signature on filesystems with coarse
// timestamps.
var scanSettle = 2 * time.Second

// fileSignature is what a rescan compares before reading a file again: any
// write, truncation, rename over, or metadata change moves the change time.
type fileSignature struct {
	dev, ino     uint64
	size         int64
	mtime, ctime int64
}

type scanHash struct {
	sig  fileSignature
	hash string
}

func (s fileSignature) settledBefore(t time.Time) bool {
	limit := t.Add(-scanSettle).UnixNano()
	return s.mtime < limit && s.ctime < limit
}
