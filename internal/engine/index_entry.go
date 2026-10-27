package engine

import (
	"bytes"
	"encoding/hex"
)

func equalDigest(sum []byte, hash [32]byte) bool { return bytes.Equal(sum, hash[:]) }

// indexHash converts hashes which were already validated at the metadata
// boundary. Directory hashes and cleared tombstones have no digest.
func indexHash(s string) [32]byte {
	h, _ := parseHash(s)
	return h
}

func (i IndexEntry) hashString() string {
	if i.Directory {
		return "directory"
	}
	return hex.EncodeToString(i.Hash[:])
}

// contentRow reconstructs only the transfer identity. The original remote path
// remains the AEAD binding even when hydration publishes a moved placeholder.
func (r *Replica) contentRow(i IndexEntry) Row {
	pid, _ := PathID(r.key, r.folder, i.Path)
	return Row{FolderID: r.folder, PathID: pid, BlobID: hex.EncodeToString(i.Blob[:]), Version: i.Version, SealedSize: SealedSize(i.Size)}
}

func (i IndexEntry) metadata() FileMetadata {
	return FileMetadata{Path: i.Path, BlobID: hex.EncodeToString(i.Blob[:]), Hash: i.hashString(), Size: i.Size, Mode: i.Mode, Directory: i.Directory}
}
