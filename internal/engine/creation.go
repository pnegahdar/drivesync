package engine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"
)

type FolderChallenge struct {
	ID    string
	Proof []byte
}

// The stateless, principal-bound challenge keeps IDs authority-generated,
// without exposing an existence oracle or retaining unauthenticated drafts.
func (s *Server) PrepareFolder(ctx context.Context, p Principal) (FolderChallenge, error) {
	if !p.valid() {
		return FolderChallenge{}, ErrDenied
	}
	if e := ctx.Err(); e != nil {
		return FolderChallenge{}, e
	}
	v := FolderChallenge{ID: randomID(), Proof: make([]byte, 8)}
	binary.BigEndian.PutUint64(v.Proof, uint64(s.now().Add(5*time.Minute).UnixNano()))
	v.Proof = append(v.Proof, s.creationMAC(p, v.ID, v.Proof)...)
	return v, nil
}
func (s *Server) creationMAC(p Principal, id string, expiry []byte) []byte {
	h := hmac.New(sha256.New, s.wakes.creationKey[:])
	h.Write([]byte("drivesync/folder-creation/"))
	h.Write([]byte(principalKey(p)))
	h.Write([]byte(id))
	h.Write(expiry)
	return h.Sum(nil)
}
func (s *Server) validCreation(p Principal, spec FolderSpec) bool {
	return validID(spec.ID) && len(spec.CreationProof) == 40 && len(spec.KeyCheck) == 64 && hex.EncodeToString(spec.KeyCheck[:16]) == spec.ID && int64(binary.BigEndian.Uint64(spec.CreationProof[:8])) > s.now().UnixNano() && hmac.Equal(spec.CreationProof[8:], s.creationMAC(p, spec.ID, spec.CreationProof[:8]))
}

// CreateFolder computes the salted check only after the authority names the
// folder. The key remains in this client helper for both transports.
func CreateFolder(ctx context.Context, c Client, spec FolderSpec, k FolderKey) (Folder, error) {
	if k == (FolderKey{}) {
		return Folder{}, ErrKey
	}
	v, e := c.PrepareFolder(ctx)
	if e != nil {
		return Folder{}, e
	}
	spec.ID, spec.CreationProof = v.ID, v.Proof
	spec.KeyCheck = KeyCheck(k, v.ID)
	return c.CreateFolder(ctx, spec)
}
