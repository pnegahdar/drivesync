// Package drivesync shares encrypted folders through ordinary local directories.
// Create a folder with a name and a key, then attach it wherever files are needed.
package drivesync

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pnegahdar/drivesync/internal/engine"
)

// Principal is an opaque identity supplied by the embedding authenticator.
type Principal struct{ Tenant, Subject string }

type Role string

const (
	Owner  Role = "owner"
	Writer Role = "writer"
	Reader Role = "reader"
)

var (
	ErrDenied    = engine.ErrDenied
	ErrBusy      = engine.ErrBusy
	ErrExpired   = engine.ErrExpired
	ErrQuota     = engine.ErrQuota
	ErrConflict  = engine.ErrConflict
	ErrInvalid   = engine.ErrInvalid
	ErrKey       = engine.ErrKey
	ErrIntegrity = engine.ErrIntegrity
	ErrWaitLimit = engine.ErrWaitLimit
)

// LimitError identifies a folder limit that rejected an operation.
type LimitError struct {
	Limit              string
	Maximum, Requested int64
	cause              error
}

func (e *LimitError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s limit: %d requested, %d limit", e.Limit, e.Requested, e.Maximum)
}
func (e *LimitError) Unwrap() error { return e.cause }

// Limits bounds sealed bytes, live entries and retained rows. Zero is unlimited
// except MaxRows, which selects a bounded safety budget. Sharing requires explicit
// positive byte, row and file caps. Bytes include encryption and metadata overhead.
type Limits struct{ MaxFileBytes, MaxTotalBytes, MaxFiles, MaxRows int64 }

// Quota is an owner's plan. MaxFiles bounds retained rows across their folders.
type Quota struct{ MaxTotalBytes, MaxFolders, MaxFileBytes, MaxFiles int64 }
type QuotaPolicy interface {
	Quota(context.Context, Principal) (Quota, error)
}
type QuotaFunc func(context.Context, Principal) (Quota, error)

func (f QuotaFunc) Quota(ctx context.Context, p Principal) (Quota, error) { return f(ctx, p) }

type FolderKey [32]byte

func NewFolderKey() FolderKey { return FolderKey(engine.NewFolderKey()) }

// String is the 64-character hex form ParseFolderKey accepts.
func (k FolderKey) String() string { return hex.EncodeToString(k[:]) }

// ParseFolderKey imports a 64-character hex encoding of a random 256-bit key.
// Passphrases and hashes of passphrases are not suitable folder keys.
func ParseFolderKey(s string) (FolderKey, error) {
	var k FolderKey
	b, e := hex.DecodeString(s)
	if e != nil || len(b) != len(k) {
		return k, ErrKey
	}
	copy(k[:], b)
	if k == (FolderKey{}) {
		return k, ErrKey
	}
	return k, nil
}

type Usage struct{ Bytes, Files, Reserved int64 }
type FolderSpec struct {
	Name, Description string
	Limits            Limits
}
type Folder struct {
	ID                string
	Name, Description string
	Owner             Principal
	Role              Role
	Limits            Limits
	Usage             Usage
}

// Options configures a local replica. StateDir must be dedicated and outside
// all synced trees; its default is a folder/root directory in the user's cache.
// Manual disables automatic sync; Sync remains available explicitly.
type Options struct {
	Name, StateDir                          string
	Debounce, RescanInterval, RetryInterval time.Duration
	Ignore                                  []string
	Manual                                  bool
	// Moved tells Attach this directory is the same root renamed on the same
	// volume. A matching marker token and inode keep the replica index.
	Moved bool
}
type Rejection struct{ Path, Reason string }
type Status struct {
	PendingUpBytes, PendingDownBytes int64
	Conflicts                        []string
	Rejected                         []Rejection
	Quarantined                      []string
	LastSync                         time.Time
	Errors                           []string
}
