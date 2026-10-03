// Package drivesync synchronizes encrypted shared folders to ordinary local directories.
package drivesync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"
)

type Principal struct{ Tenant, Subject string }

func (p Principal) valid() bool {
	return p.Tenant != "" && p.Subject != "" && len(p.Tenant) <= 256 && len(p.Subject) <= 256 && utf8.ValidString(p.Tenant) && utf8.ValidString(p.Subject)
}

type Role string

const (
	Owner  Role = "owner"
	Writer Role = "writer"
	Reader Role = "reader"
)

var (
	ErrDenied    = errors.New("not found or access denied")
	ErrBusy      = errors.New("path has an active upload")
	ErrExpired   = errors.New("upload reservation expired")
	ErrQuota     = errors.New("owner quota limit")
	ErrConflict  = errors.New("version conflict")
	ErrInvalid   = errors.New("invalid request")
	ErrKey       = errors.New("incorrect folder key")
	ErrIntegrity = errors.New("encrypted data integrity failure")
)

type LimitError struct {
	Limit              string
	Maximum, Requested int64
}

func (e *LimitError) Error() string {
	return fmt.Sprintf("%s limit: %d requested, %d limit", e.Limit, e.Requested, e.Maximum)
}

type Limits struct{ MaxFileBytes, MaxTotalBytes, MaxFiles, MaxRows int64 }
type Quota struct{ MaxTotalBytes, MaxFolders, MaxFileBytes, MaxFiles int64 }

// Zero limits mean unlimited; negative values are invalid. Bytes are sealed bytes.
type QuotaPolicy interface {
	Quota(context.Context, Principal) (Quota, error)
}
type QuotaFunc func(context.Context, Principal) (Quota, error)

func (f QuotaFunc) Quota(c context.Context, p Principal) (Quota, error) { return f(c, p) }

type Usage struct{ Bytes, Files, Rows, Reserved, ReservedFiles, ReservedRows, GarbageRows int64 }
type FolderSpec struct {
	Name, Description string
	Limits            Limits
	KeyCheck          []byte
}
type Folder struct {
	ID                string
	Owner             Principal
	Name, Description string
	Limits            Limits
	KeyCheck          []byte
	Version           uint64
	Usage             Usage
	Role              Role
}
type Row struct {
	FolderID, PathID, BlobID string
	Version                  uint64
	SealedSize               int64
	Metadata                 []byte
	Deleted                  bool
}
type UploadRequest struct {
	PathID        string
	BaseVersion   uint64
	SealedSize    int64
	MetadataBytes int64
	// Deletes are CAS-bound rename credits, committed atomically with this upload.
	Deletes []Mutation
}
type Ticket struct {
	ID, FolderID, BlobID, PathID             string
	Principal                                Principal
	BaseVersion                              uint64
	SealedSize, ReservedBytes, ReservedFiles int64
	ReservedRows                             int64
	Expires                                  time.Time
	Uploaded                                 bool
	Writing                                  bool
	Deletes                                  []Mutation
}
type Mutation struct {
	PathID      string
	BaseVersion uint64
	TicketID    string
	Metadata    []byte
	Deleted     bool
}
type Delta struct {
	Next    string `json:",omitempty"`
	Version uint64
	Rows    []Row
}

// ConflictError identifies only the paths whose bases are stale.
type ConflictError struct{ Paths []string }

func (e *ConflictError) Error() string { return ErrConflict.Error() }
func (e *ConflictError) Unwrap() error { return ErrConflict }

const RowCost int64 = 256
const MaxRequestBytes int64 = 1 << 50
const MaxGrants = 256

type Event struct {
	Version uint64
	Err     error
}

// Client is an authenticated transport. Server authorization is independent of transport.
type Client interface {
	CreateFolder(context.Context, FolderSpec) (Folder, error)
	Grant(context.Context, string, Principal, Role) error
	Revoke(context.Context, string, Principal) error
	DeleteFolder(context.Context, string) error
	SetLimits(context.Context, string, Limits) error
	ListFolders(context.Context) ([]Folder, error)
	GetFolder(context.Context, string) (Folder, error)
	Reserve(context.Context, string, UploadRequest) (Ticket, error)
	Upload(context.Context, string, Ticket, io.Reader) error
	CancelUpload(context.Context, string, string) error
	Commit(context.Context, string, []Mutation) (Delta, error)
	Changes(context.Context, string, uint64) (Delta, error)
	Download(context.Context, string, string) (io.ReadCloser, error)
	Wait(context.Context, string, uint64) (uint64, error)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func validID(s string) bool     { b, e := hex.DecodeString(s); return e == nil && len(b) == 16 }
func validPathID(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
func validLimits(l Limits) bool {
	return l.MaxFileBytes >= 0 && l.MaxTotalBytes >= 0 && l.MaxFiles >= 0 && l.MaxRows >= 0
}
func minimum(a, b int64) int64 {
	if a == 0 {
		return b
	}
	if b == 0 || a < b {
		return a
	}
	return b
}
