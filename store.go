package drivesync

import (
	"context"
	"database/sql"
	"io"

	"github.com/pnegahdar/drivesync/internal/engine"
)

// metaStore is the private authority store. SQLite and Postgres both implement it.
type metaStore interface {
	engine.MetaStore
	Close() error
}

// MetaStore is the authority metadata store. Metadata storage is concrete because
// a custom transaction interface would expose private replication/accounting
// records. BlobStore and QuotaPolicy remain the pluggable extension points.
type MetaStore struct{ store metaStore }

func OpenSQLiteMetaStore(path string) (*MetaStore, error) {
	s, e := engine.OpenSQLiteMetaStore(path)
	if e != nil {
		return nil, e
	}
	return &MetaStore{store: s}, nil
}

// OpenPostgresMetaStore uses db, which the caller owns and must open with the
// pgx stdlib driver (driver name "pgx"). schema is created when missing; an
// empty schema name uses "drivesync". Close does not close db. Several
// authority processes may share one schema. db must allow at least three
// connections: one stays on LISTEN and two remain for transactions. A limit
// of one or two is rejected; unlimited (zero) is accepted. Garbage collection
// claims a lease row in the schema (a holder id and an expiry, renewed during
// the pass) and does not hold a pooled connection. A frozen holder stops
// blocking after the lease expires.
func OpenPostgresMetaStore(ctx context.Context, db *sql.DB, schema string) (*MetaStore, error) {
	s, e := engine.OpenPostgresMetaStore(ctx, db, schema)
	if e != nil {
		return nil, e
	}
	return &MetaStore{store: s}, nil
}

func (m *MetaStore) Close() error { return m.store.Close() }

// BlobStore holds immutable sealed blobs. Keys are authority-generated IDs.
// Put streams bytes, Size reports actual stored bytes, and Delete is idempotent
// (os.ErrNotExist is also accepted). Implementations must honor cancellation.
// Put must publish only after clean EOF; a reader error must discard staging.
type BlobStore interface {
	Put(context.Context, string, string, io.Reader) (int64, error)
	Open(context.Context, string, string) (io.ReadCloser, error)
	Size(context.Context, string, string) (int64, error)
	Delete(context.Context, string, string) error
}

// MemoryBlobStore is useful for tests and small in-process deployments.
type MemoryBlobStore struct{ store *engine.MemoryBlobStore }

func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{store: engine.NewMemoryBlobStore()}
}
func (b *MemoryBlobStore) Put(ctx context.Context, folder, id string, r io.Reader) (int64, error) {
	return b.store.Put(ctx, folder, id, r)
}
func (b *MemoryBlobStore) Open(ctx context.Context, folder, id string) (io.ReadCloser, error) {
	return b.store.Open(ctx, folder, id)
}
func (b *MemoryBlobStore) Size(ctx context.Context, folder, id string) (int64, error) {
	return b.store.Size(ctx, folder, id)
}
func (b *MemoryBlobStore) Delete(ctx context.Context, folder, id string) error {
	return b.store.Delete(ctx, folder, id)
}

// DirectoryBlobStore streams sealed bytes to a durable local directory.
type DirectoryBlobStore struct{ store *engine.DirectoryBlobStore }

func OpenDirectoryBlobStore(path string) (*DirectoryBlobStore, error) {
	b, e := engine.OpenDirectoryBlobStore(path)
	if e != nil {
		return nil, e
	}
	return &DirectoryBlobStore{store: b}, nil
}
func (b *DirectoryBlobStore) Close() error { return b.store.Close() }
func (b *DirectoryBlobStore) Put(ctx context.Context, folder, id string, r io.Reader) (int64, error) {
	return b.store.Put(ctx, folder, id, r)
}
func (b *DirectoryBlobStore) Open(ctx context.Context, folder, id string) (io.ReadCloser, error) {
	return b.store.Open(ctx, folder, id)
}
func (b *DirectoryBlobStore) Size(ctx context.Context, folder, id string) (int64, error) {
	return b.store.Size(ctx, folder, id)
}
func (b *DirectoryBlobStore) Delete(ctx context.Context, folder, id string) error {
	return b.store.Delete(ctx, folder, id)
}
