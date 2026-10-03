package drivesync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type FolderRecord struct {
	Folder Folder
	Grants map[string]Role
}
type Metadata struct {
	Folders map[string]FolderRecord
	Files   map[string]map[string]Row
	Tickets map[string]Ticket
}

func newMetadata() *Metadata {
	return &Metadata{Folders: map[string]FolderRecord{}, Files: map[string]map[string]Row{}, Tickets: map[string]Ticket{}}
}

// MetaStore serializes transactions across all server instances sharing it. A callback's
// error rolls back every change. Implementations must never expose state after a callback.
type MetaStore interface {
	Transaction(context.Context, func(*Metadata) error) error
}

// SQLiteMetaStore stores one durable SQL row per folder, file and reservation.
type SQLiteMetaStore struct{ db *sql.DB }

func OpenSQLiteMetaStore(name string) (*SQLiteMetaStore, error) {
	db, e := sql.Open("sqlite", name)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=10000", "PRAGMA synchronous=FULL", `CREATE TABLE IF NOT EXISTS folders (id TEXT PRIMARY KEY, data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS files (folder TEXT NOT NULL, path TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,path))`, `CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, data BLOB NOT NULL)`, `CREATE INDEX IF NOT EXISTS files_blob ON files(folder,json_extract(data,'$.BlobID'))`} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	return &SQLiteMetaStore{db: db}, nil
}
func (s *SQLiteMetaStore) Close() error { return s.db.Close() }
func (s *SQLiteMetaStore) Transaction(ctx context.Context, fn func(*Metadata) error) error {
	// BEGIN's first write acquires the SQLite writer lock before reading any accounting.
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "UPDATE folders SET id=id WHERE 0"); e != nil {
		return e
	}
	m := newMetadata()
	old := map[string]map[string][]byte{}
	for _, table := range []string{"folders", "files", "tickets"} {
		old[table] = map[string][]byte{}
		q := "SELECT id,data FROM " + table
		if table == "files" {
			q = "SELECT folder || '/' || path,data FROM files"
		}
		rs, e := tx.QueryContext(ctx, q)
		if e != nil {
			return e
		}
		for rs.Next() {
			var id string
			var data []byte
			if e = rs.Scan(&id, &data); e != nil {
				rs.Close()
				return e
			}
			old[table][id] = data
			switch table {
			case "folders":
				var v FolderRecord
				if e = json.Unmarshal(data, &v); e == nil {
					m.Folders[id] = v
					if m.Files[id] == nil {
						m.Files[id] = map[string]Row{}
					}
				}
			case "tickets":
				var v Ticket
				if e = json.Unmarshal(data, &v); e == nil {
					m.Tickets[id] = v
				}
			case "files":
				var v Row
				if e = json.Unmarshal(data, &v); e == nil {
					if m.Files[v.FolderID] == nil {
						m.Files[v.FolderID] = map[string]Row{}
					}
					m.Files[v.FolderID][v.PathID] = v
				}
			}
			if e != nil {
				rs.Close()
				return e
			}
		}
		e = rs.Err()
		rs.Close()
		if e != nil {
			return e
		}
	}
	if e = fn(m); e != nil {
		return e
	}
	next := map[string]map[string][]byte{"folders": {}, "files": {}, "tickets": {}}
	for id, v := range m.Folders {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		next["folders"][id] = b
	}
	for f, rows := range m.Files {
		for p, v := range rows {
			b, e := json.Marshal(v)
			if e != nil {
				return e
			}
			next["files"][f+"/"+p] = b
		}
	}
	for id, v := range m.Tickets {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		next["tickets"][id] = b
	}
	for _, table := range []string{"folders", "files", "tickets"} {
		for id, b := range next[table] {
			if bytes.Equal(b, old[table][id]) {
				continue
			}
			if table == "files" {
				if _, e = tx.ExecContext(ctx, "INSERT INTO files(folder,path,data) VALUES(?,?,?) ON CONFLICT(folder,path) DO UPDATE SET data=excluded.data", id[:32], id[33:], b); e != nil {
					return e
				}
			} else {
				if _, e = tx.ExecContext(ctx, "INSERT INTO "+table+"(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", id, b); e != nil {
					return e
				}
			}
		}
		for id := range old[table] {
			if _, ok := next[table][id]; ok {
				continue
			}
			if table == "files" {
				_, e = tx.ExecContext(ctx, "DELETE FROM files WHERE folder=? AND path=?", id[:32], id[33:])
			} else {
				_, e = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id=?", id)
			}
			if e != nil {
				return e
			}
		}
	}
	return tx.Commit()
}

// BlobStore must create immutable objects, report actual stored sizes, and honor
// context cancellation. Objects are identified by server-generated folder/blob IDs.
// A presigned implementation can implement the same contract without an SDK here.
type BlobStore interface {
	Put(context.Context, string, string, io.Reader) (int64, error)
	Open(context.Context, string, string) (io.ReadCloser, error)
	Size(context.Context, string, string) (int64, error)
	Delete(context.Context, string, string) error
}
type MemoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

func NewMemoryBlobStore() *MemoryBlobStore { return &MemoryBlobStore{blobs: map[string][]byte{}} }
func (b *MemoryBlobStore) Put(ctx context.Context, f, id string, r io.Reader) (int64, error) {
	if _, e := blobPath(f, id); e != nil {
		return 0, e
	}
	p, e := io.ReadAll(&contextReader{ctx, r})
	if e != nil {
		return 0, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := f + "/" + id
	if _, ok := b.blobs[key]; ok {
		return 0, ErrInvalid
	}
	b.blobs[key] = p
	return int64(len(p)), nil
}
func (b *MemoryBlobStore) Open(ctx context.Context, f, id string) (io.ReadCloser, error) {
	if _, e := blobPath(f, id); e != nil {
		return nil, e
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.blobs[f+"/"+id]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(v)), nil
}
func (b *MemoryBlobStore) Size(ctx context.Context, f, id string) (int64, error) {
	if _, e := blobPath(f, id); e != nil {
		return 0, e
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.blobs[f+"/"+id]
	if !ok {
		return 0, os.ErrNotExist
	}
	return int64(len(v)), ctx.Err()
}
func (b *MemoryBlobStore) Delete(ctx context.Context, f, id string) error {
	if _, e := blobPath(f, id); e != nil {
		return e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.blobs, f+"/"+id)
	return ctx.Err()
}

type DirectoryBlobStore struct{ root *os.Root }

func OpenDirectoryBlobStore(dir string) (*DirectoryBlobStore, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	r, e := os.OpenRoot(dir)
	if e != nil {
		return nil, e
	}
	return &DirectoryBlobStore{r}, nil
}
func (b *DirectoryBlobStore) Close() error { return b.root.Close() }
func blobPath(f, id string) (string, error) {
	if !validID(f) || !validID(id) {
		return "", ErrInvalid
	}
	return filepath.Join(f, id), nil
}
func (b *DirectoryBlobStore) Put(ctx context.Context, f, id string, r io.Reader) (int64, error) {
	p, e := blobPath(f, id)
	if e != nil {
		return 0, e
	}
	if e = b.root.Mkdir(f, 0700); e != nil && !errors.Is(e, os.ErrExist) {
		return 0, e
	}
	temp := filepath.Join(f, ".upload-"+randomID())
	w, e := b.root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return 0, e
	}
	defer b.root.Remove(temp)
	n, e := io.Copy(w, &contextReader{ctx, r})
	if e == nil {
		e = w.Sync()
	}
	ce := w.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return n, e
	}
	// Link publishes atomically without ever replacing an immutable object.
	if e = b.root.Link(temp, p); e != nil {
		return n, e
	}
	d, e := b.root.Open(f)
	if e != nil {
		return n, e
	}
	defer d.Close()
	return n, d.Sync()
}
func (b *DirectoryBlobStore) Open(ctx context.Context, f, id string) (io.ReadCloser, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	p, e := blobPath(f, id)
	if e != nil {
		return nil, e
	}
	return b.root.Open(p)
}
func (b *DirectoryBlobStore) Size(ctx context.Context, f, id string) (int64, error) {
	p, e := blobPath(f, id)
	if e != nil {
		return 0, e
	}
	s, e := b.root.Stat(p)
	if e != nil {
		return 0, e
	}
	return s.Size(), ctx.Err()
}
func (b *DirectoryBlobStore) Delete(ctx context.Context, f, id string) error {
	p, e := blobPath(f, id)
	if e != nil {
		return e
	}
	e = b.root.Remove(p)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return e
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

// authorizationReader is an optional fast path for per-chunk checks. Keeping each
// check a fresh SQL query makes cross-server revocation visible without loading
// or locking all metadata for every 64 KiB chunk.
type authorizationReader interface {
	authorize(context.Context, Principal, string, bool, string, string, time.Time) (Ticket, error)
}

func (s *SQLiteMetaStore) authorize(ctx context.Context, p Principal, id string, write bool, ticket, blob string, now time.Time) (Ticket, error) {
	var folderJSON, ticketJSON []byte
	query := `SELECT f.data, COALESCE(t.data,'null') FROM folders f LEFT JOIN tickets t ON t.id=? AND json_extract(t.data,'$.FolderID')=f.id WHERE f.id=?`
	args := []any{ticket, id}
	if ticket != "" {
		query += ` AND t.id IS NOT NULL`
	}
	if blob != "" {
		query += ` AND EXISTS(SELECT 1 FROM files r WHERE r.folder=f.id AND json_extract(r.data,'$.BlobID')=? AND json_extract(r.data,'$.Deleted')=0)`
		args = append(args, blob)
	}
	if e := s.db.QueryRowContext(ctx, query, args...).Scan(&folderJSON, &ticketJSON); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return Ticket{}, ErrDenied
		}
		return Ticket{}, e
	}
	var f FolderRecord
	if e := json.Unmarshal(folderJSON, &f); e != nil {
		return Ticket{}, e
	}
	m := &Metadata{Folders: map[string]FolderRecord{id: f}}
	if _, e := access(m, p, id, write, false); e != nil {
		return Ticket{}, e
	}
	var t Ticket
	if ticket != "" {
		if e := json.Unmarshal(ticketJSON, &t); e != nil {
			return t, e
		}
		if t.FolderID != id || t.Principal != p || !t.Expires.After(now) {
			return Ticket{}, ErrDenied
		}
	}
	return t, nil
}

type versionReader interface {
	folderVersion(context.Context, Principal, string) (uint64, error)
}

func (s *SQLiteMetaStore) folderVersion(ctx context.Context, p Principal, id string) (uint64, error) {
	var data []byte
	if e := s.db.QueryRowContext(ctx, "SELECT data FROM folders WHERE id=?", id).Scan(&data); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return 0, ErrDenied
		}
		return 0, e
	}
	var f FolderRecord
	if e := json.Unmarshal(data, &f); e != nil {
		return 0, e
	}
	if _, e := access(&Metadata{Folders: map[string]FolderRecord{id: f}}, p, id, false, false); e != nil {
		return 0, e
	}
	return f.Folder.Version, nil
}
