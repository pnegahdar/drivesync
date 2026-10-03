package drivesync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type FolderRecord struct {
	Folder    Folder
	Grants    map[string]Role
	Allocated bool
}
type Garbage struct {
	FolderID, BlobID string
	Owner            Principal
	Size             int64
}
type Metadata struct {
	Folders map[string]FolderRecord
	Files   map[string]map[string]Row
	Tickets map[string]Ticket
	Garbage map[string]Garbage
}

func newMetadata() *Metadata {
	return &Metadata{Folders: map[string]FolderRecord{}, Files: map[string]map[string]Row{}, Tickets: map[string]Ticket{}, Garbage: map[string]Garbage{}}
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
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=10000", "PRAGMA synchronous=FULL", `CREATE TABLE IF NOT EXISTS folders (id TEXT PRIMARY KEY, data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS files (folder TEXT NOT NULL, path TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,path))`, `CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS garbage (id TEXT PRIMARY KEY, data BLOB NOT NULL)`, `CREATE INDEX IF NOT EXISTS folders_owner ON folders(json_extract(data,'$.Folder.Owner.Tenant'),json_extract(data,'$.Folder.Owner.Subject'))`, `CREATE INDEX IF NOT EXISTS tickets_folder ON tickets(json_extract(data,'$.FolderID'))`, `CREATE INDEX IF NOT EXISTS files_version ON files(folder,json_extract(data,'$.Version'),path)`, `CREATE INDEX IF NOT EXISTS garbage_owner ON garbage(json_extract(data,'$.Owner.Tenant'),json_extract(data,'$.Owner.Subject'))`, `CREATE INDEX IF NOT EXISTS files_blob ON files(folder,json_extract(data,'$.BlobID'))`} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	return &SQLiteMetaStore{db: db}, nil
}
func (s *SQLiteMetaStore) Close() error { return s.db.Close() }

type transactionScope struct {
	Principal Principal
	Folder    string
	Owner     bool
	GC        bool
	NoFiles   bool
}
type scopeKey struct{}

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
	for _, table := range []string{"folders", "files", "tickets", "garbage"} {
		old[table] = map[string][]byte{}
		q := "SELECT id,data FROM " + table
		if table == "files" {
			q = "SELECT folder || '/' || path,data FROM files"
		}
		var args []any
		if scope, ok := ctx.Value(scopeKey{}).(transactionScope); ok {
			filter := "id=?"
			args = []any{scope.Folder}
			if scope.Owner {
				filter = `json_extract(data,'$.Folder.Owner.Tenant')=(SELECT json_extract(data,'$.Folder.Owner.Tenant') FROM folders WHERE id=?) AND json_extract(data,'$.Folder.Owner.Subject')=(SELECT json_extract(data,'$.Folder.Owner.Subject') FROM folders WHERE id=?)`
				args = append(args, scope.Folder)
			}
			if scope.Folder == "" {
				filter = `json_extract(data,'$.Folder.Owner.Tenant')=? AND json_extract(data,'$.Folder.Owner.Subject')=?`
				args = []any{scope.Principal.Tenant, scope.Principal.Subject}
				if !scope.Owner {
					filter += ` OR EXISTS(SELECT 1 FROM json_each(json_extract(data,'$.Grants')) WHERE key=?)`
					args = append(args, principalKey(scope.Principal))
				}
			}
			if scope.NoFiles && table == "files" {
				q += " WHERE 0"
				args = nil
			} else if scope.GC {
				if table == "files" {
					q += " WHERE 0"
				}
				args = nil
			} else if table == "folders" {
				q += " WHERE " + filter
			} else if table == "files" {
				q += " WHERE folder IN (SELECT id FROM folders WHERE " + filter + ")"
			} else if table == "tickets" {
				q += ` WHERE json_extract(data,'$.FolderID') IN (SELECT id FROM folders WHERE ` + filter + ")"
			} else { // Deleted folders' garbage remains charged to its primary owner.
				if scope.Folder == "" && scope.Owner {
					q += ` WHERE json_extract(data,'$.Owner.Tenant')=? AND json_extract(data,'$.Owner.Subject')=?`
					args = []any{scope.Principal.Tenant, scope.Principal.Subject}
				} else {
					q += ` WHERE json_extract(data,'$.Owner') IN (SELECT json_extract(data,'$.Folder.Owner') FROM folders WHERE ` + filter + ")"
				}
				if scope.NoFiles && table == "files" {
					q += " WHERE 0"
					args = nil
				} else if scope.GC {
					q = "SELECT id,data FROM garbage"
					args = nil
				}
			}
		}
		rs, e := tx.QueryContext(ctx, q, args...)
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
			case "garbage":
				var v Garbage
				if e = json.Unmarshal(data, &v); e == nil {
					m.Garbage[id] = v
				}
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
	next := map[string]map[string][]byte{"folders": {}, "files": {}, "tickets": {}, "garbage": {}}
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
	for id, v := range m.Garbage {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		next["garbage"][id] = b
	}
	for id, v := range m.Tickets {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		next["tickets"][id] = b
	}
	for _, table := range []string{"folders", "files", "tickets", "garbage"} {
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
	if e := durableDirectory(dir); e != nil {
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
	if e = durableMkdirAll(b.root, f); e != nil {
		return 0, e
	}
	temp := filepath.Join(f, ".upload-"+id)
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
	if e = b.root.Remove(temp); e != nil {
		return n, e
	}
	d, e := b.root.Open(f)
	if e != nil {
		return n, e
	}
	defer d.Close()
	return n, syncDirectoryFile(d)
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
	if e = ctx.Err(); e != nil {
		return e
	}
	for _, name := range []string{p, filepath.Join(f, ".upload-"+id)} {
		if e = b.root.Remove(name); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	d, e := b.root.Open(f)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	defer d.Close()
	return syncDirectoryFile(d)
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
		if t.FolderID != id || t.Principal != p {
			return Ticket{}, ErrDenied
		}
		if !t.Expires.After(now) {
			return Ticket{}, ErrExpired
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

func (s *SQLiteMetaStore) changesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	version, e := s.folderVersion(ctx, p, id)
	if e != nil {
		return Delta{}, e
	}
	if after > version {
		return Delta{}, ErrInvalid
	}
	if until == 0 {
		until = version
	}
	if until < after || until > version {
		return Delta{}, ErrInvalid
	}
	cv, cp, e := parsePage(page, after, until)
	if e != nil {
		return Delta{}, e
	}
	rows, e := s.db.QueryContext(ctx, `SELECT data FROM files WHERE folder=? AND json_extract(data,'$.Version')>? AND json_extract(data,'$.Version')<=? AND (json_extract(data,'$.Version')>? OR (json_extract(data,'$.Version')=? AND path>?)) ORDER BY json_extract(data,'$.Version'),path LIMIT 513`, id, after, until, cv, cv, cp)
	if e != nil {
		return Delta{}, e
	}
	defer rows.Close()
	out := Delta{Version: until}
	for rows.Next() {
		var data []byte
		var row Row
		if e = rows.Scan(&data); e != nil {
			return Delta{}, e
		}
		if e = json.Unmarshal(data, &row); e != nil {
			return Delta{}, e
		}
		if len(out.Rows) == 512 {
			last := out.Rows[len(out.Rows)-1]
			out.Next = fmt.Sprintf("%d/%s", last.Version, last.PathID)
			break
		}
		out.Rows = append(out.Rows, row)
	}
	if e = rows.Err(); e != nil {
		return Delta{}, e
	}
	// Close before fresh authorization (SQLite uses a single connection).
	rows.Close()
	if _, e = s.folderVersion(ctx, p, id); e != nil {
		return Delta{}, e
	}
	return out, nil
}
