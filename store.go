package drivesync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type FolderRecord struct {
	Folder    Folder
	Grants    map[string]Role `json:"-"`
	Allocated bool
	Deleted   bool
	FileUsage Usage
}
type Garbage struct {
	FolderID, BlobID string
	Owner            Principal
	Size             int64
	Writing          bool
}
type Metadata struct {
	Folders     map[string]FolderRecord
	Files       map[string]map[string]Row
	Tickets     map[string]Ticket
	Garbage     map[string]Garbage
	Accounts    map[string]Account
	selected    map[string]Usage
	baseline    map[string]Account
	filesLoaded bool
	ctx         context.Context
}

func newMetadata() *Metadata {
	return &Metadata{Folders: map[string]FolderRecord{}, Files: map[string]map[string]Row{}, Tickets: map[string]Ticket{}, Garbage: map[string]Garbage{}, Accounts: map[string]Account{}, baseline: map[string]Account{}, filesLoaded: true}
}

// MetaStore serializes transactions across all server instances sharing it. A callback's
// error rolls back every change. Implementations must never expose state after a callback.
// For writes, load scoped records and owner Accounts, call Metadata.Prepare,
// then the callback and Metadata.Finish; persist changes atomically. With Paths
// selected, Files contains only those rows and FileUsage is the full cache.
// ReadOnly scopes use an independent snapshot and never persist the callback.
// Private account totals must not require account-wide rows.
type MetaStore interface {
	Transaction(context.Context, func(*Metadata) error) error
}

// SQLiteMetaStore stores one durable SQL row per folder, file and reservation.
type SQLiteMetaStore struct {
	db        *sql.DB
	reads     *sql.DB
	wakes     *notifications
	release   func()
	closeOnce sync.Once
}

func (s *SQLiteMetaStore) Notifications() *Notifications { return s.wakes }

func OpenSQLiteMetaStore(name string) (*SQLiteMetaStore, error) {
	if name == ":memory:" {
		name = "file:drivesync-" + randomID() + "?mode=memory&cache=shared"
	}
	db, e := sql.Open("sqlite", name)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=10000", "PRAGMA synchronous=FULL",
		`CREATE TABLE IF NOT EXISTS folders (id TEXT PRIMARY KEY, owner TEXT NOT NULL, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS grants (folder TEXT NOT NULL, principal TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,principal))`,
		`CREATE INDEX IF NOT EXISTS grants_principal ON grants(principal,folder)`,
		`CREATE INDEX IF NOT EXISTS folders_owner ON folders(owner)`,
		`CREATE TABLE IF NOT EXISTS files (folder TEXT NOT NULL, path TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,path))`,
		`CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, folder TEXT NOT NULL, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS garbage (id TEXT PRIMARY KEY, folder TEXT NOT NULL, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, data BLOB NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS folder_names ON folders(owner,json_extract(data,'$.Folder.Name')) WHERE json_extract(data,'$.Deleted')=0`,
		`CREATE INDEX IF NOT EXISTS tickets_folder ON tickets(folder)`,
		`CREATE INDEX IF NOT EXISTS garbage_folder ON garbage(folder)`,
		`CREATE INDEX IF NOT EXISTS files_version ON files(folder,json_extract(data,'$.Version'),path)`,
		`CREATE INDEX IF NOT EXISTS files_tombstones ON files(json_extract(data,'$.DeletedAt'),folder) WHERE json_extract(data,'$.Deleted')=1`,
		`CREATE INDEX IF NOT EXISTS files_blob ON files(folder,json_extract(data,'$.BlobID'))`,
	} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	hubName := ":memory:"
	var seq int
	var databaseName, file string
	if e = db.QueryRow("PRAGMA database_list").Scan(&seq, &databaseName, &file); e != nil {
		db.Close()
		return nil, e
	}
	if file != "" {
		hubName = file
		if canonical, e := filepath.EvalSymlinks(file); e == nil {
			hubName = canonical
		}
	}
	readName := name
	if file != "" {
		readName = (&url.URL{Scheme: "file", Path: file}).String() + "?mode=ro"
	}
	sep := "?"
	if strings.Contains(readName, "?") {
		sep = "&"
	}
	reads, e := sql.Open("sqlite", readName+sep+"_pragma=query_only(1)&_pragma=busy_timeout(10000)")
	if e != nil {
		db.Close()
		return nil, e
	}
	reads.SetMaxOpenConns(8)
	if e = reads.Ping(); e != nil {
		reads.Close()
		db.Close()
		return nil, e
	}
	wakes, release := sqliteNotifications(hubName)
	return &SQLiteMetaStore{db: db, reads: reads, wakes: wakes, release: release}, nil
}
func (s *SQLiteMetaStore) Close() error {
	e := errors.Join(s.reads.Close(), s.db.Close())
	s.closeOnce.Do(s.release)
	return e
}

// Scope describes the records needed by a metadata transaction. Folder operations
// select only Folder and its primary owner counter; an empty Folder lists visible
// definitions or creates one. GC is an explicit maintenance scope. A nonnil
// Paths selects only those file rows (empty means none); nil loads all unless
// NoFiles/GC is set. ReadOnly callbacks are read snapshots without accounting
// updates or a writer lock.
type Scope struct {
	Principal Principal
	Folder    string
	Create    bool
	GC        bool
	NoFiles   bool
	ReadOnly  bool
	Paths     []string
}
type scopeKey struct{}

// ScopeFromContext lets alternate MetaStore implementations select the same
// records as SQLite. An unscoped transaction is an administrative full view.
func ScopeFromContext(ctx context.Context) (Scope, bool) {
	v, ok := ctx.Value(scopeKey{}).(Scope)
	return v, ok
}

func (s *SQLiteMetaStore) Transaction(ctx context.Context, fn func(*Metadata) error) error {
	// BEGIN's first write acquires the SQLite writer lock before reading any accounting.
	scope, scoped := ScopeFromContext(ctx)
	pool := s.db
	if scope.ReadOnly {
		pool = s.reads
	}
	tx, e := pool.BeginTx(ctx, &sql.TxOptions{ReadOnly: scope.ReadOnly})
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if !scope.ReadOnly {
		if _, e = tx.ExecContext(ctx, "UPDATE folders SET id=id WHERE 0"); e != nil {
			return e
		}
	}
	if scope, ok := ScopeFromContext(ctx); ok && scope.Folder != "" && scope.Principal.valid() {
		if _, e := sqlRole(ctx, tx, scope.Principal, scope.Folder, false); e != nil {
			return e
		}
	}
	m := newMetadata()
	old := map[string]map[string][]byte{}
	for _, table := range []string{"folders", "grants", "files", "tickets", "garbage"} {
		old[table] = map[string][]byte{}
		q := "SELECT id,data FROM " + table
		if table == "files" {
			q = "SELECT folder || '/' || path,data FROM files"
		}
		if table == "grants" {
			q = "SELECT folder || '/' || principal,data FROM grants"
		}
		var args []any
		if scope, ok := ctx.Value(scopeKey{}).(Scope); ok {
			m.filesLoaded = !scope.NoFiles && !scope.GC && scope.Paths == nil
			filter := "id=?"
			args = []any{scope.Folder}
			if scope.Folder == "" {
				if scope.Create {
					filter = "0"
					args = nil
				} else {
					filter = `id IN (SELECT folder FROM grants WHERE principal=?)`
					args = []any{principalKey(scope.Principal)}
				}
			}
			if scope.GC {
				args = nil
				if table == "files" {
					q += " WHERE 0"
				}
			} else if scope.NoFiles && table == "files" {
				q += " WHERE 0"
				args = nil
			} else if table == "folders" {
				q += " WHERE " + filter
			} else if table == "files" {
				q += " WHERE folder IN (SELECT id FROM folders WHERE " + filter + ")"
				if scope.Paths != nil {
					if len(scope.Paths) == 0 {
						q += " AND 0"
					} else {
						q += " AND path IN (" + strings.TrimSuffix(strings.Repeat("?,", len(scope.Paths)), ",") + ")"
						for _, p := range scope.Paths {
							args = append(args, p)
						}
					}
				}
			} else {
				q += ` WHERE folder IN (SELECT id FROM folders WHERE ` + filter + ")"
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
			case "grants":
				var role Role
				e = json.Unmarshal(data, &role)
				f := m.Folders[id[:32]]
				if id[33:] != principalKey(f.Folder.Owner) {
					f.Grants[id[33:]] = role
				}
				m.Folders[id[:32]] = f
			case "garbage":
				var v Garbage
				if e = json.Unmarshal(data, &v); e == nil {
					m.Garbage[id] = v
				}
			case "folders":
				var v FolderRecord
				if e = json.Unmarshal(data, &v); e == nil {
					v.Grants = map[string]Role{}
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
		if table == "grants" {
			if scope, ok := ScopeFromContext(ctx); ok && scope.Folder != "" && scope.Principal.valid() {
				if _, e = access(m, scope.Principal, scope.Folder, false, false); e != nil {
					return e
				}
			}
		}

	}
	if scoped && scope.ReadOnly {
		m.ctx = ctx
		return fn(m)
	}
	keys := map[string]bool{}
	if scope, ok := ctx.Value(scopeKey{}).(Scope); ok && scope.Folder == "" && scope.Principal.valid() {
		keys[principalKey(scope.Principal)] = true
	}
	for _, f := range m.Folders {
		keys[principalKey(f.Folder.Owner)] = true
	}
	for _, g := range m.Garbage {
		keys[principalKey(g.Owner)] = true
	}
	if scoped && scope.GC {
		rows, qe := tx.QueryContext(ctx, "SELECT id,data FROM accounts")
		if qe != nil {
			return qe
		}
		for rows.Next() {
			var key string
			var data []byte
			var a Account
			if qe = rows.Scan(&key, &data); qe == nil {
				qe = json.Unmarshal(data, &a)
			}
			if qe != nil {
				rows.Close()
				return qe
			}
			m.Accounts[key] = a
		}
		qe = rows.Err()
		rows.Close()
		if qe != nil {
			return qe
		}
	} else {
		for key := range keys {
			var data []byte
			e = tx.QueryRowContext(ctx, "SELECT data FROM accounts WHERE id=?", key).Scan(&data)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
			var a Account
			if len(data) > 0 {
				if e = json.Unmarshal(data, &a); e != nil {
					return e
				}
			}
			m.Accounts[key] = a
		}
	}
	beforeAccounts := make(map[string]Account, len(m.Accounts))
	for k, a := range m.Accounts {
		beforeAccounts[k] = a
	}
	m.Prepare(m.filesLoaded)
	if e = fn(m); e != nil {
		return e
	}
	m.Finish()
	for key, a := range m.Accounts {
		if a == beforeAccounts[key] {
			continue
		}
		b, e := json.Marshal(a)
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO accounts(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", key, b); e != nil {
			return e
		}
	}

	next := map[string]map[string][]byte{"folders": {}, "grants": {}, "files": {}, "tickets": {}, "garbage": {}}
	for id, v := range m.Folders {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		next["folders"][id] = b
		if !v.Deleted {
			next["grants"][id+"/"+principalKey(v.Folder.Owner)], _ = json.Marshal(Owner)
			for key, role := range v.Grants {
				next["grants"][id+"/"+key], _ = json.Marshal(role)
			}
		}
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
	for _, table := range []string{"folders", "grants", "files", "tickets", "garbage"} {
		var upsert, remove *sql.Stmt
		defer func() {
			if upsert != nil {
				upsert.Close()
			}
			if remove != nil {
				remove.Close()
			}
		}()
		for id, b := range next[table] {
			if bytes.Equal(b, old[table][id]) {
				continue
			}
			query := "INSERT INTO " + table + "(id,folder,data) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data,folder=excluded.folder"
			folder := ""
			if table == "tickets" {
				folder = m.Tickets[id].FolderID
			}
			if table == "garbage" {
				folder = m.Garbage[id].FolderID
			}
			args := []any{id, folder, b}
			switch table {
			case "files":
				query = "INSERT INTO files(folder,path,data) VALUES(?,?,?) ON CONFLICT(folder,path) DO UPDATE SET data=excluded.data"
				args = []any{id[:32], id[33:], b}
			case "folders":
				query = "INSERT INTO folders(id,owner,data) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data,owner=excluded.owner"
				args = []any{id, principalKey(m.Folders[id].Folder.Owner), b}
			case "grants":
				query = "INSERT INTO grants(folder,principal,data) VALUES(?,?,?) ON CONFLICT(folder,principal) DO UPDATE SET data=excluded.data"
				args = []any{id[:32], id[33:], b}
			}
			if upsert == nil {
				upsert, e = tx.PrepareContext(ctx, query)
				if e != nil {
					return e
				}
			}
			if _, e = upsert.ExecContext(ctx, args...); e != nil {
				if table == "folders" && strings.Contains(e.Error(), "UNIQUE constraint failed") {
					return ErrConflict
				}
				return e
			}
		}
		for id := range old[table] {
			if _, ok := next[table][id]; ok {
				continue
			}
			query := "DELETE FROM " + table + " WHERE id=?"
			args := []any{id}
			if table == "files" {
				query = "DELETE FROM files WHERE folder=? AND path=?"
				args = []any{id[:32], id[33:]}
			} else if table == "grants" {
				query = "DELETE FROM grants WHERE folder=? AND principal=?"
				args = []any{id[:32], id[33:]}
			}
			if remove == nil {
				remove, e = tx.PrepareContext(ctx, query)
				if e != nil {
					return e
				}
			}
			if _, e = remove.ExecContext(ctx, args...); e != nil {
				return e
			}
		}
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	changed := map[string]bool{}
	for _, table := range []string{"folders", "grants"} {
		for id, b := range next[table] {
			if !bytes.Equal(b, old[table][id]) {
				if table == "grants" {
					id = id[:32]
				}
				changed[id] = true
			}
		}
		for id := range old[table] {
			if _, ok := next[table][id]; !ok {
				if table == "grants" {
					id = id[:32]
				}
				changed[id] = true
			}
		}
	}
	for id := range changed {
		s.wakes.notify(id)
	}
	return nil
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
	if !p.valid() {
		return Ticket{}, ErrDenied
	}
	var roleJSON, ticketJSON []byte
	query := `SELECT g.data, COALESCE(t.data,'null') FROM grants g JOIN folders f ON f.id=g.folder LEFT JOIN tickets t ON t.id=? AND t.folder=g.folder WHERE g.folder=? AND g.principal=?`
	args := []any{ticket, id, principalKey(p)}
	if ticket != "" {
		query += ` AND t.id IS NOT NULL`
	}
	if blob != "" {
		query += ` AND EXISTS(SELECT 1 FROM files r WHERE r.folder=f.id AND json_extract(r.data,'$.BlobID')=? AND json_extract(r.data,'$.Deleted')=0)`
		args = append(args, blob)
	}
	if e := s.reads.QueryRowContext(ctx, query, args...).Scan(&roleJSON, &ticketJSON); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return Ticket{}, ErrDenied
		}
		return Ticket{}, e
	}
	var role Role
	if e := json.Unmarshal(roleJSON, &role); e != nil {
		return Ticket{}, e
	}
	if role != Owner && role != Writer && role != Reader || write && role == Reader {
		return Ticket{}, ErrDenied
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
	if !p.valid() {
		return 0, ErrDenied
	}
	var version uint64
	e := s.reads.QueryRowContext(ctx, `SELECT json_extract(f.data,'$.Folder.Version') FROM grants g JOIN folders f ON f.id=g.folder WHERE g.folder=? AND g.principal=? AND json_extract(g.data,'$') IN ('owner','writer','reader')`, id, principalKey(p)).Scan(&version)
	if errors.Is(e, sql.ErrNoRows) {
		return 0, ErrDenied
	}
	return version, e
}

type sqlQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func sqlRole(ctx context.Context, q sqlQueryer, p Principal, id string, write bool) (Role, error) {
	if !p.valid() {
		return "", ErrDenied
	}
	var data []byte
	e := q.QueryRowContext(ctx, "SELECT data FROM grants WHERE folder=? AND principal=?", id, principalKey(p)).Scan(&data)
	if errors.Is(e, sql.ErrNoRows) {
		return "", ErrDenied
	}
	if e != nil {
		return "", e
	}
	var role Role
	if e = json.Unmarshal(data, &role); e != nil {
		return "", e
	}
	if role != Owner && role != Writer && role != Reader || write && role == Reader {
		return "", ErrDenied
	}
	return role, nil
}

func (s *SQLiteMetaStore) changesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	version, e := s.folderVersion(ctx, p, id)
	if e != nil {
		return Delta{}, e
	}
	var horizon uint64
	if e = s.reads.QueryRowContext(ctx, "SELECT json_extract(data,'$.Folder.Horizon') FROM folders WHERE id=?", id).Scan(&horizon); e != nil {
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
	mode, e := pageMode(page, after, until, horizon)
	if e != nil {
		return Delta{}, e
	}
	query := `SELECT data FROM files WHERE folder=? AND json_extract(data,'$.Version')>? AND json_extract(data,'$.Version')<=? AND (json_extract(data,'$.Version')>? OR (json_extract(data,'$.Version')=? AND path>?)) ORDER BY json_extract(data,'$.Version'),path LIMIT 513`
	args := []any{id, after, until, mode.version, mode.version, mode.path}
	if mode.full {
		query = `SELECT data FROM files WHERE folder=? AND path>? ORDER BY path LIMIT 513`
		args = []any{id, mode.path}
	}
	rows, e := s.reads.QueryContext(ctx, query, args...)
	if e != nil {
		return Delta{}, e
	}
	defer rows.Close()
	out := Delta{Version: until, Horizon: mode.horizon, Full: mode.full}
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
			out.Next = mode.next(last)
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
