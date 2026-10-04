package engine

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
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type FolderRecord struct {
	AuthEpoch     string
	Folder        Folder
	Grants        map[string]Role `json:"-"`
	Allocated     bool
	Deleted       bool
	FileUsage     Usage
	TransferUsage Usage
}
type Garbage struct {
	FolderID, BlobID string
	Owner            Principal
	Size             int64
	Writing          bool
	// Writer and Lease are copied from the ticket that became this garbage.
	// Another authority's lease is still live until Lease, so collection must
	// not delete the blob early.
	Writer string
	Lease  time.Time
}
type Metadata struct {
	Folders           map[string]FolderRecord
	Files             map[string]map[string]Row
	Tickets           map[string]Ticket
	Garbage           map[string]Garbage
	Accounts          map[string]Account
	selected          map[string]Usage
	baseline          map[string]Account
	filesLoaded       bool
	transfersPartial  bool
	selectedTransfers map[string]Usage
	ctx               context.Context
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
		// Default cache is 2MiB and autocheckpoint is 1000 pages. A garbage backlog
		// larger than that misses the cache and checkpoints mid-pass, so time per
		// row grows with the table. 16MiB and a 40MiB WAL threshold keep one pass linear.
		"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=10000", "PRAGMA synchronous=FULL",
		"PRAGMA cache_size=-16384", "PRAGMA wal_autocheckpoint=10000",
		`CREATE TABLE IF NOT EXISTS folders (id TEXT PRIMARY KEY, owner TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', live INTEGER NOT NULL DEFAULT 1, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS grants (folder TEXT NOT NULL, principal TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,principal))`,
		`CREATE INDEX IF NOT EXISTS grants_principal ON grants(principal,folder)`,
		`CREATE INDEX IF NOT EXISTS folders_owner ON folders(owner)`,
		`CREATE TABLE IF NOT EXISTS files (folder TEXT NOT NULL, path TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(folder,path))`,
		`CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, folder TEXT NOT NULL, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS garbage (id TEXT PRIMARY KEY, folder TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0, writing INTEGER NOT NULL DEFAULT 0, data BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, data BLOB NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS folder_names ON folders(owner, name) WHERE live<>0`,
		`CREATE INDEX IF NOT EXISTS tickets_folder ON tickets(folder,id)`,
		`CREATE INDEX IF NOT EXISTS tickets_path ON tickets(folder,json_extract(data,'$.PathID'))`,
		`CREATE INDEX IF NOT EXISTS garbage_folder ON garbage(folder,id)`,
		`CREATE INDEX IF NOT EXISTS files_version ON files(folder,json_extract(data,'$.Version'),path)`,
		`CREATE INDEX IF NOT EXISTS files_tombstones ON files(json_extract(data,'$.DeletedAt'),folder) WHERE json_extract(data,'$.Deleted')=1`,
		`CREATE INDEX IF NOT EXISTS files_blob ON files(folder,json_extract(data,'$.BlobID'))`,
	} {
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	// Databases created before these columns still open. Fresh databases already
	// have them, so the duplicate-column error is expected. Writing is backfilled
	// only when the column is new; afterwards the column is authoritative.
	if _, e = db.Exec(`ALTER TABLE garbage ADD COLUMN size INTEGER NOT NULL DEFAULT 0`); e != nil && !strings.Contains(strings.ToLower(e.Error()), "duplicate column") {
		db.Close()
		return nil, e
	}
	if _, e = db.Exec(`ALTER TABLE garbage ADD COLUMN writing INTEGER NOT NULL DEFAULT 0`); e != nil && !strings.Contains(strings.ToLower(e.Error()), "duplicate column") {
		db.Close()
		return nil, e
	} else if e == nil {
		if _, e = db.Exec(`UPDATE garbage SET writing=1 WHERE json_extract(data,'$.Writing')=1`); e != nil {
			db.Close()
			return nil, e
		}
	}
	hubName := name
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
	TicketPaths []string
	Principal   Principal
	Folder      string
	Create      bool
	GC          bool
	NoFiles     bool
	ReadOnly    bool
	Paths       []string
	Tickets     []string
	Garbage     []string
	Write       bool
	Owner       bool
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
	if scoped && !scope.ReadOnly && scope.Folder != "" && scope.Principal.valid() {
		if role, e := sqlRole(ctx, s.reads, scope.Principal, scope.Folder, scope.Write); e != nil {
			return e
		} else if scope.Owner && role != Owner {
			return ErrDenied
		}
	}
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
	changed, e := persistScoped(ctx, tx, fn, false)
	if e != nil {
		return e
	}
	if scope.ReadOnly {
		return nil
	}
	if e = tx.Commit(); e != nil {
		return e
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
	var w *os.File
	for attempt := 0; attempt < 3; attempt++ {
		w, e = b.root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if !errors.Is(e, os.ErrNotExist) {
			break
		}
		if e = durableMkdirAll(b.root, f); e != nil {
			return 0, e
		}
	}
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
	e = syncDirectoryFile(d)
	d.Close()
	if e != nil {
		return e
	}
	if e = b.root.Remove(f); e != nil {
		if errors.Is(e, os.ErrNotExist) || errors.Is(e, syscall.ENOTEMPTY) || errors.Is(e, syscall.EEXIST) {
			return nil
		}
		return e
	}
	parent, e := b.root.Open(".")
	if e != nil {
		return e
	}
	defer parent.Close()
	return syncDirectoryFile(parent)
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
		var epoch string
		if e := s.reads.QueryRowContext(ctx, "SELECT COALESCE(json_extract(data,'$.AuthEpoch'),'') FROM folders WHERE id=?", id).Scan(&epoch); e != nil {
			return Ticket{}, ErrDenied
		}
		if t.AuthEpoch != epoch {
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

// listFolders returns the caller's folders from the principal grant index.
// Usage is the same cache ListFolders shows when no file, ticket, or garbage
// rows are loaded: file totals plus this folder's transfer counters.
func (s *SQLiteMetaStore) listFolders(ctx context.Context, p Principal) ([]Folder, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, e := s.reads.QueryContext(ctx, `
SELECT f.data, g.data
FROM grants g
JOIN folders f ON f.id = g.folder
WHERE g.principal = ?
ORDER BY f.id`, principalKey(p))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Folder{}
	for rows.Next() {
		var folderData, roleData []byte
		if e = rows.Scan(&folderData, &roleData); e != nil {
			return nil, e
		}
		var f FolderRecord
		var role Role
		if e = json.Unmarshal(folderData, &f); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(roleData, &role); e != nil {
			return nil, e
		}
		if f.Deleted {
			continue
		}
		if p == f.Folder.Owner {
			role = Owner
		}
		if role != Owner && role != Writer && role != Reader {
			continue
		}
		v := f.Folder
		u := f.FileUsage
		extra := f.TransferUsage
		u.Bytes = sat(u.Bytes, extra.Bytes)
		u.Reserved, u.ReservedFiles, u.ReservedRows, u.GarbageRows = extra.Reserved, extra.ReservedFiles, extra.ReservedRows, extra.GarbageRows
		v.Usage = u
		v.Role = role
		out = append(out, v)
	}
	return out, rows.Err()
}

// deleteGarbageBatch removes one batch of garbage and its cached usage.
// It does not load the folder's other tickets or garbage, and the caller
// commits this transaction before the next batch so the writer is released.
func (s *SQLiteMetaStore) deleteGarbageBatch(ctx context.Context, folder string, ids []string, live func(string) bool) error {
	if len(ids) == 0 {
		return nil
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, "UPDATE folders SET id=id WHERE 0"); e != nil {
		return e
	}
	kept := make([]string, 0, len(ids))
	for _, id := range ids {
		if live == nil || !live(id) {
			kept = append(kept, id)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	var folderData []byte
	if e = tx.QueryRowContext(ctx, "SELECT data FROM folders WHERE id=?", folder).Scan(&folderData); e != nil {
		return e
	}
	var f FolderRecord
	if e = json.Unmarshal(folderData, &f); e != nil {
		return e
	}
	var removedBytes, removedRows int64
	for start := 0; start < len(kept); start += 256 {
		end := min(start+256, len(kept))
		args := make([]any, 0, 1+end-start)
		args = append(args, folder)
		q := "DELETE FROM garbage WHERE folder=? AND id IN ("
		for _, id := range kept[start:end] {
			q += "?,"
			args = append(args, id)
		}
		q = strings.TrimSuffix(q, ",") + ") RETURNING size"
		rows, e := tx.QueryContext(ctx, q, args...)
		if e != nil {
			return e
		}
		for rows.Next() {
			var size int64
			if e = rows.Scan(&size); e != nil {
				rows.Close()
				return e
			}
			removedRows++
			removedBytes = sat(removedBytes, size)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
	}
	if removedRows == 0 {
		return tx.Commit()
	}
	f.TransferUsage.Bytes = max(0, f.TransferUsage.Bytes-removedBytes)
	f.TransferUsage.GarbageRows = max(0, f.TransferUsage.GarbageRows-removedRows)
	body, e := json.Marshal(f)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE folders SET data=? WHERE id=?", body, folder); e != nil {
		return e
	}
	if !f.Allocated {
		key := principalKey(f.Folder.Owner)
		var acctData []byte
		var acct Account
		e = tx.QueryRowContext(ctx, "SELECT data FROM accounts WHERE id=?", key).Scan(&acctData)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if len(acctData) > 0 {
			if e = json.Unmarshal(acctData, &acct); e != nil {
				return e
			}
		}
		acct.Bytes = max(0, acct.Bytes-removedBytes)
		acct.Rows = max(0, acct.Rows-removedRows)
		ab, e := json.Marshal(acct)
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO accounts(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", key, ab); e != nil {
			return e
		}
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	s.wakes.notify(folder)
	return nil
}

func execIn(ctx context.Context, tx sqlTx, prefix string, args []any) error {
	for len(args) > 0 {
		n := min(len(args), 256)
		q := prefix + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
		if _, e := tx.ExecContext(ctx, q, args[:n]...); e != nil {
			return e
		}
		args = args[n:]
	}
	return nil
}

// Bounded parameterized batches preserve the transaction boundary and avoid
// preparing/executing a statement for every fixture or multi-path commit row.
func writeValues(ctx context.Context, tx sqlTx, prefix, suffix string, width int, args []any) error {
	for len(args) > 0 {
		n := min(len(args), 256*width)
		values := strings.TrimSuffix(strings.Repeat("("+strings.TrimSuffix(strings.Repeat("?,", width), ",")+"),", n/width), ",")
		if _, e := tx.ExecContext(ctx, prefix+values+suffix, args[:n]...); e != nil {
			return e
		}
		args = args[n:]
	}
	return nil
}
