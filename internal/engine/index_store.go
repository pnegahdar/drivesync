package engine

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
)

// The replica index stores each entry once under its parent directory: dirs
// interns every directory that has indexed children, and entries keys a
// compact record by (dir, name). The root is the implicit directory 0, so
// clearing the index is DELETE FROM entries plus DELETE FROM dirs.
//
// user_version 2 is this layout. Version 0 with entries(path,data) is the
// JSON layout, migrated once in a single transaction.
const indexSchemaVersion = 2

const indexSchema = `CREATE TABLE IF NOT EXISTS dirs (id INTEGER PRIMARY KEY,parent INTEGER NOT NULL,name TEXT NOT NULL,UNIQUE(parent,name));
CREATE TABLE IF NOT EXISTS entries (dir INTEGER NOT NULL,name TEXT NOT NULL,data BLOB NOT NULL,PRIMARY KEY(dir,name)) WITHOUT ROWID`

// Record layout: codec version, little-endian uint16 flags, uvarint Version,
// Size, and Mode, then the fields whose flags are set, in flag order: raw
// Hash, raw Blob, uvarint Dev and Inode, and the Local alias as a uvarint
// length and its bytes. An optional field is present exactly when it is
// nonzero (Local: when it differs from Path), so every entry has one encoding.
const indexCodec = 1

const (
	entryDeleted uint16 = 1 << iota
	entryAwaiting
	entryPlaceholder
	entryRemoteOnly
	entryDirectory
	entryHash
	entryBlob
	entryDevice
	entryLocal
	entryFlags = entryLocal<<1 - 1
)

func indexCorrupt(why string) error {
	return fmt.Errorf("%w: replica index %s", ErrInvalid, why)
}

func encodeIndexEntry(v IndexEntry) ([]byte, error) {
	if _, e := NormalizePath(v.Path); e != nil {
		return nil, e
	}
	if _, e := NormalizePath(v.Local); e != nil {
		return nil, e
	}
	if v.Size < 0 {
		return nil, indexCorrupt("entry has a negative size")
	}
	var flags uint16
	for n, set := range [...]bool{v.Deleted, v.Awaiting, v.Placeholder, v.RemoteOnly, v.Directory, v.Hash != [32]byte{}, v.Blob != [16]byte{}, v.Dev != 0 || v.Inode != 0, v.Local != v.Path} {
		if set {
			flags |= 1 << n
		}
	}
	b := make([]byte, 0, 3+3*binary.MaxVarintLen64+len(v.Hash)+len(v.Blob))
	b = append(b, indexCodec)
	b = binary.LittleEndian.AppendUint16(b, flags)
	b = binary.AppendUvarint(b, v.Version)
	b = binary.AppendUvarint(b, uint64(v.Size))
	b = binary.AppendUvarint(b, uint64(v.Mode))
	if flags&entryHash != 0 {
		b = append(b, v.Hash[:]...)
	}
	if flags&entryBlob != 0 {
		b = append(b, v.Blob[:]...)
	}
	if flags&entryDevice != 0 {
		b = binary.AppendUvarint(b, v.Dev)
		b = binary.AppendUvarint(b, v.Inode)
	}
	if flags&entryLocal != 0 {
		b = binary.AppendUvarint(b, uint64(len(v.Local)))
		b = append(b, v.Local...)
	}
	return b, nil
}

// decodeIndexEntry accepts only the canonical encoding of a valid entry.
func decodeIndexEntry(p string, b []byte) (IndexEntry, error) {
	if _, e := NormalizePath(p); e != nil {
		return IndexEntry{}, indexCorrupt("entry has an invalid path")
	}
	if len(b) < 3 || b[0] != indexCodec {
		return IndexEntry{}, indexCorrupt("entry has an unknown encoding")
	}
	flags := binary.LittleEndian.Uint16(b[1:])
	if flags&^entryFlags != 0 {
		return IndexEntry{}, indexCorrupt("entry has unknown flags")
	}
	b = b[3:]
	ok := true
	next := func() uint64 {
		v, n := binary.Uvarint(b)
		if n <= 0 || n != len(binary.AppendUvarint(nil, v)) {
			ok = false
			return 0
		}
		b = b[n:]
		return v
	}
	v := IndexEntry{Path: p, Local: p, Deleted: flags&entryDeleted != 0, Awaiting: flags&entryAwaiting != 0, Placeholder: flags&entryPlaceholder != 0, RemoteOnly: flags&entryRemoteOnly != 0, Directory: flags&entryDirectory != 0}
	v.Version = next()
	size, mode := next(), next()
	if !ok || size > math.MaxInt64 || mode > math.MaxUint32 {
		return IndexEntry{}, indexCorrupt("entry has an invalid number")
	}
	v.Size, v.Mode = int64(size), uint32(mode)
	if flags&entryHash != 0 {
		if len(b) < len(v.Hash) {
			return IndexEntry{}, indexCorrupt("entry is truncated")
		}
		b = b[copy(v.Hash[:], b):]
		if v.Hash == [32]byte{} {
			return IndexEntry{}, indexCorrupt("entry has a noncanonical hash")
		}
	}
	if flags&entryBlob != 0 {
		if len(b) < len(v.Blob) {
			return IndexEntry{}, indexCorrupt("entry is truncated")
		}
		b = b[copy(v.Blob[:], b):]
		if v.Blob == [16]byte{} {
			return IndexEntry{}, indexCorrupt("entry has a noncanonical blob")
		}
	}
	if flags&entryDevice != 0 {
		v.Dev = next()
		v.Inode = next()
		if !ok || v.Dev == 0 && v.Inode == 0 {
			return IndexEntry{}, indexCorrupt("entry has an invalid file identity")
		}
	}
	if flags&entryLocal != 0 {
		n := next()
		if !ok || n > uint64(len(b)) {
			return IndexEntry{}, indexCorrupt("entry has an invalid local path")
		}
		v.Local, b = string(b[:n]), b[n:]
		if _, e := NormalizePath(v.Local); e != nil || v.Local == p {
			return IndexEntry{}, indexCorrupt("entry has an invalid local path")
		}
	}
	if len(b) != 0 {
		return IndexEntry{}, indexCorrupt("entry has trailing bytes")
	}
	return v, nil
}

func parseHex(s string, out []byte) error {
	if s == "" {
		clear(out)
		return nil
	}
	if len(s) != 2*len(out) || strings.Trim(s, "0123456789abcdef") != "" {
		return ErrInvalid
	}
	_, e := hex.Decode(out, []byte(s))
	return e
}

// parseHash reads a lowercase hex BLAKE3 digest. Empty is the zero digest.
func parseHash(s string) (h [32]byte, e error) { return h, parseHex(s, h[:]) }

// parseBlob reads a lowercase hex blob id. Empty is the zero id.
func parseBlob(s string) (b [16]byte, e error) { return b, parseHex(s, b[:]) }

// initializeIndex prepares a replica state database: connection pragmas, every
// state table, and a one-time transactional migration of the JSON index. The
// old table is dropped only in the transaction that commits its conversion.
// A database from a newer release is refused rather than misread.
func initializeIndex(db *sql.DB) error {
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL"} {
		if _, e := db.Exec(q); e != nil {
			return e
		}
	}
	var version int
	if e := db.QueryRow("PRAGMA user_version").Scan(&version); e != nil {
		return e
	}
	if version > indexSchemaVersion {
		return indexCorrupt("is from a newer release")
	}
	var legacy bool
	if e := db.QueryRow("SELECT EXISTS(SELECT 1 FROM pragma_table_info('entries') WHERE name='path')").Scan(&legacy); e != nil {
		return e
	}
	if version == indexSchemaVersion && !legacy {
		return nil
	}
	if legacy && version != 0 {
		return indexCorrupt("has an inconsistent schema")
	}
	tx, e := db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, q := range []string{`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY,value TEXT NOT NULL)`, `CREATE TABLE IF NOT EXISTS pending (id TEXT PRIMARY KEY,data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS tombstones (id TEXT PRIMARY KEY,version INTEGER NOT NULL)`} {
		if _, e = tx.Exec(q); e != nil {
			return e
		}
	}
	if legacy {
		if _, e = tx.Exec("ALTER TABLE entries RENAME TO legacy_entries"); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(indexSchema); e != nil {
		return e
	}
	if legacy {
		if e = migrateLegacyIndex(tx); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", indexSchemaVersion)); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	if legacy {
		// Return the JSON table's pages to the filesystem. The index is already
		// complete, so a failed VACUUM (for instance, no room for the copy)
		// only leaves free pages behind.
		_, _ = db.Exec("VACUUM")
	}
	return nil
}

// legacyIndexEntry is the JSON IndexEntry written before user_version 2,
// including the oldest form without Size, Row, Dev, or Inode.
type legacyIndexEntry struct {
	Path        string `json:"Path"`
	Local       string `json:"Local"`
	Hash        string `json:"Hash"`
	Version     uint64 `json:"Version"`
	Size        int64  `json:"Size"`
	Mode        uint32 `json:"Mode"`
	Dev         uint64 `json:"Dev"`
	Inode       uint64 `json:"Inode"`
	Placeholder bool   `json:"Placeholder"`
	RemoteOnly  bool   `json:"RemoteOnly"`
	Deleted     bool   `json:"Deleted"`
	Awaiting    bool   `json:"Awaiting"`
	Directory   bool   `json:"Directory"`
	Row         *struct {
		BlobID string `json:"BlobID"`
	} `json:"Row"`
}

func migrateLegacyIndex(tx *sql.Tx) error {
	rows, e := tx.Query("SELECT path,data FROM legacy_entries")
	if e != nil {
		return e
	}
	var entries []IndexEntry
	for rows.Next() {
		var key string
		var data []byte
		if e = rows.Scan(&key, &data); e != nil {
			break
		}
		var old legacyIndexEntry
		if json.Unmarshal(data, &old) != nil || old.Path != key || old.Size < 0 {
			e = indexCorrupt("has an unreadable legacy entry")
			break
		}
		v := IndexEntry{Path: old.Path, Local: old.Local, Version: old.Version, Size: old.Size, Mode: old.Mode, Dev: old.Dev, Inode: old.Inode, Placeholder: old.Placeholder, RemoteOnly: old.RemoteOnly, Deleted: old.Deleted, Awaiting: old.Awaiting, Directory: old.Directory}
		if old.Hash != "directory" {
			if v.Hash, e = parseHash(old.Hash); e != nil {
				e = indexCorrupt("has a legacy entry with an invalid hash")
				break
			}
		}
		if old.Row != nil {
			if v.Blob, e = parseBlob(old.Row.BlobID); e != nil {
				e = indexCorrupt("has a legacy entry with an invalid blob")
				break
			}
		}
		entries = append(entries, v)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	w := newIndexWriter(context.Background(), tx)
	for _, v := range entries {
		if e = w.store(v); e != nil {
			return e
		}
	}
	if e = w.close(); e != nil {
		return e
	}
	_, e = tx.Exec("DROP TABLE legacy_entries")
	return e
}

// loadIndex calls fn with every entry. Any inconsistency (an orphaned or
// cyclic directory, an invalid name, a malformed record) fails the load.
func loadIndex(db *sql.DB, fn func(IndexEntry) error) error {
	type dir struct {
		parent int64
		name   string
	}
	dirs := map[int64]dir{}
	rows, e := db.Query("SELECT id,parent,name FROM dirs")
	if e != nil {
		return e
	}
	for rows.Next() {
		var id int64
		var d dir
		if e = rows.Scan(&id, &d.parent, &d.name); e != nil {
			break
		}
		if id <= 0 || d.parent < 0 || id == d.parent || !indexName(d.name) {
			e = indexCorrupt("has an invalid directory")
			break
		}
		dirs[id] = d
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	paths := map[int64]string{0: ""}
	var resolve func(int64, int) (string, error)
	resolve = func(id int64, depth int) (string, error) {
		if p, ok := paths[id]; ok {
			return p, nil
		}
		d, ok := dirs[id]
		if !ok || depth > len(dirs) {
			return "", indexCorrupt("has an orphaned or cyclic directory")
		}
		parent, e := resolve(d.parent, depth+1)
		if e != nil {
			return "", e
		}
		p := path.Join(parent, d.name)
		if _, e = NormalizePath(p); e != nil {
			return "", indexCorrupt("has an invalid directory")
		}
		paths[id] = p
		return p, nil
	}
	for id := range dirs {
		if _, e = resolve(id, 0); e != nil {
			return e
		}
	}
	rows, e = db.Query("SELECT dir,name,data FROM entries")
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		var data []byte
		if e = rows.Scan(&id, &name, &data); e != nil {
			return e
		}
		parent, ok := paths[id]
		if !ok || !indexName(name) {
			return indexCorrupt("has an entry outside its directories")
		}
		v, e := decodeIndexEntry(path.Join(parent, name), data)
		if e != nil {
			return e
		}
		if e = fn(v); e != nil {
			return e
		}
	}
	return rows.Err()
}

// LoadIndex reads a replica's index.sqlite for diagnostics and tests. It
// validates exactly as attaching does.
func LoadIndex(db *sql.DB, fn func(IndexEntry) error) error { return loadIndex(db, fn) }

func indexName(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.Contains(s, "/")
}

// indexWriter writes entries inside one transaction. Its directory ids are
// cached only for that transaction, so a rollback cannot leave a stale id.
// close prunes directories that deletes emptied and must precede Commit.
type indexWriter struct {
	ctx                              context.Context
	tx                               *sql.Tx
	ids                              map[string]int64
	emptied                          map[string]bool
	find, insert, put, remove, prune *sql.Stmt
}

func newIndexWriter(ctx context.Context, tx *sql.Tx) *indexWriter {
	return &indexWriter{ctx: ctx, tx: tx, ids: map[string]int64{".": 0}, emptied: map[string]bool{}}
}

func (w *indexWriter) statement(s **sql.Stmt, q string) (*sql.Stmt, error) {
	if *s == nil {
		prepared, e := w.tx.PrepareContext(w.ctx, q)
		if e != nil {
			return nil, e
		}
		*s = prepared
	}
	return *s, nil
}

// dir returns the id of directory p, creating missing rows when create is
// set. Without create, a missing directory is (0, false).
func (w *indexWriter) dir(p string, create bool) (int64, bool, error) {
	if id, ok := w.ids[p]; ok {
		return id, true, nil
	}
	parent, ok, e := w.dir(path.Dir(p), create)
	if e != nil || !ok {
		return 0, false, e
	}
	name := path.Base(p)
	find, e := w.statement(&w.find, "SELECT id FROM dirs WHERE parent=? AND name=?")
	if e != nil {
		return 0, false, e
	}
	var id int64
	e = find.QueryRowContext(w.ctx, parent, name).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) && create {
		var insert *sql.Stmt
		if insert, e = w.statement(&w.insert, "INSERT INTO dirs(parent,name) VALUES(?,?)"); e == nil {
			var result sql.Result
			if result, e = insert.ExecContext(w.ctx, parent, name); e == nil {
				id, e = result.LastInsertId()
			}
		}
	}
	if errors.Is(e, sql.ErrNoRows) {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	w.ids[p] = id
	return id, true, nil
}

func (w *indexWriter) store(v IndexEntry) error {
	data, e := encodeIndexEntry(v)
	if e != nil {
		return e
	}
	id, _, e := w.dir(path.Dir(v.Path), true)
	if e != nil {
		return e
	}
	put, e := w.statement(&w.put, "INSERT INTO entries(dir,name,data) VALUES(?,?,?) ON CONFLICT(dir,name) DO UPDATE SET data=excluded.data")
	if e != nil {
		return e
	}
	_, e = put.ExecContext(w.ctx, id, path.Base(v.Path), data)
	return e
}

func (w *indexWriter) delete(p string) error {
	if _, e := NormalizePath(p); e != nil {
		return e
	}
	parent := path.Dir(p)
	id, ok, e := w.dir(parent, false)
	if e != nil || !ok {
		return e
	}
	remove, e := w.statement(&w.remove, "DELETE FROM entries WHERE dir=? AND name=?")
	if e != nil {
		return e
	}
	if _, e = remove.ExecContext(w.ctx, id, path.Base(p)); e != nil {
		return e
	}
	if parent != "." {
		w.emptied[parent] = true
	}
	return nil
}

// close removes each directory row a delete left without entries or child
// directories, deepest first, then continues with its parent. Without
// deletes it does nothing.
func (w *indexWriter) close() error {
	pending := make([]string, 0, len(w.emptied))
	for p := range w.emptied {
		pending = append(pending, p)
	}
	for len(pending) > 0 {
		sort.Slice(pending, func(a, b int) bool { return strings.Count(pending[a], "/") > strings.Count(pending[b], "/") })
		p := pending[0]
		pending = pending[1:]
		id, ok, e := w.dir(p, false)
		if e != nil {
			return e
		}
		if !ok {
			continue
		}
		prune, e := w.statement(&w.prune, "DELETE FROM dirs WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM entries WHERE dir=?1) AND NOT EXISTS(SELECT 1 FROM dirs WHERE parent=?1)")
		if e != nil {
			return e
		}
		result, e := prune.ExecContext(w.ctx, id)
		if e != nil {
			return e
		}
		if n, _ := result.RowsAffected(); n == 0 {
			continue
		}
		delete(w.ids, p)
		if parent := path.Dir(p); parent != "." && !w.emptied[parent] {
			w.emptied[parent] = true
			pending = append(pending, parent)
		}
	}
	w.emptied = map[string]bool{}
	return nil
}

// storeIndex stores and deletes entries in one transaction.
func storeIndex(ctx context.Context, db *sql.DB, store []IndexEntry, del []string) error {
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	w := newIndexWriter(ctx, tx)
	for _, p := range del {
		if e = w.delete(p); e != nil {
			return e
		}
	}
	for _, v := range store {
		if e = w.store(v); e != nil {
			return e
		}
	}
	if e = w.close(); e != nil {
		return e
	}
	return tx.Commit()
}
