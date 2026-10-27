package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func openIndexDB(t *testing.T, p string) *sql.DB {
	t.Helper()
	db, e := sql.Open("sqlite", p)
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func freshIndex(t *testing.T) *sql.DB {
	t.Helper()
	db := openIndexDB(t, filepath.Join(t.TempDir(), "index.sqlite"))
	if e := initializeIndex(db); e != nil {
		t.Fatal(e)
	}
	return db
}

func loadAll(t *testing.T, db *sql.DB) map[string]IndexEntry {
	t.Helper()
	out := map[string]IndexEntry{}
	if e := loadIndex(db, func(v IndexEntry) error {
		if _, ok := out[v.Path]; ok {
			t.Fatalf("duplicate %s", v.Path)
		}
		out[v.Path] = v
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return out
}

func filled(b byte) (h [32]byte, blob [16]byte) {
	for i := range h {
		h[i] = b + byte(i)
	}
	for i := range blob {
		blob[i] = b ^ byte(i+1)
	}
	return
}

func sampleEntries() []IndexEntry {
	h, blob := filled(7)
	return []IndexEntry{
		{Path: "a.txt", Local: "a.txt", Hash: h, Version: 3, Size: 12, Mode: 0644, Dev: 1, Inode: 2},
		{Path: "docs", Local: "docs", Directory: true, Version: 1, Mode: 0755, Dev: 16777220, Inode: math.MaxUint64},
		{Path: "docs/remote.bin", Local: "docs/remote.bin", Hash: h, Blob: blob, Version: math.MaxUint64, Size: math.MaxInt64, Mode: math.MaxUint32, Placeholder: true, RemoteOnly: true},
		{Path: "docs/deep/x/y/placeholder", Local: "docs/deep/x/y/placeholder (case conflict 0123).txt", Hash: h, Blob: blob, Version: 9, Size: 1, Mode: 0600, Placeholder: true, Dev: 5, Inode: 6},
		{Path: "gone", Local: "gone", Deleted: true, Version: 4},
		{Path: "waiting", Local: "waiting", Awaiting: true},
		{Path: "zero-ids", Local: "zero-ids", Placeholder: true, Version: 1},
	}
}

func TestIndexCodecRoundTrip(t *testing.T) {
	for _, v := range sampleEntries() {
		b, e := encodeIndexEntry(v)
		if e != nil {
			t.Fatal(v.Path, e)
		}
		got, e := decodeIndexEntry(v.Path, b)
		if e != nil || !reflect.DeepEqual(got, v) {
			t.Fatalf("%s: %+v %v", v.Path, got, e)
		}
		// Every strict prefix must be rejected rather than read short.
		for n := range b {
			if _, e := decodeIndexEntry(v.Path, b[:n]); !errors.Is(e, ErrInvalid) {
				t.Fatalf("%s: prefix %d accepted: %v", v.Path, n, e)
			}
		}
	}
	h, blob := filled(1)
	remote := IndexEntry{Path: "x", Local: "x", Hash: h, Blob: blob, Version: 1 << 40, Size: 1 << 30, Mode: 0644, Placeholder: true, RemoteOnly: true}
	if b, _ := encodeIndexEntry(remote); len(b) > 70 {
		t.Fatalf("remote-only placeholder record is %d bytes", len(b))
	}
	remote.Dev, remote.Inode, remote.RemoteOnly = 1<<24, 1<<40, false
	if b, _ := encodeIndexEntry(remote); len(b) > 80 {
		t.Fatalf("physical placeholder record is %d bytes", len(b))
	}
}

func TestIndexCodecRejectsInvalid(t *testing.T) {
	h, blob := filled(3)
	valid := IndexEntry{Path: "p", Local: "p", Hash: h, Blob: blob, Version: 1, Size: 2, Mode: 3, Dev: 4, Inode: 5}
	for _, bad := range []IndexEntry{
		{Path: "../p", Local: "p"},
		{Path: "p", Local: ""},
		{Path: "p", Local: "/abs"},
		{Path: "p", Local: "a/../b"},
		{Path: "p", Local: "p", Size: -1},
	} {
		if _, e := encodeIndexEntry(bad); e == nil {
			t.Fatalf("encoded %+v", bad)
		}
	}
	header := func(flags uint16) []byte {
		return binary.LittleEndian.AppendUint16([]byte{indexCodec}, flags)
	}
	base := func(flags uint16, rest ...byte) []byte {
		return append(append(header(flags), 1, 2, 3), rest...)
	}
	good, _ := encodeIndexEntry(valid)
	cases := map[string][]byte{
		"empty":            nil,
		"version":          append([]byte{2}, good[1:]...),
		"zero version":     append([]byte{0}, good[1:]...),
		"unknown flag":     base(entryFlags + 1),
		"high flag":        base(1 << 15),
		"overlong varint":  append(header(0), 0x81, 0x00, 2, 3),
		"overflow varint":  append(header(0), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02, 2, 3),
		"size overflow":    append(binary.AppendUvarint(append(header(0), 1), math.MaxInt64+1), 3),
		"mode overflow":    binary.AppendUvarint(append(header(0), 1, 2), math.MaxUint32+1),
		"zero hash":        base(entryHash, make([]byte, 32)...),
		"zero blob":        base(entryBlob, make([]byte, 16)...),
		"zero identity":    base(entryDevice, 0, 0),
		"short hash":       base(entryHash, 1, 2),
		"alias is path":    base(entryLocal, 1, 'p'),
		"alias traversal":  base(entryLocal, 4, '.', '.', '/', 'x'),
		"alias reserved":   base(entryLocal, 10, '.', 'd', 'r', 'i', 'v', 'e', 's', 'y', 'n', 'c'),
		"alias empty":      base(entryLocal, 0),
		"alias overlength": base(entryLocal, 9, 'a'),
		"alias huge":       binary.AppendUvarint(base(entryLocal), math.MaxUint64),
		"trailing":         append(bytes.Clone(good), 0),
	}
	for name, b := range cases {
		if v, e := decodeIndexEntry("p", b); !errors.Is(e, ErrInvalid) {
			t.Errorf("%s accepted: %+v %v", name, v, e)
		}
	}
	for _, p := range []string{"", "../x", "a//b", "/a", ".drivesync-root", "a\x00b"} {
		if _, e := decodeIndexEntry(p, good); !errors.Is(e, ErrInvalid) {
			t.Errorf("path %q accepted", p)
		}
	}
}

func TestIndexStoreDeleteAndPrune(t *testing.T) {
	db := freshIndex(t)
	ctx := context.Background()
	entries := sampleEntries()
	if e := storeIndex(ctx, db, entries, nil); e != nil {
		t.Fatal(e)
	}
	want := map[string]IndexEntry{}
	for _, v := range entries {
		want[v.Path] = v
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
	// An update keeps one row; a delete of a missing path is a no-op.
	entries[0].Version++
	want[entries[0].Path] = entries[0]
	if e := storeIndex(ctx, db, entries[:1], []string{"never/was", "missing"}); e != nil {
		t.Fatal(e)
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
	dirNames := func() []string {
		rows, e := db.Query("SELECT name FROM dirs ORDER BY name")
		if e != nil {
			t.Fatal(e)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var n string
			rows.Scan(&n)
			out = append(out, n)
		}
		return out
	}
	if got := dirNames(); !reflect.DeepEqual(got, []string{"deep", "docs", "x", "y"}) {
		t.Fatalf("dirs %v", got)
	}
	// Deleting the only deep file prunes its empty chain, but not docs,
	// which still holds an entry.
	delete(want, "docs/deep/x/y/placeholder")
	if e := storeIndex(ctx, db, nil, []string{"docs/deep/x/y/placeholder"}); e != nil {
		t.Fatal(e)
	}
	if got := dirNames(); !reflect.DeepEqual(got, []string{"docs"}) {
		t.Fatalf("dirs %v", got)
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
	// Delete and recreate in one transaction keeps the directory.
	h, _ := filled(9)
	again := IndexEntry{Path: "docs/new", Local: "docs/new", Hash: h}
	want[again.Path] = again
	delete(want, "docs/remote.bin")
	if e := storeIndex(ctx, db, []IndexEntry{again}, []string{"docs/remote.bin"}); e != nil {
		t.Fatal(e)
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
}

func TestIndexWriterRollbackLeavesNoDirectory(t *testing.T) {
	db := freshIndex(t)
	ctx := context.Background()
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	w := newIndexWriter(ctx, tx)
	if e = w.store(IndexEntry{Path: "a/b/c", Local: "a/b/c"}); e != nil {
		t.Fatal(e)
	}
	if e = w.store(IndexEntry{Path: "a/b/bad", Local: "../escape"}); e == nil {
		t.Fatal("stored an escaping local path")
	}
	tx.Rollback()
	var n int
	if db.QueryRow("SELECT (SELECT COUNT(*) FROM dirs)+(SELECT COUNT(*) FROM entries)").Scan(&n); n != 0 {
		t.Fatalf("rolled-back rows remain: %d", n)
	}
	if e = storeIndex(ctx, db, []IndexEntry{{Path: "a/b/c", Local: "a/b/c"}, {Path: "a/d", Local: "a/d"}}, nil); e != nil {
		t.Fatal(e)
	}
	if got := loadAll(t, db); len(got) != 2 || got["a/b/c"].Local != "a/b/c" {
		t.Fatalf("%+v", got)
	}
}

func TestIndexLoadRejectsCorruptTree(t *testing.T) {
	good, _ := encodeIndexEntry(IndexEntry{Path: "x", Local: "x"})
	for name, setup := range map[string][]string{
		"orphan entry":     {"INSERT INTO entries VALUES(7,'x',?)"},
		"orphan dir":       {"INSERT INTO dirs VALUES(2,9,'a')", "INSERT INTO entries VALUES(2,'x',?)"},
		"cycle":            {"INSERT INTO dirs VALUES(2,3,'a')", "INSERT INTO dirs VALUES(3,2,'b')"},
		"self parent":      {"INSERT INTO dirs VALUES(2,2,'a')"},
		"slash dir":        {"INSERT INTO dirs VALUES(2,0,'a/b')"},
		"dotdot dir":       {"INSERT INTO dirs VALUES(2,0,'..')"},
		"reserved dir":     {"INSERT INTO dirs VALUES(2,0,'.drivesync-state')"},
		"slash name":       {"INSERT INTO entries VALUES(0,'a/x',?)"},
		"empty name":       {"INSERT INTO entries VALUES(0,'',?)"},
		"bad record":       {"INSERT INTO entries VALUES(0,'x',X'01')"},
		"negative dir id":  {"INSERT INTO dirs VALUES(-1,0,'a')"},
		"overlong segment": {"INSERT INTO dirs VALUES(2,0,'" + strings.Repeat("a", 256) + "')"},
	} {
		t.Run(name, func(t *testing.T) {
			db := freshIndex(t)
			for _, q := range setup {
				var e error
				if strings.Contains(q, "?") {
					_, e = db.Exec(q, good)
				} else {
					_, e = db.Exec(q)
				}
				if e != nil {
					t.Fatal(e)
				}
			}
			if e := loadIndex(db, func(IndexEntry) error { return nil }); !errors.Is(e, ErrInvalid) {
				t.Fatalf("loaded: %v", e)
			}
		})
	}
	db := freshIndex(t)
	if _, e := db.Exec("PRAGMA user_version=3"); e != nil {
		t.Fatal(e)
	}
	if e := initializeIndex(db); !errors.Is(e, ErrInvalid) {
		t.Fatalf("accepted a newer schema: %v", e)
	}
}

// legacyIndexFixture is the exact pre-version-2 schema and JSON, written as
// literals so the migration is not tested against the new type's encoding.
var legacyIndexFixture = []string{
	`CREATE TABLE entries (path TEXT PRIMARY KEY,data BLOB NOT NULL)`,
	`CREATE TABLE config (key TEXT PRIMARY KEY,value TEXT NOT NULL)`,
	`CREATE TABLE pending (id TEXT PRIMARY KEY,data BLOB NOT NULL)`,
	`CREATE TABLE tombstones (id TEXT PRIMARY KEY,version INTEGER NOT NULL)`,
	`INSERT INTO config VALUES('binding','f/dir'),('session','s1'),('version','12')`,
	`INSERT INTO pending VALUES('` + strings.Repeat("ab", 32) + `','{"Row":{"FolderID":"f","PathID":"p","BlobID":"b","Version":3},"Kind":"quarantine","RetryAt":"0001-01-01T00:00:00Z"}')`,
	`INSERT INTO tombstones VALUES('` + strings.Repeat("cd", 32) + `',7)`,
	`INSERT INTO entries VALUES('notes.txt','{"Placeholder":false,"RemoteOnly":false,"Size":5,"Row":null,"Dev":0,"Inode":0,"Deleted":false,"Awaiting":false,"Path":"notes.txt","Local":"notes.txt","Hash":"` + strings.Repeat("0f", 32) + `","Version":2,"Directory":false,"Mode":420}')`,
	`INSERT INTO entries VALUES('docs','{"Placeholder":false,"RemoteOnly":false,"Size":0,"Row":null,"Dev":16777220,"Inode":991,"Deleted":false,"Awaiting":false,"Path":"docs","Local":"docs","Hash":"directory","Version":1,"Directory":true,"Mode":493}')`,
	`INSERT INTO entries VALUES('docs/lazy.pdf','{"Placeholder":true,"RemoteOnly":false,"Size":4096,"Row":{"FolderID":"f","PathID":"` + strings.Repeat("11", 32) + `","BlobID":"` + strings.Repeat("a1", 16) + `","Version":5,"SealedSize":4200,"Metadata":"AAEC","Deleted":false,"DeletedAt":0},"Dev":16777220,"Inode":992,"Deleted":false,"Awaiting":false,"Path":"docs/lazy.pdf","Local":"docs/Lazy (case conflict 1111).pdf","Hash":"` + strings.Repeat("e2", 32) + `","Version":5,"Directory":false,"Mode":384}')`,
	`INSERT INTO entries VALUES('archive/old/blob.bin','{"Placeholder":true,"RemoteOnly":true,"Size":9,"Row":{"FolderID":"f","PathID":"` + strings.Repeat("22", 32) + `","BlobID":"` + strings.Repeat("b2", 16) + `","Version":8,"SealedSize":100,"Metadata":"AAEC"},"Dev":0,"Inode":0,"Deleted":false,"Awaiting":false,"Path":"archive/old/blob.bin","Local":"archive/old/blob.bin","Hash":"` + strings.Repeat("c3", 32) + `","Version":8,"Directory":false,"Mode":420}')`,
	`INSERT INTO entries VALUES('removed.txt','{"Placeholder":false,"RemoteOnly":false,"Size":0,"Row":null,"Dev":0,"Inode":0,"Deleted":true,"Awaiting":false,"Path":"removed.txt","Local":"removed.txt","Hash":"","Version":6,"Directory":false,"Mode":420}')`,
	`INSERT INTO entries VALUES('old-style','{"Deleted":false,"Awaiting":true,"Path":"old-style","Local":"old-style","Hash":"","Version":0,"Directory":false,"Mode":0}')`,
}

func legacyIndexWant() map[string]IndexEntry {
	h := func(b byte) (out [32]byte) {
		for i := range out {
			out[i] = b
		}
		return
	}
	blob := func(b byte) (out [16]byte) {
		for i := range out {
			out[i] = b
		}
		return
	}
	return map[string]IndexEntry{
		"notes.txt":            {Path: "notes.txt", Local: "notes.txt", Hash: h(0x0f), Size: 5, Version: 2, Mode: 0644},
		"docs":                 {Path: "docs", Local: "docs", Directory: true, Dev: 16777220, Inode: 991, Version: 1, Mode: 0755},
		"docs/lazy.pdf":        {Path: "docs/lazy.pdf", Local: "docs/Lazy (case conflict 1111).pdf", Hash: h(0xe2), Blob: blob(0xa1), Placeholder: true, Size: 4096, Dev: 16777220, Inode: 992, Version: 5, Mode: 0600},
		"archive/old/blob.bin": {Path: "archive/old/blob.bin", Local: "archive/old/blob.bin", Hash: h(0xc3), Blob: blob(0xb2), Placeholder: true, RemoteOnly: true, Size: 9, Version: 8, Mode: 0644},
		"removed.txt":          {Path: "removed.txt", Local: "removed.txt", Deleted: true, Version: 6, Mode: 0644},
		"old-style":            {Path: "old-style", Local: "old-style", Awaiting: true},
	}
}

func legacyIndex(t *testing.T, extra ...string) (*sql.DB, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "index.sqlite")
	db := openIndexDB(t, p)
	for _, q := range append(append([]string{}, legacyIndexFixture...), extra...) {
		if _, e := db.Exec(q); e != nil {
			t.Fatal(q, e)
		}
	}
	return db, p
}

func dumpTable(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, e := db.Query(q)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if e = rows.Scan(ptrs...); e != nil {
			t.Fatal(e)
		}
		out = append(out, fmt.Sprintf("%q", vals))
	}
	sort.Strings(out)
	return out
}

func TestIndexLegacyMigration(t *testing.T) {
	db, p := legacyIndex(t)
	pending := dumpTable(t, db, "SELECT * FROM pending")
	tombstones := dumpTable(t, db, "SELECT * FROM tombstones")
	config := dumpTable(t, db, "SELECT * FROM config")
	if e := initializeIndex(db); e != nil {
		t.Fatal(e)
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, legacyIndexWant()) {
		t.Fatalf("migrated %+v", got)
	}
	var version, legacy int
	db.QueryRow("PRAGMA user_version").Scan(&version)
	db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'legacy%' OR sql LIKE '%path TEXT%'").Scan(&legacy)
	if version != indexSchemaVersion || legacy != 0 {
		t.Fatalf("version %d legacy tables %d", version, legacy)
	}
	if !reflect.DeepEqual(dumpTable(t, db, "SELECT * FROM pending"), pending) || !reflect.DeepEqual(dumpTable(t, db, "SELECT * FROM tombstones"), tombstones) || !reflect.DeepEqual(dumpTable(t, db, "SELECT * FROM config"), config) {
		t.Fatal("migration changed pending, tombstones, or config")
	}
	var free int
	db.QueryRow("PRAGMA freelist_count").Scan(&free)
	if free != 0 {
		t.Fatalf("%d free pages after migration", free)
	}
	// A reopen finds version 2 and leaves the index alone.
	db.Close()
	db = openIndexDB(t, p)
	if e := initializeIndex(db); e != nil {
		t.Fatal(e)
	}
	if got := loadAll(t, db); !reflect.DeepEqual(got, legacyIndexWant()) {
		t.Fatalf("reopened %+v", got)
	}
}

func TestIndexLegacyMigrationFailureKeepsOldIndex(t *testing.T) {
	row := func(key, local, hash, extra string) string {
		return `INSERT INTO entries VALUES('` + key + `','{"Path":"bad","Local":"` + local + `","Hash":"` + hash + `"` + extra + `}')`
	}
	for name, q := range map[string]string{
		"hash":         row("bad", "bad", "xyz", ""),
		"upper hash":   row("bad", "bad", strings.Repeat("AB", 32), ""),
		"short hash":   row("bad", "bad", "abcd", ""),
		"blob":         row("bad", "bad", "", `,"Row":{"BlobID":"zz"}`),
		"key mismatch": row("other", "bad", "", ""),
		"traversal":    row("bad", "../bad", "", ""),
		"empty local":  row("bad", "", "", ""),
		"reserved":     row("bad", ".drivesync-state", "", ""),
		"negative":     row("bad", "bad", "", `,"Size":-1`),
		"mode":         row("bad", "bad", "", `,"Mode":4294967296`),
		"json":         `INSERT INTO entries VALUES('bad','{"Path":"bad"')`,
		"bad path":     `INSERT INTO entries VALUES('a//b','{"Path":"a//b","Local":"a/b"}')`,
	} {
		t.Run(name, func(t *testing.T) {
			db, _ := legacyIndex(t, q)
			before := dumpTable(t, db, "SELECT * FROM entries")
			if e := initializeIndex(db); e == nil {
				t.Fatal("migrated an invalid legacy index")
			}
			var version, dirs int
			db.QueryRow("PRAGMA user_version").Scan(&version)
			db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('dirs','legacy_entries')").Scan(&dirs)
			if version != 0 || dirs != 0 || !reflect.DeepEqual(dumpTable(t, db, "SELECT * FROM entries"), before) {
				t.Fatalf("failed migration left changes: version %d tables %d", version, dirs)
			}
		})
	}
}

// TestIndexDiskPerEntry bounds the steady-state SQLite cost of realistic
// remote-only placeholders, below 200 bytes per file.
func TestIndexDiskPerEntry(t *testing.T) {
	p := filepath.Join(t.TempDir(), "index.sqlite")
	db := openIndexDB(t, p)
	if e := initializeIndex(db); e != nil {
		t.Fatal(e)
	}
	const files = 20000
	var entries []IndexEntry
	for i := range files {
		h, blob := filled(byte(i))
		binary.LittleEndian.PutUint32(h[:], uint32(i))
		binary.LittleEndian.PutUint32(blob[:], uint32(i))
		p := fmt.Sprintf("projects/client-%02d/src/components/module_%03d/component_%05d.tsx", i%17, i%211, i)
		entries = append(entries, IndexEntry{Path: p, Local: p, Hash: h, Blob: blob, Version: uint64(100000 + i), Size: int64(1000 + i*37), Mode: 0644, Placeholder: true, RemoteOnly: true})
	}
	if e := storeIndex(context.Background(), db, entries, nil); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); e != nil {
		t.Fatal(e)
	}
	var pages, size, free int64
	db.QueryRow("PRAGMA page_count").Scan(&pages)
	db.QueryRow("PRAGMA page_size").Scan(&size)
	db.QueryRow("PRAGMA freelist_count").Scan(&free)
	per := float64((pages-free)*size) / files
	t.Logf("%.1f bytes per entry (%d pages of %d)", per, pages, size)
	if per > 200 {
		t.Fatalf("%.1f bytes per entry", per)
	}
}

// TestIndexLegacyReattach rewrites a real lazy replica's state into the JSON
// layout, reattaches, and fetches through the migrated placeholders.
func TestIndexLegacyReattach(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	files := map[string]string{"top.txt": "top", "docs/a.txt": "alpha", "docs/deep/b.txt": "bravo"}
	for p, data := range files {
		putRemote(t, base, f, k, p, 0, []byte(data), 0600)
	}
	d := t.TempDir()
	dir, state := filepath.Join(d, "files"), filepath.Join(d, "state")
	opts := Options{Manual: true, OnDemand: true, StateDir: state}
	r, e := Attach(syncCtx, base, f.ID, k, dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	for p := range files {
		assertPlaceholder(t, r, p)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	db := openIndexDB(t, filepath.Join(state, "index.sqlite"))
	before := loadAll(t, db)
	if len(before) < len(files) {
		t.Fatalf("index %+v", before)
	}
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{"DROP TABLE entries", "DROP TABLE dirs", "CREATE TABLE entries (path TEXT PRIMARY KEY,data BLOB NOT NULL)", "PRAGMA user_version=0"} {
		if _, e = tx.Exec(q); e != nil {
			t.Fatal(e)
		}
	}
	for _, v := range before {
		hash := ""
		if v.Directory {
			hash = "directory"
		} else if v.Hash != [32]byte{} {
			hash = fmt.Sprintf("%x", v.Hash)
		}
		var row map[string]any
		if v.Blob != [16]byte{} {
			row = map[string]any{"FolderID": f.ID, "PathID": "unused", "BlobID": fmt.Sprintf("%x", v.Blob), "Version": v.Version, "Metadata": "AAEC"}
		}
		data, _ := json.Marshal(map[string]any{"Path": v.Path, "Local": v.Local, "Hash": hash, "Version": v.Version, "Size": v.Size, "Mode": v.Mode, "Dev": v.Dev, "Inode": v.Inode, "Placeholder": v.Placeholder, "RemoteOnly": v.RemoteOnly, "Deleted": v.Deleted, "Awaiting": v.Awaiting, "Directory": v.Directory, "Row": row})
		if _, e = tx.Exec("INSERT INTO entries VALUES(?,?)", v.Path, data); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	db.Close()
	r, e = Attach(syncCtx, base, f.ID, k, dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	r.syncMu.Lock()
	after := maps.Clone(r.index)
	r.syncMu.Unlock()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("reattached index differs:\n%+v\n%+v", after, before)
	}
	fetchOK(t, r)
	for p, data := range files {
		if b, e := os.ReadFile(filepath.Join(dir, p)); e != nil || string(b) != data {
			t.Fatalf("%s: %q %v", p, b, e)
		}
	}
}
