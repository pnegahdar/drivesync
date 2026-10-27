package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// DRIVESYNC_INDEX_MEASURE=1 reports replica index disk and heap costs for
// realistic remote-only paths. It only uses Attach, Sync, Close, Status, and
// SQLite introspection, so it runs unchanged across index representations.
// DRIVESYNC_INDEX_MEASURE_DISK and DRIVESYNC_INDEX_MEASURE_MEMORY override
// the 10,000 and 100,000 file counts.
func TestIndexMeasure(t *testing.T) {
	if os.Getenv("DRIVESYNC_INDEX_MEASURE") != "1" {
		t.Skip("set DRIVESYNC_INDEX_MEASURE=1")
	}
	count := func(name string, fallback int) int {
		n := fallback
		if v := os.Getenv(name); v != "" {
			if _, e := fmt.Sscan(v, &n); e != nil || n <= 0 {
				t.Fatalf("%s=%q", name, v)
			}
		}
		return n
	}
	t.Run("disk", func(t *testing.T) {
		files := count("DRIVESYNC_INDEX_MEASURE_DISK", 10000)
		s, _ := testServer(t)
		f, k := folderFor(t, s.Client(owner), Limits{})
		dirs := seedMeasureRows(t, s, f, k, files)
		d := t.TempDir()
		c := &lazyCounter{Client: s.Client(owner)}
		start := time.Now()
		r := attachMeasure(t, c, f, k, d)
		if e := r.Sync(syncCtx); e != nil {
			r.Close()
			t.Fatal(e)
		}
		elapsed := time.Since(start)
		checkRemoteOnly(t, r, c, d)
		if e := r.Close(); e != nil {
			t.Fatal(e)
		}
		t.Logf("files=%d seeded_dirs=%d attach+sync=%s", files, dirs, elapsed)
		reportIndexDisk(t, filepath.Join(d, "state"), files)
	})
	t.Run("memory", func(t *testing.T) {
		files := count("DRIVESYNC_INDEX_MEASURE_MEMORY", 100000)
		s, _ := testServer(t)
		f, k := folderFor(t, s.Client(owner), Limits{})
		empty, emptyKey := folderFor(t, s.Client(owner), Limits{})
		dirs := seedMeasureRows(t, s, f, k, files)
		full, blank := t.TempDir(), t.TempDir()
		c := &lazyCounter{Client: s.Client(owner)}
		// Persist both indexes, then drop every Replica, Delta, and seed slice
		// before measuring a cold Attach that only loads the stored index.
		for _, p := range []struct {
			f   Folder
			k   FolderKey
			dir string
		}{{f, k, full}, {empty, emptyKey, blank}} {
			start := time.Now()
			r := attachMeasure(t, c, p.f, p.k, p.dir)
			if e := r.Sync(syncCtx); e != nil {
				r.Close()
				t.Fatal(e)
			}
			if p.dir == full {
				t.Logf("files=%d seeded_dirs=%d initial attach+sync=%s", files, dirs, time.Since(start))
				checkRemoteOnly(t, r, c, p.dir)
			}
			if e := r.Close(); e != nil {
				t.Fatal(e)
			}
		}
		reportIndexDisk(t, filepath.Join(full, "state"), files)
		measure := func(f Folder, k FolderKey, dir string) (live, sys, garbage int64, mallocs uint64, took time.Duration, status Status) {
			settleHeap()
			var before, after, released runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			r := attachMeasure(t, c, f, k, dir)
			took = time.Since(start)
			settleHeap()
			runtime.ReadMemStats(&after)
			status = r.Status()
			runtime.KeepAlive(r)
			if e := r.Close(); e != nil {
				t.Fatal(e)
			}
			r = nil
			settleHeap()
			runtime.ReadMemStats(&released)
			live = int64(after.HeapAlloc) - int64(before.HeapAlloc)
			sys = int64(after.Sys) - int64(before.Sys)
			garbage = int64(after.TotalAlloc-before.TotalAlloc) - live
			mallocs = after.Mallocs - before.Mallocs
			t.Logf("  heap before=%d attached=%d released=%d (released-before=%+d)", before.HeapAlloc, after.HeapAlloc, released.HeapAlloc, int64(released.HeapAlloc)-int64(before.HeapAlloc))
			return
		}
		var emptyLive []int64
		for i := 0; i < 3; i++ {
			live, sys, garbage, mallocs, took, _ := measure(empty, emptyKey, blank)
			emptyLive = append(emptyLive, live)
			t.Logf("empty attach #%d: live_heap=%+d sys=%+d transient=%d mallocs=%d load=%s", i+1, live, sys, garbage, mallocs, took)
		}
		var fullLive []int64
		for i := 0; i < 3; i++ {
			live, sys, garbage, mallocs, took, st := measure(f, k, full)
			fullLive = append(fullLive, live)
			t.Logf("full attach #%d: live_heap=%+d sys=%+d transient=%d mallocs=%d load=%s placeholders=%d", i+1, live, sys, garbage, mallocs, took, st.Placeholders)
		}
		base, got := median(emptyLive), median(fullLive)
		entries := files + dirs
		t.Logf("median live heap: empty=%d full=%d net=%d -> %.1f B/file, %.1f B/entry (files+dirs=%d)", base, got, got-base, float64(got-base)/float64(files), float64(got-base)/float64(entries), entries)
	})
}

func attachMeasure(t testing.TB, c Client, f Folder, k FolderKey, d string) *Replica {
	t.Helper()
	r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(d, "files"), Options{Manual: true, OnDemand: true, MaxFolderPlaceholders: 1, StateDir: filepath.Join(d, "state")})
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func settleHeap() {
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
}

func median(v []int64) int64 {
	v = append([]int64(nil), v...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v[len(v)/2]
}

// measurePath is exactly 61 bytes: ten clients, twelve months, and a
// five-digit report number.
func measurePath(i int) string {
	return fmt.Sprintf("projects/client-%02d/reports/2026/%02d/quarterly-report-%05d.pdf", (i/12)%10, i%12+1, i)
}

// seedMeasureRows writes authenticated file and ancestor directory rows in one
// authority transaction, with no blobs, and returns the directory count.
func seedMeasureRows(t testing.TB, s *Server, f Folder, k FolderKey, files int) int {
	t.Helper()
	dirs := map[string]bool{}
	for i := 0; i < files; i++ {
		for q := path.Dir(measurePath(i)); q != "."; q = path.Dir(q) {
			dirs[q] = true
		}
	}
	e := s.Meta.Transaction(context.Background(), func(m *Metadata) error {
		add := func(meta FileMetadata) error {
			pid, e := PathID(k, f.ID, meta.Path)
			if e != nil {
				return e
			}
			sealed, e := SealMetadata(k, f.ID, pid, meta)
			if e != nil {
				return e
			}
			m.Files[f.ID][pid] = Row{FolderID: f.ID, PathID: pid, BlobID: meta.BlobID, Metadata: sealed, Version: 1, SealedSize: SealedSize(meta.Size)}
			return nil
		}
		for p := range dirs {
			if e := add(FileMetadata{Path: p, BlobID: randomID(), Hash: "directory", Mode: 0755, Directory: true}); e != nil {
				return e
			}
		}
		var h [32]byte
		for i := 0; i < files; i++ {
			if _, e := rand.Read(h[:]); e != nil {
				return e
			}
			if e := add(FileMetadata{Path: measurePath(i), BlobID: randomID(), Hash: hex.EncodeToString(h[:]), Size: 40000 + int64(i*7919%4000000), Mode: 0644}); e != nil {
				return e
			}
		}
		folder := m.Folders[f.ID]
		folder.Folder.Version = 1
		m.Folders[f.ID] = folder
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return len(dirs)
}

func checkRemoteOnly(t testing.TB, r *Replica, c *lazyCounter, d string) {
	t.Helper()
	regular, folders := 0, 0
	e := filepath.WalkDir(filepath.Join(d, "files"), func(p string, entry fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			folders++
		} else if !strings.HasPrefix(entry.Name(), ".drivesync") {
			regular++
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if regular != 0 || c.downloads.Load() != 0 || c.reserves.Load() != 0 || len(r.Status().Errors) != 0 {
		t.Fatal("not remote-only:", regular, c.downloads.Load(), c.reserves.Load(), r.Status().Errors)
	}
	t.Logf("local tree: %d directories (incl. root), 0 files; status placeholders=%d", folders, r.Status().Placeholders)
}

// reportIndexDisk inspects a copy of a closed replica index so the measured
// state is never modified.
func reportIndexDisk(t testing.TB, state string, files int) {
	t.Helper()
	size := func(name string) int64 {
		info, e := os.Stat(filepath.Join(state, name))
		if e != nil {
			return 0
		}
		return info.Size()
	}
	main, wal, shm := size("index.sqlite"), size("index.sqlite-wal"), size("index.sqlite-shm")
	copyDir := t.TempDir()
	copied := filepath.Join(copyDir, "index.sqlite")
	in, e := os.Open(filepath.Join(state, "index.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	out, e := os.Create(copied)
	if e == nil {
		_, e = io.Copy(out, in)
		if ce := out.Close(); e == nil {
			e = ce
		}
	}
	in.Close()
	if e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", copied)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var pageSize, pages, free int64
	_ = db.QueryRow("PRAGMA page_size").Scan(&pageSize)
	_ = db.QueryRow("PRAGMA page_count").Scan(&pages)
	_ = db.QueryRow("PRAGMA freelist_count").Scan(&free)
	t.Logf("index.sqlite after Close: main=%d wal=%d shm=%d (page_size=%d pages=%d free=%d) -> %.1f B/file on disk", main, wal, shm, pageSize, pages, free, float64(main+wal+shm)/float64(files))
	onDisk := map[string]int64{}
	if rows, e := db.Query("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name"); e == nil {
		for rows.Next() {
			var name string
			var n int64
			if rows.Scan(&name, &n) == nil {
				onDisk[name] = n
			}
		}
		rows.Close()
	}
	tables := []string{}
	rows, e := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
		var name string
		if e = rows.Scan(&name); e != nil {
			t.Fatal(e)
		}
		tables = append(tables, name)
	}
	rows.Close()
	var payload int64
	for _, table := range tables {
		cols, e := db.Query(fmt.Sprintf("SELECT name, type FROM pragma_table_info('%s')", table))
		if e != nil {
			t.Fatal(e)
		}
		var names, types []string
		for cols.Next() {
			var name, typ string
			if e = cols.Scan(&name, &typ); e != nil {
				t.Fatal(e)
			}
			names, types = append(names, name), append(types, typ)
		}
		cols.Close()
		sums := make([]string, len(names))
		for i, n := range names {
			sums[i] = fmt.Sprintf("COALESCE(SUM(LENGTH(%q)),0)", n)
		}
		values := make([]any, len(names)+1)
		var n int64
		lengths := make([]int64, len(names))
		values[0] = &n
		for i := range lengths {
			values[i+1] = &lengths[i]
		}
		if e = db.QueryRow(fmt.Sprintf("SELECT COUNT(*), %s FROM %q", strings.Join(sums, ","), table)).Scan(values...); e != nil {
			t.Fatal(e)
		}
		parts := []string{}
		var total int64
		for i, name := range names {
			total += lengths[i]
			parts = append(parts, fmt.Sprintf("%s %s=%d", name, types[i], lengths[i]))
		}
		payload += total
		avg := 0.0
		if n > 0 {
			avg = float64(total) / float64(n)
		}
		line := fmt.Sprintf("table %s: rows=%d column_bytes=%d (%.1f B/row) [%s] table_btree_bytes=%d", table, n, total, avg, strings.Join(parts, " "), onDisk[table])
		// Files end in .pdf; report their average apart from directory rows.
		for _, name := range names {
			if name != "path" && name != "name" {
				continue
			}
			var fileRows int64
			fileBytes := make([]int64, len(names))
			targets := []any{&fileRows}
			for i := range fileBytes {
				targets = append(targets, &fileBytes[i])
			}
			q := fmt.Sprintf("SELECT COUNT(*), %s FROM %q WHERE %q LIKE '%%.pdf'", strings.Join(sums, ","), table, name)
			if db.QueryRow(q).Scan(targets...) == nil && fileRows > 0 {
				var all int64
				avgs := []string{}
				for i, c := range names {
					all += fileBytes[i]
					avgs = append(avgs, fmt.Sprintf("%s=%.1f", c, float64(fileBytes[i])/float64(fileRows)))
				}
				line += fmt.Sprintf("; .pdf rows=%d avg=%.1f B [%s], other rows=%d", fileRows, float64(all)/float64(fileRows), strings.Join(avgs, " "), n-fileRows)
			}
		}
		t.Log(line)
	}
	for name, n := range onDisk {
		if !contains(tables, name) {
			t.Logf("btree %s: pages_bytes=%d", name, n)
		}
	}
	t.Logf("all column payload=%d (%.1f B/file); on-disk overhead=%.1f B/file", payload, float64(payload)/float64(files), float64(main+wal+shm-payload)/float64(files))
	vacuumed := filepath.Join(copyDir, "vacuum.sqlite")
	if _, e = db.Exec("VACUUM INTO ?", vacuumed); e == nil {
		if info, e := os.Stat(vacuumed); e == nil {
			t.Logf("VACUUM INTO size=%d (%.1f B/file)", info.Size(), float64(info.Size())/float64(files))
		}
	}
}

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
