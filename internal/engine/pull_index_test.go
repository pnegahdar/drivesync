package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func indexRowCount(t *testing.T, r *Replica) int {
	t.Helper()
	var count int
	if e := r.db.QueryRow("SELECT COUNT(*) FROM entries").Scan(&count); e != nil {
		t.Fatal(e)
	}
	return count
}

func TestPlaceholderBatchSyncsParentsBeforeIndex(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"a", "b", "c"} {
		putRemote(t, base, f, k, p, 0, []byte(p), 0600)
	}
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	original := syncDirectoryFile
	defer func() { syncDirectoryFile = original }()
	parents := 0
	syncDirectoryFile = func(file *os.File) error {
		parents++
		if count := indexRowCount(t, r); count != 0 {
			t.Fatalf("index committed before parent barrier: %d", count)
		}
		for _, p := range []string{"a", "b", "c"} {
			assertPlaceholder(t, r, p)
		}
		return original(file)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if parents != 1 || indexRowCount(t, r) != 3 || r.Status().Placeholders != 3 || c.downloads.Load() != 0 {
		t.Fatal("batch result", parents, r.Status(), c.downloads.Load())
	}
}

func TestPlaceholderBatchRecoversUnindexedFiles(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "file", 0, []byte("remote"), 0700)
	c := &lazyCounter{Client: base}
	r := lazyReplica(t, c, f, k)
	// Crash after the empty inode and its directory entry exist, before either
	// an index entry or the pull cursor has committed.
	if e := os.WriteFile(filepath.Join(r.dir, "file"), nil, 0); e != nil {
		t.Fatal(e)
	}
	if e := r.syncParent("file"); e != nil {
		t.Fatal(e)
	}
	before, e := os.Lstat(filepath.Join(r.dir, "file"))
	if e != nil {
		t.Fatal(e)
	}
	dir, state := r.dir, r.opts.StateDir
	if r.version != 0 || indexRowCount(t, r) != 0 {
		t.Fatal("fixture advanced")
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	again, e := Attach(syncCtx, c, f.ID, k, dir, Options{Manual: true, OnDemand: true, StateDir: state})
	if e != nil {
		t.Fatal(e)
	}
	defer again.Close()
	if e = again.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	after, e := os.Lstat(filepath.Join(dir, "file"))
	if e != nil {
		t.Fatal(e)
	}
	if !os.SameFile(before, after) || again.Status().Placeholders != 1 || c.downloads.Load() != 0 || c.uploads.Load() != 0 {
		t.Fatal("orphan was not adopted", again.Status(), c.downloads.Load(), c.uploads.Load())
	}
	fetchOK(t, again, "file")
	if data, _ := os.ReadFile(filepath.Join(dir, "file")); string(data) != "remote" {
		t.Fatal(string(data))
	}
}

func TestPlaceholderBatchBarrierFailureLeavesIndexAndCursor(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"a", "b", "c"} {
		putRemote(t, base, f, k, p, 0, []byte("remote:"+p), 0600)
	}
	r := lazyReplica(t, base, f, k)
	original := syncDirectoryFile
	defer func() { syncDirectoryFile = original }()
	failed := errors.New("placeholder parent sync failed")
	syncDirectoryFile = func(*os.File) error { return failed }
	if e := r.Sync(syncCtx); !errors.Is(e, failed) {
		t.Fatal(e)
	}
	if len(r.index) != 0 || indexRowCount(t, r) != 0 || r.version != 0 || r.Status().Placeholders != 0 {
		t.Fatal("failed barrier acknowledged", r.index, r.version, r.Status())
	}
	for _, p := range []string{"a", "b", "c"} {
		assertPlaceholder(t, r, p)
	}
	syncDirectoryFile = original
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if indexRowCount(t, r) != 3 || r.Status().Placeholders != 3 {
		t.Fatal(r.Status())
	}
}

func TestPlaceholderBatchIndexRollbackRecovers(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"a", "b", "c"} {
		putRemote(t, base, f, k, p, 0, []byte(p), 0600)
	}
	r := lazyReplica(t, base, f, k)
	if _, e := r.db.Exec("CREATE TRIGGER fail_entry BEFORE INSERT ON entries WHEN NEW.path='b' BEGIN SELECT RAISE(FAIL,'injected index write failure'); END"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e == nil {
		t.Fatal("index failure ignored")
	}
	if len(r.index) != 0 || indexRowCount(t, r) != 0 || r.version != 0 || r.Status().Placeholders != 0 {
		t.Fatal("partial index survived rollback", r.index, r.version, r.Status())
	}
	if _, e := r.db.Exec("DROP TRIGGER fail_entry"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if indexRowCount(t, r) != 3 || r.Status().Placeholders != 3 {
		t.Fatal(r.Status())
	}
}

func TestPlaceholderBatchesStayBounded(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	source := replicaFor(t, base, f, k, "source", true)
	for n := 0; n < 1001; n++ {
		writeLocal(t, source, fmt.Sprintf("f%04d", n), "content")
	}
	if e := source.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := lazyReplica(t, base, f, k)
	r.opts.MaxFolderPlaceholders = 2000
	original := syncDirectoryFile
	defer func() { syncDirectoryFile = original }()
	var counts []int
	syncDirectoryFile = func(file *os.File) error { counts = append(counts, indexRowCount(t, r)); return original(file) }
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(counts, []int{0, 1000}) || indexRowCount(t, r) != 1001 {
		t.Fatal("unexpected publication batches", counts, indexRowCount(t, r))
	}
}

func TestFullPullIndexRollbackKeepsPublishedFiles(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"a", "b", "c"} {
		putRemote(t, base, f, k, p, 0, []byte("remote:"+p), 0600)
	}
	c := &lazyCounter{Client: base}
	r := replicaFor(t, c, f, k, "full", true)
	if _, e := r.db.Exec("CREATE TRIGGER fail_entry BEFORE INSERT ON entries WHEN NEW.path='b' BEGIN SELECT RAISE(FAIL,'injected index write failure'); END"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); !errors.Is(e, errPullIndex) {
		t.Fatal(e)
	}
	if len(r.index) != 0 || indexRowCount(t, r) != 0 || r.version != 0 || len(r.Status().Conflicts) != 0 || c.reserves.Load() != 0 {
		t.Fatal("uncommitted remote files treated as local edits", r.index, r.version, r.Status(), c.reserves.Load())
	}
	for _, p := range []string{"a", "b", "c"} {
		if data, e := os.ReadFile(filepath.Join(r.dir, p)); e != nil || string(data) != "remote:"+p {
			t.Fatal(p, string(data), e)
		}
	}
	if _, e := r.db.Exec("DROP TRIGGER fail_entry"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if indexRowCount(t, r) != 3 || c.downloads.Load() != 3 || c.uploads.Load() != 0 {
		t.Fatal("did not adopt durable downloads", c.downloads.Load(), c.uploads.Load())
	}
}

func TestUnindexedPlaceholderAcceptsRemoteDelete(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	row := putRemote(t, base, f, k, "file", 0, []byte("remote"), 0600)
	r := lazyReplica(t, base, f, k)
	if e := os.WriteFile(filepath.Join(r.dir, "file"), nil, 0); e != nil {
		t.Fatal(e)
	}
	if e := r.syncParent("file"); e != nil {
		t.Fatal(e)
	}
	if _, e := base.Commit(syncCtx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Lstat(filepath.Join(r.dir, "file")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("orphan survived its authenticated tombstone", e)
	}
	if !r.index["file"].Deleted || r.Status().Placeholders != 0 {
		t.Fatal(r.index, r.Status())
	}
}
