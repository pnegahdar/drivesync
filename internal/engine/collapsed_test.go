package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func collapsedReplica(t *testing.T, c Client, f Folder, k FolderKey, total, perFolder int) *Replica {
	t.Helper()
	d := t.TempDir()
	r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(d, "files"), Options{Manual: true, OnDemand: true, MaxPlaceholders: total, MaxFolderPlaceholders: perFolder, GuideName: "DRIVE-GUIDE.md", Guide: "Use drive get to read files.", StateDir: filepath.Join(d, "state")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestCollapsedFoldersFetchGuidesAndLocalWrites(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"a/one", "a/two", "a/three", "b/one"} {
		writeLocal(t, w, p, "remote:"+p)
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	counter := &lazyCounter{Client: c}
	r := collapsedReplica(t, counter, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if !r.collapsed["a"] || r.collapsed["b"] || counter.downloads.Load() != 0 {
		t.Fatal(r.collapsed, counter.downloads.Load())
	}
	if _, e := os.Stat(filepath.Join(r.dir, "a/one")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
	assertPlaceholder(t, r, "b/one")
	entries, e := r.Entries("a/*")
	if e != nil || len(entries) != 3 {
		t.Fatal(entries, e)
	}
	for _, i := range entries {
		if !i.Placeholder || i.Size != int64(len("remote:"+i.Path)) {
			t.Fatal(i)
		}
	}
	guide, e := os.ReadFile(filepath.Join(r.dir, "DRIVE-GUIDE.md"))
	if e != nil || !strings.Contains(string(guide), "a/ (3 files, 38 B)") {
		t.Fatal(string(guide), e)
	}
	if got := fetchOK(t, r, "a/one"); len(got.Fetched) != 1 || got.Bytes != 12 {
		t.Fatal(got)
	}
	if r.index["a/two"].RemoteOnly != true || r.index["a/one"].Placeholder {
		t.Fatal(r.index)
	}
	guide, _ = os.ReadFile(filepath.Join(r.dir, "a/DRIVE-GUIDE.md"))
	if strings.Contains(string(guide), "one (") || !strings.Contains(string(guide), "two (") {
		t.Fatal(string(guide))
	}
	for n := 0; n < 2; n++ {
		if e = r.Sync(syncCtx); e != nil {
			t.Fatal(e)
		}
	}
	if counter.reserves.Load() != 0 {
		t.Fatal("uploaded guides or absent remote rows", counter.reserves.Load())
	}
	old := r.index["a/two"].Version
	writeLocal(t, r, "a/two", "unseen local write")
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if r.index["a/two"].Version != old || len(r.Status().Conflicts) != 1 {
		t.Fatal(r.index["a/two"], r.Status())
	}
	assertPlaceholder(t, r, "a/two")
	data, e := os.ReadFile(filepath.Join(r.dir, r.Status().Conflicts[0]))
	if e != nil || string(data) != "unseen local write" {
		t.Fatal(string(data), e)
	}
	putRemote(t, c, f, k, "a/new", 0, []byte("new"), 0600)
	if e = r.Sync(syncCtx); e != nil || !r.index["a/new"].RemoteOnly {
		t.Fatal(e, r.index["a/new"])
	}
	r.opts.OnDemand = false
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if r.Status().Placeholders != 0 {
		t.Fatal(r.Status())
	}
	for _, p := range []string{"DRIVE-GUIDE.md", "a/DRIVE-GUIDE.md"} {
		if _, e = os.Stat(filepath.Join(r.dir, p)); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("guide survived", p, e)
		}
	}
}

func TestCollapsedBreadthFirstAndLaterLimit(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"a/one", "a/two", "b/one", "b/two"} {
		writeLocal(t, w, p, "bytes")
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 4, 10)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if r.collapsed["a"] || !r.collapsed["b"] {
		t.Fatal(r.collapsed)
	}
	assertPlaceholder(t, r, "a/one")
	if !r.index["b/one"].RemoteOnly {
		t.Fatal(r.index)
	}
	putRemote(t, c, f, k, "a/new", 0, []byte("new"), 0600)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if !r.index["a/new"].RemoteOnly || r.collapsed["a"] {
		t.Fatal(r.index["a/new"], r.collapsed)
	}
	// Expanding one requested file leaves its siblings remote-only.
	fetchOK(t, r, "b/one")
	if !r.index["b/two"].RemoteOnly {
		t.Fatal(r.index)
	}
}

func TestCollapsedFolderLocalDelete(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"a/one", "a/two", "a/three", "b/one"} {
		writeLocal(t, w, p, "bytes")
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.RemoveAll(filepath.Join(r.dir, "a")); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	d, e := c.Changes(syncCtx, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, row := range d.Rows {
		m, e := OpenMetadata(k, f.ID, row)
		if e != nil {
			t.Fatal(e)
		}
		if strings.HasPrefix(m.Path, "a/") || m.Path == "a" {
			if !row.Deleted {
				t.Fatal("subtree survived", m.Path)
			}
		}
	}
}

func TestGuideCrashBeforeIndexAndReservedRemoteName(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	for n := 0; n < 3; n++ {
		putRemote(t, c, f, k, fmt.Sprintf("file%d", n), 0, []byte("content"), 0600)
	}
	putRemote(t, c, f, k, "DRIVE-GUIDE.md", 0, []byte("remote guide collision"), 0600)
	counter := &lazyCounter{Client: c}
	r := collapsedReplica(t, counter, f, k, 20, 2)
	if _, e := r.db.Exec("CREATE TRIGGER fail_entry BEFORE INSERT ON entries BEGIN SELECT RAISE(FAIL, 'crash'); END"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); !errors.Is(e, errPullIndex) {
		t.Fatal(e)
	}
	if r.version != 0 || indexRowCount(t, r) != 0 {
		t.Fatal(r.version, r.index)
	}
	if guide, e := os.ReadFile(filepath.Join(r.dir, "DRIVE-GUIDE.md")); e != nil || !strings.Contains(string(guide), "file0") {
		t.Fatal(string(guide), e)
	}
	if _, e := r.db.Exec("DROP TRIGGER fail_entry"); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if len(r.index) != 3 || len(r.ignoredRows) != 1 || counter.reserves.Load() != 0 {
		t.Fatal(r.index, r.ignoredRows, counter.reserves.Load())
	}
	if _, e := r.root.Lstat("file0"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("root should be collapsed", e)
	}
}

func TestCollapsedImplicitFoldersRestartAndExpansion(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	for n := 0; n < 3; n++ {
		putRemote(t, c, f, k, fmt.Sprintf("big/nested/file%d", n), 0, []byte("content"), 0600)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if info, e := r.root.Lstat("big/nested"); e != nil || !info.IsDir() {
		t.Fatal(info, e)
	}
	opts, dir := r.opts, r.dir
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
	r, e := Attach(syncCtx, c, f.ID, k, dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if !r.collapsed["big/nested"] {
		t.Fatal("collapse lost on restart", r.collapsed)
	}
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	fetchOK(t, r, "big/nested")
	if r.collapsed["big/nested"] {
		t.Fatal("fully fetched folder stayed collapsed")
	}
	if _, e = r.root.Lstat("big/nested/DRIVE-GUIDE.md"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("expanded guide survived", e)
	}
}

func TestCollapsedDirectoryMoveHydratesBeforeRename(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"big/one", "big/two", "big/three"} {
		writeLocal(t, w, p, "content:"+p)
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(r.dir, "big"), filepath.Join(r.dir, "moved")); e != nil {
		t.Fatal(e)
	}
	for n := 0; n < 2; n++ {
		if e := r.Sync(syncCtx); e != nil {
			t.Fatal(e)
		}
	}
	for _, name := range []string{"one", "two", "three"} {
		data, e := os.ReadFile(filepath.Join(r.dir, "moved", name))
		if e != nil || string(data) != "content:big/"+name {
			t.Fatal(name, string(data), e)
		}
		if !r.index["big/"+name].Deleted || r.index["moved/"+name].Deleted || r.index["moved/"+name].Version == 0 {
			t.Fatal(r.index)
		}
	}
}

func TestCollapsedEmptyDirectoriesAndIgnoredFetch(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"one", "two", "three"} {
		if e := os.Mkdir(filepath.Join(w.dir, p), 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	guide, e := os.ReadFile(filepath.Join(r.dir, "DRIVE-GUIDE.md"))
	if e != nil || !strings.Contains(string(guide), "one/ (0 files, 0 B)") {
		t.Fatal(string(guide), e)
	}
	r.opts.Ignore = []string{"one/"}
	got, e := r.Fetch(syncCtx, "one")
	if e != nil || len(got.Failed) != 1 {
		t.Fatal(got, e)
	}
	if _, e = r.root.Lstat("one"); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("materialized ignored directory", e)
	}
	fetchOK(t, r, "two")
	if i, e := r.root.Lstat("two"); e != nil || !i.IsDir() {
		t.Fatal(i, e)
	}
}

func TestCollapsedContainerDeletionUsesMassRemovalGuard(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for n := 0; n < 6; n++ {
		writeLocal(t, w, fmt.Sprintf("big/file%d", n), "bytes")
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	for n := 0; n < 2; n++ {
		if e := r.Sync(syncCtx); e != nil {
			t.Fatal(e)
		}
	}
	if r.baselineFiles != 0 || r.baselineBytes != 0 {
		t.Fatal("remote-only absence entered baseline", r.baselineFiles, r.baselineBytes)
	}
	if e := os.RemoveAll(filepath.Join(r.dir, "big")); e != nil {
		t.Fatal(e)
	}
	if e := r.Sync(syncCtx); e == nil || !strings.Contains(e.Error(), "deletes paused") {
		t.Fatal("container removal bypassed guard", e)
	}
	for _, i := range r.index {
		if i.Deleted {
			t.Fatal("guard allowed subtree deletion", i)
		}
	}
	r.RetryRejected()
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if !r.index["big/file0"].Deleted {
		t.Fatal("acknowledged removal did not propagate")
	}
}

func TestGuideNameChangeRevisitsRemoteRowWithoutUploadingGuide(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	putRemote(t, base, f, k, "DRIVE-GUIDE.md", 0, []byte("remote contents"), 0600)
	c := &lazyCounter{Client: base}
	r := collapsedReplica(t, c, f, k, 20, 20)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	opts, dir := r.opts, r.dir
	opts.GuideName = "OTHER-GUIDE.md"
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
	r, e := Attach(syncCtx, c, f.ID, k, dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if e = r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	assertPlaceholder(t, r, "DRIVE-GUIDE.md")
	if c.reserves.Load() != 0 || len(r.Status().Conflicts) != 0 {
		t.Fatal("uploaded old guide", r.Status(), c.reserves.Load())
	}
	fetchOK(t, r, "DRIVE-GUIDE.md")
	data, e := os.ReadFile(filepath.Join(r.dir, "DRIVE-GUIDE.md"))
	if e != nil || string(data) != "remote contents" {
		t.Fatal(string(data), e)
	}
}

func TestRemoteOnlyDirectoryAdoptionNeedsParentBarrier(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	w := replicaFor(t, c, f, k, "writer", true)
	for _, p := range []string{"one", "two", "three"} {
		if e := os.Mkdir(filepath.Join(w.dir, p), 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e := w.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	r := collapsedReplica(t, c, f, k, 20, 2)
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(filepath.Join(r.dir, "one"), 0700); e != nil {
		t.Fatal(e)
	}
	barrier := syncDirectoryFile
	defer func() { syncDirectoryFile = barrier }()
	syncDirectoryFile = func(*os.File) error { return errors.New("directory barrier failed") }
	if e := r.prepareRemoteOnly(syncCtx); e == nil || !r.index["one"].RemoteOnly {
		t.Fatal("adopted before directory durability", e, r.index["one"])
	}
	syncDirectoryFile = barrier
	if e := r.prepareRemoteOnly(syncCtx); e != nil || r.index["one"].RemoteOnly {
		t.Fatal(e, r.index["one"])
	}
}
