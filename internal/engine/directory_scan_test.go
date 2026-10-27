package engine

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
)

// legacyScan is the fs.WalkDir traversal ScanLocal used before directory
// handles. The handle walk must classify every entry the same way.
func legacyScan(r *Replica) (map[string]LocalFile, []string, error) {
	out := map[string]LocalFile{}
	r.placeholderSourcesNow()
	r.placeholderMoved = map[string]IndexEntry{}
	r.blockedLocal = map[string]bool{}
	rootInfo, e := r.root.Stat(".")
	if e != nil {
		return nil, nil, e
	}
	r.scanBounds = &dirBounds{dev: device(rootInfo), seen: map[string]error{}}
	defer func() { r.scanBounds = nil }()
	patterns := r.patterns()
	skipped := []string{}
	e = fs.WalkDir(r.root.FS(), ".", func(local string, d fs.DirEntry, e error) error {
		if e != nil {
			if local == "." {
				return e
			}
			r.blockLocal(local)
			skipped = append(skipped, local+": "+e.Error())
			return fs.SkipDir
		}
		if local == "." {
			return nil
		}
		if d.IsDir() {
			info, err := d.Info()
			if err == nil {
				err = r.directoryBoundary(local, info)
			}
			if err != nil {
				r.blockLocal(local)
				skipped = append(skipped, local+": "+err.Error())
				return fs.SkipDir
			}
		}
		if r.ignore(local, d.IsDir(), patterns) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 || (!d.IsDir() && !d.Type().IsRegular()) {
			r.blockLocal(local)
			skipped = append(skipped, local+": unsupported entry or symlink")
			return nil
		}
		p := r.remotePath(local)
		if _, err := NormalizePath(p); err != nil {
			r.blockLocal(local)
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if prior, ok := r.index[p]; d.IsDir() && ok && prior.Deleted && prior.Directory {
			return nil
		}
		v, err := r.inspectScan(p, local, nil, nil)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			r.blockLocal(local)
			skipped = append(skipped, local+": "+err.Error())
			return nil
		}
		if prior, ok := out[p]; ok {
			delete(out, p)
			r.blockLocal(local)
			r.blockLocal(prior.Local)
			return nil
		}
		if !r.localBlocked(p) {
			out[p] = v
		}
		return nil
	})
	return out, skipped, e
}

func scanSnapshot(t *testing.T, r *Replica, legacy bool) (map[string]LocalFile, map[string]bool, []string, map[string]IndexEntry) {
	t.Helper()
	var out map[string]LocalFile
	var skipped []string
	var e error
	if legacy {
		out, skipped, e = legacyScan(r)
	} else {
		out, e = r.ScanLocal()
		skipped = r.Status().Skipped
	}
	if e != nil {
		t.Fatal(e)
	}
	moved := map[string]IndexEntry{}
	for k, v := range r.placeholderMoved {
		moved[k] = v
	}
	blocked := map[string]bool{}
	for k, v := range r.blockedLocal {
		blocked[k] = v
	}
	skipped = append([]string(nil), skipped...)
	sort.Strings(skipped)
	return out, blocked, skipped, moved
}

func mkfile(t *testing.T, r *Replica, p string, data string, mode os.FileMode) {
	t.Helper()
	full := filepath.Join(r.dir, filepath.FromSlash(p))
	if e := os.MkdirAll(filepath.Dir(full), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(full, []byte(data), mode); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(full, mode); e != nil {
		t.Fatal(e)
	}
}

func trackPlaceholder(t *testing.T, r *Replica, p string) {
	t.Helper()
	info, e := os.Lstat(filepath.Join(r.dir, filepath.FromSlash(p)))
	if e != nil {
		t.Fatal(e)
	}
	r.Remember(IndexEntry{Path: p, Local: p, Hash: indexHash(hashBytes([]byte("remote " + p))), Size: 9, Mode: 0644, Version: 1, Placeholder: true, Dev: device(info), Inode: inodeOf(info)})
}

func TestHandleScanMatchesWalkDir(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := lazyReplica(t, c, f, k)
	for n := 0; n < 30; n++ {
		mkfile(t, r, fmt.Sprintf("d%d/sub%d/f%d", n%4, n%3, n), fmt.Sprint(n), 0644)
	}
	mkfile(t, r, "plain", "x", 0755)
	mkfile(t, r, "ph/tracked", "", 0)
	trackPlaceholder(t, r, "ph/tracked")
	mkfile(t, r, "ph/source", "", 0)
	trackPlaceholder(t, r, "ph/source")
	if e := os.Mkdir(filepath.Join(r.dir, "moved"), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(r.dir, "ph/source"), filepath.Join(r.dir, "moved/into")); e != nil {
		t.Fatal(e)
	}
	mkfile(t, r, "ph/copy", "", 0)
	mkfile(t, r, ".git/objects/x", "git", 0644)
	mkfile(t, r, "ign.tmp", "tmp", 0644)
	mkfile(t, r, "nested/.drivesync-root", "", 0600)
	mkfile(t, r, "nested/inner", "hidden", 0644)
	mkfile(t, r, "target/file", "t", 0644)
	if e := os.Symlink("target", filepath.Join(r.dir, "dirlink")); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("plain", filepath.Join(r.dir, "filelink")); e != nil {
		t.Fatal(e)
	}
	if e := syscall.Mkfifo(filepath.Join(r.dir, "d1", "fifo"), 0600); e != nil {
		t.Fatal(e)
	}
	mkfile(t, r, "bad:name/x", "u", 0644)
	mkfile(t, r, "trail./x", "u", 0644)
	mkfile(t, r, "locked/x", "l", 0644)
	if os.Geteuid() != 0 {
		if e := os.Chmod(filepath.Join(r.dir, "locked"), 0); e != nil {
			t.Fatal(e)
		}
		defer os.Chmod(filepath.Join(r.dir, "locked"), 0700)
	}
	r.Remember(IndexEntry{Path: "gone", Local: "gone", Hash: indexHash("directory"), Directory: true, Deleted: true, Version: 1})
	mkfile(t, r, "gone/kept", "k", 0644)

	wantOut, wantBlocked, wantSkipped, wantMoved := scanSnapshot(t, r, true)
	gotOut, gotBlocked, gotSkipped, gotMoved := scanSnapshot(t, r, false)
	if !reflect.DeepEqual(gotOut, wantOut) {
		for p, v := range wantOut {
			if gotOut[p] != v {
				t.Errorf("%s: got %+v want %+v", p, gotOut[p], v)
			}
		}
		for p, v := range gotOut {
			if _, ok := wantOut[p]; !ok {
				t.Errorf("extra %s %+v", p, v)
			}
		}
	}
	if !reflect.DeepEqual(gotBlocked, wantBlocked) {
		t.Errorf("blocked %v want %v", gotBlocked, wantBlocked)
	}
	// The handle walk reports the same entries, possibly with differently worded errors.
	if len(gotSkipped) != len(wantSkipped) {
		t.Errorf("skipped %q want %q", gotSkipped, wantSkipped)
	}
	for n := range gotSkipped {
		if n < len(wantSkipped) && strings.SplitN(gotSkipped[n], ":", 2)[0] != strings.SplitN(wantSkipped[n], ":", 2)[0] {
			t.Errorf("skipped %q want %q", gotSkipped[n], wantSkipped[n])
		}
	}
	if _, ok := gotMoved["moved/into"]; !ok || !reflect.DeepEqual(gotMoved, wantMoved) {
		t.Errorf("moved %v want %v", gotMoved, wantMoved)
	}
	if _, ok := gotOut["ph/copy"]; ok || !gotBlocked["ph/copy"] {
		t.Error("copied placeholder adopted")
	}
	for _, p := range []string{"nested/inner", "target/file", "plain"} {
		if _, ok := gotOut[p]; !ok && p != "nested/inner" {
			t.Errorf("missing %s", p)
		} else if ok && p == "nested/inner" {
			t.Error("walked into a foreign attachment")
		}
	}
	if _, ok := gotOut["dirlink/file"]; ok {
		t.Error("followed a directory symlink")
	}
}

// Sources are derived from the walk, so a tracked placeholder only counts as
// moved when it is really gone, including inside a subtree the walk skipped.
func TestPlaceholderSourcesSkipUnwalkedSubtrees(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := lazyReplica(t, c, f, k)
	mkfile(t, r, "a/ph", "", 0)
	trackPlaceholder(t, r, "a/ph")
	mkfile(t, r, "a/.drivesync-state", "", 0600)
	if _, e := r.ScanLocal(); e != nil {
		t.Fatal(e)
	}
	if len(r.placeholderSources) != 0 {
		t.Fatal("placeholder behind a marker treated as gone", r.placeholderSources)
	}
	if e := os.Remove(filepath.Join(r.dir, "a/.drivesync-state")); e != nil {
		t.Fatal(e)
	}
	if e := os.Remove(filepath.Join(r.dir, "a/ph")); e != nil {
		t.Fatal(e)
	}
	if _, e := r.ScanLocal(); e != nil {
		t.Fatal(e)
	}
	if len(r.placeholderSources) != 1 {
		t.Fatal("removed placeholder not a source", r.placeholderSources)
	}
	r.Remember(IndexEntry{Path: "remote", Local: "remote", Placeholder: true, RemoteOnly: true, Size: 1, Version: 1})
	if _, e := r.ScanLocal(); e != nil {
		t.Fatal(e)
	}
	if len(r.placeholderSources) != 1 {
		t.Fatal("remote-only entry treated as a moved placeholder", r.placeholderSources)
	}
}

func TestSafeAndVerifiedDirectories(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := lazyReplica(t, c, f, k)
	mkfile(t, r, "real/sub/file", "x", 0644)
	mkfile(t, r, "marked/.drivesync-root", "", 0600)
	if e := os.Symlink("real", filepath.Join(r.dir, "link")); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("sub", filepath.Join(r.dir, "real", "sublink")); e != nil {
		t.Fatal(e)
	}
	for p, want := range map[string]error{
		"real/sub/file":         nil,
		"real/sub/missing/deep": nil,
		"absent/x":              nil,
		"link/sub/file":         errLocalObstacle,
		"real/sublink/file":     errLocalObstacle,
		"real/sublink":          errLocalObstacle,
		"real/sub/file/child":   errLocalObstacle,
		"marked/x":              errLocalObstacle,
		"../escape":             ErrInvalid,
	} {
		if e := r.safe(p); !errors.Is(e, want) && !(want == nil && e == nil) {
			t.Errorf("safe(%s) = %v, want %v", p, e, want)
		}
	}
	if d, e := r.openVerifiedDir("link/new", true, nil); e == nil {
		d.Close()
		t.Fatal("created through a symlink")
	}
	if _, e := os.Lstat(filepath.Join(r.dir, "real", "new")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("wrote beneath the symlink target", e)
	}
	if d, e := r.openVerifiedDir("marked/new", true, nil); e == nil {
		d.Close()
		t.Fatal("created inside a foreign attachment")
	}
	if _, e := os.Lstat(filepath.Join(r.dir, "marked", "new")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal(e)
	}
	d, e := r.openVerifiedDir("real/sub", false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	if _, e = r.verifiedDirID("real/sub", d.id()); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(filepath.Join(r.dir, "real/sub"), filepath.Join(r.dir, "real/moved")); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink("moved", filepath.Join(r.dir, "real/sub")); e != nil {
		t.Fatal(e)
	}
	if _, e = r.verifiedDirID("real/sub", d.id()); e == nil {
		t.Fatal("symlink to the same directory verified")
	}
	os.Remove(filepath.Join(r.dir, "real/sub"))
	if e = os.Mkdir(filepath.Join(r.dir, "real/sub"), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = r.verifiedDirID("real/sub", d.id()); !errors.Is(e, ErrBusy) {
		t.Fatal("replacement directory verified", e)
	}
}

// exchangeDuringBarrier swaps dir out while flush syncs the first directory,
// after placeholders were created through the old parent handle.
func exchangeDuringBarrier(t *testing.T, r *Replica, dir string, replace func(old, moved string)) {
	original := syncDirectoryFile
	t.Cleanup(func() { syncDirectoryFile = original })
	done := false
	syncDirectoryFile = func(f *os.File) error {
		if !done {
			done = true
			old := filepath.Join(r.dir, filepath.FromSlash(dir))
			moved := filepath.Join(r.dir, "untracked")
			if e := os.Rename(old, moved); e != nil {
				t.Fatal(e)
			}
			replace(old, moved)
		}
		return original(f)
	}
}

func TestPlaceholderBatchRejectsExchangedParent(t *testing.T) {
	for name, replace := range map[string]func(old, moved string){
		"symlink": func(old, moved string) {
			if e := os.Symlink("../untracked", old); e != nil {
				t.Fatal(e)
			}
		},
		"directory": func(old, moved string) {
			if e := os.Mkdir(old, 0700); e != nil {
				t.Fatal(e)
			}
			// Same placeholder inodes, different parent: only the
			// directory identity distinguishes it.
			for _, p := range []string{"a", "b"} {
				if e := os.Link(filepath.Join(moved, p), filepath.Join(old, p)); e != nil {
					t.Fatal(e)
				}
			}
		},
		"removed": func(old, moved string) {},
	} {
		t.Run(name, func(t *testing.T) {
			s, _ := testServer(t)
			base := s.Client(owner)
			f, k := folderFor(t, base, Limits{})
			for _, p := range []string{"top/dir/a", "top/dir/b"} {
				putRemote(t, base, f, k, p, 0, []byte(p), 0600)
			}
			c := &lazyCounter{Client: base}
			r := lazyReplica(t, c, f, k)
			exchangeDuringBarrier(t, r, "top/dir", replace)
			if e := r.Sync(syncCtx); !errors.Is(e, errPullIndex) {
				t.Fatal("exchanged parent acknowledged", e)
			}
			if indexRowCount(t, r) != 0 || r.version != 0 {
				t.Fatal("index or cursor advanced", indexRowCount(t, r), r.version)
			}
			for _, p := range []string{"top/dir/a", "top/dir/b"} {
				if _, ok := r.index[p]; ok {
					t.Fatal("tentative entry kept", p)
				}
			}
			if c.uploads.Load() != 0 || c.downloads.Load() != 0 {
				t.Fatal("transfers", c.uploads.Load(), c.downloads.Load())
			}
		})
	}
}

func TestPlaceholderInKeepsExistingFiles(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	r := lazyReplica(t, c, f, k)
	mkfile(t, r, "dir/user", "mine", 0644)
	mkfile(t, r, "dir/orphan", "", 0)
	if e := os.Symlink("user", filepath.Join(r.dir, "dir", "link")); e != nil {
		t.Fatal(e)
	}
	d, e := r.openVerifiedDir("dir", false, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	never := func(os.FileInfo) bool { return false }
	for _, name := range []string{"user", "link"} {
		if _, e := placeholderIn(d, name, emptyPlaceholder); !errors.Is(e, ErrBusy) {
			t.Fatal(name, e)
		}
	}
	if data, _ := os.ReadFile(filepath.Join(r.dir, "dir/user")); string(data) != "mine" {
		t.Fatal("user file changed", string(data))
	}
	if _, e := placeholderIn(d, "orphan", never); !errors.Is(e, ErrBusy) {
		t.Fatal(e)
	}
	info, e := placeholderIn(d, "orphan", emptyPlaceholder)
	if e != nil || !emptyPlaceholder(info) {
		t.Fatal(info, e)
	}
	info, e = placeholderIn(d, "fresh", never)
	if e != nil || !emptyPlaceholder(info) {
		t.Fatal(info, e)
	}
	on, e := os.Lstat(filepath.Join(r.dir, "dir/fresh"))
	if e != nil || !os.SameFile(info, on) {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(filepath.Join(r.dir, "dir"))
	for _, v := range entries {
		if strings.HasPrefix(v.Name(), ".drivesync") {
			t.Fatal("temporary left", v.Name())
		}
	}
}

// A batch's later parents resync only directories that were not synced under
// the same identity earlier in the pull.
func TestPlaceholderBatchSyncsEachDirectoryOnce(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	for _, p := range []string{"x/one/a", "x/one/b", "x/two/a", "y/a"} {
		putRemote(t, base, f, k, p, 0, []byte(p), 0600)
	}
	r := lazyReplica(t, base, f, k)
	original := syncDirectoryFile
	defer func() { syncDirectoryFile = original }()
	synced := map[string]int{}
	syncDirectoryFile = func(file *os.File) error {
		rel, _ := filepath.Rel(r.dir, file.Name())
		synced[filepath.ToSlash(rel)]++
		return original(file)
	}
	if e := r.Sync(syncCtx); e != nil {
		t.Fatal(e)
	}
	for _, dir := range []string{".", "x", "x/one", "x/two", "y"} {
		if synced[dir] == 0 {
			t.Errorf("%s never synced: %v", dir, synced)
		}
	}
	if synced["x/one"] != 1 || synced["x/two"] != 1 || synced["y"] != 1 {
		t.Errorf("parents synced more than once: %v", synced)
	}
	for _, p := range []string{"x/one/a", "x/one/b", "x/two/a", "y/a"} {
		assertPlaceholder(t, r, p)
	}
}
