package engine

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

// verifiedDir is a directory reached from the attachment root one component
// at a time. Every component was a real directory, not a symlink, on the root
// device and without a foreign marker, and each opened handle is the inode
// that the no-follow stat saw. Children are addressed by one name, so no later
// operation through the handle resolves another path component.
//
// A handle stays bound to its inode even if the directory is renamed. Callers
// that acknowledge work done through one must first reopen the path and
// compare identities (see verifiedDirID).
type verifiedDir struct {
	local string
	root  *os.Root
	file  *os.File
	info  os.FileInfo
}

func (d *verifiedDir) id() placeholderID { return placeholderID{device(d.info), inodeOf(d.info)} }

func (d *verifiedDir) Close() {
	if d == nil {
		return
	}
	if d.file != nil {
		d.file.Close()
	}
	if d.root != nil && d.local != "." {
		d.root.Close()
	}
}

// rootDevice is the device of this traversal, or of the held root now.
func (r *Replica) rootDevice() (uint64, error) {
	if r.scanBounds != nil {
		return r.scanBounds.dev, nil
	}
	info, e := r.root.Stat(".")
	if e != nil {
		return 0, e
	}
	return device(info), nil
}

// boundaryAt is directoryBoundary for a directory already open as dir. The
// marker checks are single-component stats inside that handle.
func (r *Replica) boundaryAt(local string, info os.FileInfo, dir *os.Root, rootDev uint64) error {
	if info == nil || device(info) != rootDev {
		return fmt.Errorf("mount boundary: %s", local)
	}
	if r.scanBounds != nil {
		if e, ok := r.scanBounds.seen[local]; ok {
			return e
		}
	}
	var e error
	for _, name := range []string{rootMarker, stateMarker} {
		_, le := dir.Lstat(name)
		if le == nil {
			e = fmt.Errorf("foreign drivesync marker: %s", local)
			break
		}
		if !errors.Is(le, os.ErrNotExist) {
			e = le
			break
		}
	}
	if r.scanBounds != nil {
		r.scanBounds.seen[local] = e
	}
	return e
}

// openChild opens the single component name of parent as a directory and
// checks it against info, the no-follow stat already taken of that name.
func openChild(parent *os.Root, name string, info os.FileInfo) (*os.Root, error) {
	child, e := parent.OpenRoot(name)
	if e != nil {
		return nil, e
	}
	held, e := child.Stat(".")
	if e != nil || !os.SameFile(info, held) {
		child.Close()
		if e == nil {
			e = fmt.Errorf("%w: directory exchanged: %s", errLocalObstacle, name)
		}
		return nil, e
	}
	return child, nil
}

// openVerifiedDir walks local from the root without following any component.
// With create, missing components are made 0700 without syncing anything; the
// caller syncs before acknowledging anything beneath them. visit, when set,
// sees "." and then each component with the stat its handle was checked
// against, and whether this walk created it. "." is the held root.
func (r *Replica) openVerifiedDir(local string, create bool, visit func(string, os.FileInfo, bool)) (*verifiedDir, error) {
	if local != "." {
		if _, e := NormalizePath(local); e != nil {
			return nil, e
		}
	}
	rootDev, e := r.rootDevice()
	if e != nil {
		return nil, e
	}
	cur, curLocal := r.root, "."
	release := func() {
		if cur != r.root {
			cur.Close()
		}
	}
	info, e := r.root.Stat(".")
	if e != nil {
		return nil, e
	}
	if visit != nil {
		visit(".", info, false)
	}
	if local != "." {
		for _, name := range strings.Split(local, "/") {
			next := pathJoin(curLocal, name)
			s, e := cur.Lstat(name)
			created := false
			if create && errors.Is(e, os.ErrNotExist) {
				if e = cur.Mkdir(name, 0700); e == nil || errors.Is(e, os.ErrExist) {
					created = e == nil
					s, e = cur.Lstat(name)
				}
			}
			if e != nil {
				release()
				return nil, e
			}
			if s.Mode()&os.ModeSymlink != 0 {
				release()
				return nil, fmt.Errorf("%w: symlink component %s", errLocalObstacle, next)
			}
			if !s.IsDir() {
				release()
				return nil, fmt.Errorf("%w: not a directory: %s", errLocalObstacle, next)
			}
			child, e := openChild(cur, name, s)
			if e != nil {
				release()
				return nil, e
			}
			if e = r.boundaryAt(next, s, child, rootDev); e != nil {
				child.Close()
				release()
				return nil, fmt.Errorf("%w: %v", errLocalObstacle, e)
			}
			release()
			cur, curLocal, info = child, next, s
			if visit != nil {
				visit(next, s, created)
			}
		}
	}
	f, e := cur.Open(".")
	if e != nil {
		release()
		return nil, e
	}
	if held, se := f.Stat(); se != nil || !os.SameFile(info, held) {
		f.Close()
		release()
		if se == nil {
			se = fmt.Errorf("%w: directory exchanged: %s", errLocalObstacle, local)
		}
		return nil, se
	}
	return &verifiedDir{local: local, root: cur, file: f, info: info}, nil
}

// verifiedDirID reopens local without following components and requires it to
// be the directory recorded earlier. Work done through an older handle is
// acknowledged only after this passes, so a parent that was renamed away or
// exchanged for a symlink cannot publish into an untracked tree.
func (r *Replica) verifiedDirID(local string, want placeholderID) (*verifiedDir, error) {
	d, e := r.openVerifiedDir(local, false, nil)
	if e != nil {
		return nil, e
	}
	if d.id() != want {
		d.Close()
		return nil, ErrBusy
	}
	return d, nil
}

// dirCache keeps verified directory handles for a run of single-component
// operations, at most dirCacheLimit open at once. Each path keeps the identity
// its first handle had; a later handle for the same path must match it, or
// the operation fails with ErrBusy. A result obtained through the cache is a
// snapshot; whoever acknowledges work done through it re-verifies first.
type dirCache struct {
	r       *Replica
	ids     map[string]placeholderID
	handles map[string]*verifiedDir
}

const dirCacheLimit = 64

func (r *Replica) newDirCache() *dirCache {
	return &dirCache{r: r, ids: map[string]placeholderID{}, handles: map[string]*verifiedDir{}}
}

// dir returns the cached handle for dir, opening it (and with create, making
// missing components) on first use. visit runs only when a handle is opened.
func (c *dirCache) dir(dir string, create bool, visit func(string, os.FileInfo, bool)) (*verifiedDir, error) {
	if d, ok := c.handles[dir]; ok {
		return d, nil
	}
	if len(c.handles) >= dirCacheLimit {
		c.closeHandles()
	}
	d, e := c.r.openVerifiedDir(dir, create, visit)
	if e != nil {
		return nil, e
	}
	if id, ok := c.ids[dir]; ok && id != d.id() {
		d.Close()
		return nil, ErrBusy
	}
	c.ids[dir] = d.id()
	c.handles[dir] = d
	return d, nil
}

// Lstat is a no-follow stat of local as one name inside its parent's verified
// handle. A missing parent is os.ErrNotExist; it is never created.
func (c *dirCache) Lstat(local string) (os.FileInfo, error) {
	d, e := c.dir(path.Dir(local), false, nil)
	if e != nil {
		return nil, e
	}
	return d.root.Lstat(path.Base(local))
}

func (c *dirCache) closeHandles() {
	for _, d := range c.handles {
		d.Close()
	}
	c.handles = map[string]*verifiedDir{}
}

// Close releases every handle and forgets identities.
func (c *dirCache) Close() {
	c.closeHandles()
	c.ids = map[string]placeholderID{}
}

// physicalPlaceholder reports whether an index entry should have an empty
// file on disk. Remote-only entries have none.
func physicalPlaceholder(i IndexEntry) bool { return i.Placeholder && !i.Deleted && !i.RemoteOnly }

// placeholderSourcesFrom fills r.placeholderSources after a walk. A name the
// walk listed exactly is present. Anything else is confirmed with a rooted
// Lstat: it may sit in a subtree the walk skipped, and a case- or
// normalization-insensitive volume can resolve a differently spelled name.
func (r *Replica) placeholderSourcesFrom(seen map[string]bool) {
	r.placeholderSources = map[placeholderID]IndexEntry{}
	for _, i := range r.index {
		if !physicalPlaceholder(i) {
			continue
		}
		if seen[i.Local] {
			continue
		}
		if _, e := r.root.Lstat(i.Local); errors.Is(e, os.ErrNotExist) {
			r.placeholderSources[placeholderID{i.Dev, i.Inode}] = i
		}
	}
}

// localScan is one ScanLocal traversal through verified directory handles.
// Each entry costs one single-component Lstat; regular files are opened by
// name inside their parent, without resolving the path again.
type localScan struct {
	r           *Replica
	patterns    []string
	prior, next map[string]scanHash
	out         map[string]LocalFile
	skipped     []string
	rootDev     uint64
	seen        map[string]bool
}

func (s *localScan) skip(local string, e error) {
	s.r.blockLocal(local)
	s.skipped = append(s.skipped, local+": "+e.Error())
}

func (s *localScan) add(p string, v LocalFile) {
	r := s.r
	if prior, ok := s.out[p]; ok {
		delete(s.out, p)
		r.blockLocal(v.Local)
		r.blockLocal(prior.Local)
		r.reject(v.Local, v.Hash, fmt.Errorf("ambiguous remote path %s", p))
		r.reject(prior.Local, prior.Hash, fmt.Errorf("ambiguous remote path %s", p))
		return
	}
	if !r.localBlocked(p) {
		s.out[p] = v
	}
}

// walk lists dir in name order, like fs.WalkDir. A listing failure aborts the
// scan at the root and blocks the subtree anywhere else.
func (s *localScan) walk(dir *os.Root, local string) error {
	f, e := dir.Open(".")
	var entries []os.DirEntry
	if e == nil {
		entries, e = f.ReadDir(-1)
		f.Close()
	}
	if e != nil {
		if local == "." {
			return e
		}
		s.skip(local, e)
		return nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, d := range entries {
		s.seen[pathJoin(local, d.Name())] = true
	}
	for _, d := range entries {
		if e := s.visit(dir, d, pathJoin(local, d.Name())); e != nil {
			return e
		}
	}
	return nil
}

func (s *localScan) visit(dir *os.Root, d os.DirEntry, local string) error {
	r := s.r
	name := d.Name()
	info, le := dir.Lstat(name)
	isDir := d.IsDir()
	if le == nil {
		isDir = info.IsDir()
	}
	if isDir {
		if le != nil {
			s.skip(local, le)
			return nil
		}
		child, e := openChild(dir, name, info)
		if e == nil {
			if e = r.boundaryAt(local, info, child, s.rootDev); e != nil {
				child.Close()
			}
		}
		if e != nil {
			s.skip(local, e)
			return nil
		}
		defer child.Close()
		// Every ancestor was descended, so none of them is ignored.
		if r.ignoreOne(local, true, s.patterns) {
			return nil
		}
		p := r.remotePath(local)
		if _, err := NormalizePath(p); err != nil {
			r.blockLocal(local)
			r.reject(local, r.hashUnportable(local), fmt.Errorf("name is not portable; remove reserved characters/names or shorten components (255 bytes): %w", err))
			return nil
		}
		if prior, ok := r.index[p]; !ok || !prior.Deleted || !prior.Directory {
			s.add(p, LocalFile{Path: p, Local: local, Hash: "directory", Mode: uint32(info.Mode().Perm()), Directory: true})
		}
		return s.walk(child, local)
	}
	if r.ignoreOne(local, false, s.patterns) {
		return nil
	}
	mode := d.Type()
	if le == nil {
		mode = info.Mode().Type()
	}
	if mode&os.ModeSymlink != 0 || !mode.IsRegular() {
		r.blockLocal(local)
		s.skipped = append(s.skipped, local+": unsupported entry or symlink")
		return nil
	}
	p := r.remotePath(local)
	if _, err := NormalizePath(p); err != nil {
		r.blockLocal(local)
		r.reject(local, r.hashUnportable(local), fmt.Errorf("name is not portable; remove reserved characters/names or shorten components (255 bytes): %w", err))
		return nil
	}
	var v LocalFile
	e := le
	if e == nil {
		v, e = r.inspectStat(dir, name, p, local, info, s.prior, s.next)
	}
	if e != nil {
		if !errors.Is(e, os.ErrNotExist) {
			s.skip(local, e)
		}
		return nil
	}
	s.add(p, v)
	return nil
}

// scanTree is ScanLocal's traversal. The caller owns scanBounds, hashes,
// placeholder state, and status.
func (r *Replica) scanTree(patterns []string, prior, next map[string]scanHash, out map[string]LocalFile, skipped []string) ([]string, error) {
	s := &localScan{r: r, patterns: patterns, prior: prior, next: next, out: out, skipped: skipped, rootDev: r.scanBounds.dev, seen: map[string]bool{}}
	e := s.walk(r.root, ".")
	if e == nil && r.placeholderSources == nil {
		r.placeholderSourcesFrom(s.seen)
	}
	return s.skipped, e
}

// placeholderSourcesNow is the pre-walk rule: every tracked placeholder whose
// local path is gone. A walk computes it lazily, only when an untracked empty
// 0000 file needs it, and otherwise derives it from the listings it made.
func (r *Replica) placeholderSourcesNow() {
	r.placeholderSources = map[placeholderID]IndexEntry{}
	for _, i := range r.index {
		if !physicalPlaceholder(i) {
			continue
		}
		if _, e := r.root.Lstat(i.Local); errors.Is(e, os.ErrNotExist) {
			r.placeholderSources[placeholderID{i.Dev, i.Inode}] = i
		}
	}
}
