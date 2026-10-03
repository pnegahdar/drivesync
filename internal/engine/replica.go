package engine

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/zeebo/blake3"
)

type Options struct {
	Name, StateDir                          string
	Debounce, RescanInterval, RetryInterval time.Duration
	Ignore                                  []string
	Manual                                  bool
}
type Rejection struct {
	Path, Reason string
	Hash         string
	RetryAt      time.Time
}
type QuarantinedRow struct {
	Path    string
	PathID  string
	Version uint64
}
type Status struct {
	Quarantined                      []QuarantinedRow
	PendingUpBytes, PendingDownBytes int64
	Conflicts                        []string
	Rejected                         []Rejection
	Skipped                          []string
	LastSync                         time.Time
	Errors                           []string
	Version                          uint64
	Watcher                          string
}
type IndexEntry struct {
	Deleted           bool
	Awaiting          bool
	Path, Local, Hash string
	Version           uint64
	Directory         bool
	Mode              uint32
}
type LocalFile struct {
	Path, Local, Hash string
	Size              int64
	Mode              uint32
	Directory         bool
}
type pendingRow struct {
	Row     Row
	Kind    string
	RetryAt time.Time
}
type Replica struct {
	identity        rootIdentity
	allowMassDelete bool
	tombstones      map[string]uint64
	retryAt         map[string]time.Time

	client       Client
	folder       string
	session      string
	key          FolderKey
	dir          string
	opts         Options
	root         *os.Root
	db           *sql.DB
	lock         *os.File
	index        map[string]IndexEntry
	byID         map[string]string
	byLocal      map[string]string
	byFold       map[string]string
	version      uint64
	syncMu       sync.Mutex
	mu           sync.Mutex
	status       Status
	quarantine   map[string]Row
	ignoredRows  map[string]Row
	retryRows    map[string]Row
	blockedLocal map[string]bool
	rejected     map[string]Rejection
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	watcherClose func()
	closed       sync.Once
}

func Attach(ctx context.Context, c Client, id string, k FolderKey, dir string, o Options) (*Replica, error) {
	f, e := c.GetFolder(ctx, id)
	if e != nil {
		return nil, e
	}
	if len(f.KeyCheck) != 64 || hex.EncodeToString(f.KeyCheck[:16]) != id {
		return nil, ErrKey
	}
	if e = CheckKey(k, f.KeyCheck); e != nil {
		return nil, e
	}
	if o.Name == "" {
		o.Name = "replica"
	}
	o.Name = safeReplicaName(o.Name)
	if o.Debounce <= 0 {
		o.Debounce = 150 * time.Millisecond
	}
	if o.RescanInterval <= 0 {
		o.RescanInterval = 30 * time.Second
	}
	if o.RetryInterval <= 0 {
		o.RetryInterval = time.Minute
	}
	dir, e = filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	if e = durableDirectory(dir); e != nil {
		return nil, e
	}
	dir, e = filepath.EvalSymlinks(dir)
	if e != nil {
		return nil, e
	}
	if e = checkAttachments(ctx, dir, id); e != nil {
		return nil, e
	}
	o.StateDir, e = prepareState(dir, id, o.StateDir)
	if e != nil {
		return nil, e
	}
	rel, e := filepath.Rel(dir, o.StateDir)
	if e != nil || rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..") {
		return nil, fmt.Errorf("state directory must be outside synced directory")
	}
	if e = os.Chmod(o.StateDir, 0700); e != nil {
		return nil, e
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		return nil, e
	}
	lock, e := lockState(filepath.Join(o.StateDir, "lock"))
	if e != nil {
		root.Close()
		return nil, e
	}
	db, e := sql.Open("sqlite", filepath.Join(o.StateDir, "index.sqlite"))
	if e != nil {
		lock.Close()
		root.Close()
		return nil, e
	}
	db.SetMaxOpenConns(1)
	cleanup := func() { db.Close(); lock.Close(); root.Close() }
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", `CREATE TABLE IF NOT EXISTS entries (path TEXT PRIMARY KEY,data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY,value TEXT NOT NULL)`, `CREATE TABLE IF NOT EXISTS pending (id TEXT PRIMARY KEY,data BLOB NOT NULL)`, `CREATE TABLE IF NOT EXISTS tombstones (id TEXT PRIMARY KEY,version INTEGER NOT NULL)`} {
		if _, e = db.Exec(q); e != nil {
			cleanup()
			return nil, e
		}
	}
	binding := id + "/" + dir
	var old string
	e = db.QueryRow("SELECT value FROM config WHERE key='binding'").Scan(&old)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		cleanup()
		return nil, e
	}
	if old != "" && old != binding {
		cleanup()
		return nil, ErrInvalid
	}
	if _, e = db.Exec("INSERT OR IGNORE INTO config(key,value) VALUES('binding',?)", binding); e != nil {
		cleanup()
		return nil, e
	}
	var session string
	e = db.QueryRow("SELECT value FROM config WHERE key='session'").Scan(&session)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		cleanup()
		return nil, e
	}
	if session == "" {
		session = randomID()
		if _, e = db.Exec("INSERT INTO config(key,value) VALUES('session',?)", session); e != nil {
			cleanup()
			return nil, e
		}
	}
	rctx, cancel := context.WithCancel(context.Background())
	r := &Replica{tombstones: map[string]uint64{}, retryAt: map[string]time.Time{}, session: session, quarantine: map[string]Row{}, ignoredRows: map[string]Row{}, retryRows: map[string]Row{}, blockedLocal: map[string]bool{}, client: c, folder: id, key: k, dir: dir, opts: o, root: root, db: db, lock: lock, index: map[string]IndexEntry{}, byID: map[string]string{}, byLocal: map[string]string{}, byFold: map[string]string{}, rejected: map[string]Rejection{}, ctx: rctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	if e = ctx.Err(); e == nil {
		e = r.bindRoot()
	}
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	tombs, e := db.Query("SELECT id,version FROM tombstones")
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	for tombs.Next() {
		var pid string
		var version uint64
		if e = tombs.Scan(&pid, &version); e != nil {
			break
		}
		r.tombstones[pid] = version
	}
	if e == nil {
		e = tombs.Err()
	}
	tombs.Close()
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	rows, e := db.Query("SELECT data FROM entries")
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	for rows.Next() {
		var b []byte
		var v IndexEntry
		if e = rows.Scan(&b); e == nil {
			e = json.Unmarshal(b, &v)
		}
		if e != nil {
			rows.Close()
			cancel()
			cleanup()
			return nil, e
		}
		if _, e = NormalizePath(v.Path); e == nil {
			_, e = NormalizePath(v.Local)
		}
		if e != nil {
			rows.Close()
			cancel()
			cleanup()
			return nil, e
		}
		r.Remember(v)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	_ = db.QueryRow("SELECT value FROM config WHERE key='version'").Scan(&r.version)
	pendingRows, pe := db.Query("SELECT id,data FROM pending")
	if pe != nil {
		cancel()
		cleanup()
		return nil, pe
	}
	for pendingRows.Next() {
		var pid string
		var data []byte
		var saved pendingRow
		if pe = pendingRows.Scan(&pid, &data); pe == nil {
			pe = json.Unmarshal(data, &saved)
		}
		if pe != nil {
			pendingRows.Close()
			cancel()
			cleanup()
			return nil, pe
		}
		switch saved.Kind {
		case "ignored":
			r.ignoredRows[pid] = saved.Row
		case "retry":
			r.retryRows[pid] = saved.Row
			r.retryAt[pid] = saved.RetryAt
		case "quarantine":
			r.quarantine[pid] = saved.Row
		default:
			pe = ErrInvalid
		}
	}
	pe = pendingRows.Err()
	pendingRows.Close()
	if pe != nil {
		cancel()
		cleanup()
		return nil, pe
	}
	for _, item := range mustReadDir(o.StateDir) {
		if strings.HasPrefix(item.Name(), "upload-") && !item.IsDir() {
			_ = os.Remove(filepath.Join(o.StateDir, item.Name()))
		}
	}
	// Staging files never become user content after a crash.
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if e == nil && !d.IsDir() && strings.HasPrefix(d.Name(), ".drivesync-tmp-") {
			rel, _ := filepath.Rel(dir, p)
			_ = root.Remove(rel)
		}
		return nil
	})
	if o.Manual {
		r.status.Watcher = "manual"
		close(r.done)
	} else {
		closeWatch, kind, e := watchDirectory(rctx, dir, r.wake)
		if e != nil {
			r.addError(e)
			kind = "polling"
		}
		r.watcherClose = closeWatch
		r.status.Watcher = kind
		initialVersion := r.version
		go r.run()
		go r.listen(initialVersion)
	}
	return r, nil
}
func safeReplicaName(s string) string {
	var b strings.Builder
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			b.WriteRune(c)
		}
	}
	if b.Len() == 0 {
		return "replica"
	}
	v := b.String()
	if len(v) > 32 {
		v = v[:32]
	}
	return v
}
func (r *Replica) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.status
	s.Conflicts = append([]string(nil), s.Conflicts...)
	s.Skipped = append([]string(nil), s.Skipped...)
	s.Errors = append([]string(nil), s.Errors...)
	s.Quarantined = append([]QuarantinedRow(nil), s.Quarantined...)
	s.Rejected = nil
	for _, v := range r.rejected {
		s.Rejected = append(s.Rejected, v)
	}
	sort.Slice(s.Rejected, func(i, j int) bool { return s.Rejected[i].Path < s.Rejected[j].Path })
	return s
}
func (r *Replica) addError(e error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.Errors = append(r.status.Errors, e.Error())
	if len(r.status.Errors) > 20 {
		r.status.Errors = r.status.Errors[len(r.status.Errors)-20:]
	}
}
func (r *Replica) Close() error {
	var e error
	r.closed.Do(func() {
		r.cancel()
		if r.watcherClose != nil {
			r.watcherClose()
		}
		<-r.done
		r.syncMu.Lock()
		defer r.syncMu.Unlock()
		e = r.db.Close()
		if ce := r.root.Close(); e == nil {
			e = ce
		}
		if ce := r.lock.Close(); e == nil {
			e = ce
		}
	})
	return e
}
func (r *Replica) RetryRejected() {
	r.syncMu.Lock()
	r.allowMassDelete = true
	r.retryAt = map[string]time.Time{}
	for pid, row := range r.quarantine {
		r.retryRows[pid] = row
		delete(r.quarantine, pid)
	}
	r.syncMu.Unlock()
	r.mu.Lock()
	r.rejected = map[string]Rejection{}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *Replica) listen(v uint64) {
	for {
		n, e := r.client.Wait(r.ctx, r.folder, v)
		if e != nil {
			if r.ctx.Err() != nil {
				return
			}
			r.addError(e)
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(func() time.Duration {
				if errors.Is(e, ErrWaitLimit) {
					return 5 * time.Second
				}
				return time.Second
			}()):
			}
			continue
		}
		v = n
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
}
func (r *Replica) run() {
	defer close(r.done)
	ticker := time.NewTicker(r.opts.RescanInterval)
	defer ticker.Stop()
	_ = r.Sync(r.ctx)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			_ = r.Sync(r.ctx)
		case <-r.wake:
			timer := time.NewTimer(r.opts.Debounce)
		debounce:
			for {
				select {
				case <-r.ctx.Done():
					timer.Stop()
					return
				case <-r.wake:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(r.opts.Debounce)
				case <-timer.C:
					break debounce
				}
			}
			_ = r.Sync(r.ctx)
		}
	}
}
func (r *Replica) save(v IndexEntry) error {
	if !v.Deleted {
		if other, ok := r.byLocal[v.Local]; ok && other != v.Path && !r.index[other].Deleted {
			return fmt.Errorf("local path already mapped: %s", v.Local)
		}
	}
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if _, e = r.db.Exec("INSERT INTO entries(path,data) VALUES(?,?) ON CONFLICT(path) DO UPDATE SET data=excluded.data", v.Path, b); e == nil {
		r.Remember(v)
	}
	return e
}
func (r *Replica) forget(p string) error {
	_, e := r.db.Exec("DELETE FROM entries WHERE path=?", p)
	if e == nil {
		r.unremember(p)
	}
	return e
}
func (r *Replica) ignore(p string, dir bool, patterns []string) bool {
	if r.ignoreOne(p, dir, patterns) {
		return true
	}
	for parent := path.Dir(p); parent != "."; parent = path.Dir(parent) {
		if r.ignoreOne(parent, true, patterns) {
			return true
		}
	}
	return false
}
func (r *Replica) ignoreOne(p string, dir bool, patterns []string) bool {
	p = strings.ToLower(foldPath(p))
	base := path.Base(p)
	if base == ".ds_store" || base == ".drivesyncignore" || strings.HasPrefix(base, ".drivesync") || strings.HasPrefix(base, ".~") || strings.HasPrefix(base, "~$") || strings.HasSuffix(base, "~") || strings.HasSuffix(base, ".swp") || strings.HasSuffix(base, ".swo") || strings.HasSuffix(base, ".tmp") {
		return true
	}
	for _, raw := range patterns {
		pattern := strings.ToLower(foldPath(strings.TrimSpace(raw)))
		if pattern == "" || strings.HasPrefix(pattern, "#") {
			continue
		}
		onlyDir := strings.HasSuffix(pattern, "/")
		pattern = strings.TrimSuffix(pattern, "/")
		if onlyDir && !dir {
			continue
		}
		if ok, _ := path.Match(pattern, p); ok {
			return true
		}
		if !strings.Contains(pattern, "/") {
			if ok, _ := path.Match(pattern, base); ok {
				return true
			}
		}
	}
	return false
}
func (r *Replica) remotePath(local string) string {
	if p, ok := r.byLocal[local]; ok {
		return p
	}
	for parent := path.Dir(local); parent != "."; parent = path.Dir(parent) {
		if remote, ok := r.byLocal[parent]; ok {
			v := r.index[remote]
			if v.Directory && !v.Deleted {
				return remote + strings.TrimPrefix(local, parent)
			}
		}
	}
	return local
}

func (r *Replica) ScanLocal() (map[string]LocalFile, error) {
	out := map[string]LocalFile{}
	r.blockedLocal = map[string]bool{}
	patterns := r.patterns()
	skipped := []string{}
	e := filepath.WalkDir(r.dir, func(full string, d fs.DirEntry, e error) error {
		if e != nil {
			skipped = append(skipped, full+": "+e.Error())
			if rel, re := filepath.Rel(r.dir, full); re == nil {
				r.blockLocal(filepath.ToSlash(rel))
			}
			return nil
		}
		if full == r.dir {
			return nil
		}
		rel, e := filepath.Rel(r.dir, full)
		if e != nil {
			return e
		}
		local := filepath.ToSlash(rel)
		if r.ignore(local, d.IsDir(), patterns) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			r.blockLocal(local)
			skipped = append(skipped, local+": symlink")
			return nil
		}
		p := r.remotePath(local)
		if _, e = NormalizePath(p); e != nil {
			r.blockLocal(local)
			skipped = append(skipped, local+": invalid path")
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if prior, ok := r.index[p]; d.IsDir() && ok && prior.Deleted && prior.Directory {
			return nil
		}
		v, e := r.inspect(p, local)
		if e != nil {
			if errors.Is(e, os.ErrNotExist) {
				return nil
			}
			r.blockLocal(local)
			skipped = append(skipped, local+": "+e.Error())
			return nil
		}
		out[p] = v
		return nil
	})
	r.mu.Lock()
	r.status.Skipped = skipped
	r.mu.Unlock()
	return out, e
}
func (r *Replica) safe(p string) error {
	if _, e := NormalizePath(p); e != nil {
		return e
	}
	parts := strings.Split(p, "/")
	for i := range parts {
		s, e := r.root.Lstat(strings.Join(parts[:i+1], "/"))
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if s.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink component %s", errLocalObstacle, p)
		}
		if i < len(parts)-1 && !s.IsDir() {
			return os.ErrNotExist
		}
	}
	return nil
}
func (r *Replica) inspect(p, local string) (LocalFile, error) {
	v := LocalFile{Path: p, Local: local}
	if e := r.safe(local); e != nil {
		return v, e
	}
	before, e := r.root.Lstat(local)
	if e != nil {
		return v, e
	}
	if !before.IsDir() && !before.Mode().IsRegular() {
		return v, fmt.Errorf("%w: unsupported entry %s", errLocalObstacle, local)
	}
	f, e := openLocal(r.root, local)
	if e != nil {
		return v, e
	}
	defer f.Close()
	s, e := f.Stat()
	if e != nil {
		return v, e
	}
	if !os.SameFile(before, s) || (!s.IsDir() && !s.Mode().IsRegular()) {
		return v, fmt.Errorf("%w: unsupported entry %s", errLocalObstacle, local)
	}
	v.Mode = uint32(s.Mode().Perm())
	v.Directory = s.IsDir()
	v.Size = s.Size()
	if v.Directory {
		v.Hash = "directory"
		v.Size = 0
		return v, nil
	}
	if !s.Mode().IsRegular() {
		return v, fmt.Errorf("%w: unsupported entry %s", errLocalObstacle, local)
	}
	h := blake3.New()
	if _, e = io.Copy(h, f); e != nil {
		return v, e
	}
	v.Hash = fmt.Sprintf("%x", h.Sum(nil))
	return v, nil
}
func sameFile(v LocalFile, i IndexEntry) bool {
	return v.Hash == i.Hash && v.Directory == i.Directory && v.Mode == i.Mode
}
func (r *Replica) Sync(ctx context.Context) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	defer func() { r.allowMassDelete = false }()
	if e := r.ctx.Err(); e != nil {
		return e
	}
	e := r.sync(ctx)
	if e != nil {
		r.addError(e)
	}
	if e == nil {
		r.mu.Lock()
		r.status.Errors = nil
		r.mu.Unlock()
	}
	if pe := r.persistStatus(); pe != nil && e == nil {
		e = pe
		r.addError(pe)
	}
	return e
}
func (r *Replica) sync(ctx context.Context) error {
	if e := r.validRoot(); e != nil {
		return e
	}
	folder, e := r.client.GetFolder(ctx, r.folder)
	if e != nil {
		return e
	}
	var firstError error
	record := func(e error) {
		if e != nil {
			r.addError(e)
			if firstError == nil {
				firstError = e
			}
		}
	}
	writable := folder.Role == Owner || folder.Role == Writer
	local, e := r.ScanLocal()
	if e != nil {
		return e
	}
	deleteErr := r.deleteSafety(local)
	record(deleteErr)
	record(r.pull(ctx, local))
	local, e = r.ScanLocal()
	if e != nil {
		return e
	}
	paths := make([]string, 0, len(local))
	var up int64
	for p, v := range local {
		if i, ok := r.index[p]; !ok || !sameFile(v, i) {
			paths = append(paths, p)
			up += SealedSize(v.Size)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		if strings.Count(paths[i], "/") == strings.Count(paths[j], "/") {
			return paths[i] < paths[j]
		}
		return strings.Count(paths[i], "/") < strings.Count(paths[j], "/")
	})
	r.mu.Lock()
	r.status.PendingUpBytes = up
	r.mu.Unlock()
	renames := map[string]string{}
	heldDeletes := map[string]bool{}
	// Build candidate indexes once. Scan contains exact on-disk spelling and
	// already excludes unreadable trees, so no per-candidate directory listing.
	hashes := map[string][]string{}
	folded := map[string][]string{}
	presentLocal := map[string]bool{}
	for _, v := range local {
		presentLocal[v.Local] = true
	}
	for old, i := range r.index {
		if deleteErr != nil || i.Deleted || i.Awaiting || r.localBlocked(old) || presentLocal[i.Local] {
			continue
		}
		hashes[i.Hash] = append(hashes[i.Hash], old)
		folded[foldPath(i.Local)] = append(folded[foldPath(i.Local)], old)
	}
	takeCandidate := func(candidates map[string][]string, key string, directory bool) string {
		for len(candidates[key]) > 0 {
			list := candidates[key]
			old := list[len(list)-1]
			candidates[key] = list[:len(list)-1]
			if !heldDeletes[old] && r.index[old].Directory == directory {
				return old
			}
		}
		return ""
	}
	for _, p := range paths {
		v := local[p]
		if i, known := r.index[p]; known && !i.Deleted {
			continue
		}
		old := takeCandidate(hashes, v.Hash, v.Directory)
		if old == "" {
			old = takeCandidate(folded, foldPath(v.Local), v.Directory)
		}
		if old != "" {
			renames[p] = old
			heldDeletes[old] = true
		}
	}
	// Independent uploads get their headroom before pending atomic renames.
	sort.SliceStable(paths, func(i, j int) bool { _, a := renames[paths[i]]; _, b := renames[paths[j]]; return !a && b })
	pairs := map[string]string{}
	for next, old := range renames {
		a, _ := PathID(r.key, r.folder, next)
		b, _ := PathID(r.key, r.folder, old)
		pairs[a] = b
		pairs[b] = a
	}
	batch := []Mutation{}
	snapshots := map[string]LocalFile{}
	tickets := []Ticket{}
	var flush func() error
	flush = func() error {
		if len(batch) == 0 {
			return nil
		}
		for _, m := range batch {
			if m.Deleted {
				if e := r.validRoot(); e != nil {
					return e
				}
				break
			}
		}
		d, e := r.client.Commit(ctx, r.folder, batch)
		if errors.Is(e, ErrConflict) {
			// Atomic rollback leaves non-conflicting tickets usable. Archive only stale
			// paths and retry the rest, retaining any rename pair as one transaction.
			var ce *ConflictError
			bad := map[string]bool{}
			if errors.As(e, &ce) {
				for _, pid := range ce.Paths {
					bad[pid] = true
				}
			}
			for _, m := range batch {
				if bad[m.PathID] {
					if other := pairs[m.PathID]; other != "" {
						bad[other] = true
					}
				}
			}
			remaining := []Mutation{}
			for _, m := range batch {
				if bad[m.PathID] {
					if !m.Deleted {
						for p, v := range snapshots {
							pid, _ := PathID(r.key, r.folder, p)
							if pid == m.PathID {
								if ae := r.awaitWinner(p); ae != nil {
									return ae
								}
								if !v.Directory {
									if _, pe := r.preserve(v.Local); pe != nil {
										return pe
									}
								}
							}
						}
						_ = r.client.CancelUpload(ctx, r.folder, m.TicketID)
					}
					continue
				}
				remaining = append(remaining, m)
			}
			if len(bad) == 0 {
				record(e)
				batch = nil
				snapshots = map[string]LocalFile{}
				tickets = nil
				return nil
			}
			if len(remaining) > 0 {
				d, e = r.client.Commit(ctx, r.folder, remaining)
			} else {
				e = nil
				d = Delta{}
			}
		}
		if isLimit(e) && len(batch) > 1 {
			units := [][]Mutation{}
			used := map[string]bool{}
			byPath := map[string]Mutation{}
			for _, m := range batch {
				byPath[m.PathID] = m
			}
			for _, m := range batch {
				if used[m.PathID] {
					continue
				}
				unit := []Mutation{m}
				used[m.PathID] = true
				if peer, ok := byPath[pairs[m.PathID]]; ok {
					unit = append(unit, peer)
					used[peer.PathID] = true
				}
				units = append(units, unit)
			}
			if len(units) > 1 {
				saved, uploaded := snapshots, tickets
				for _, unit := range units {
					batch = unit
					snapshots = map[string]LocalFile{}
					tickets = nil
					ids := map[string]bool{}
					for _, m := range unit {
						ids[m.PathID] = true
					}
					for p, v := range saved {
						pid, _ := PathID(r.key, r.folder, p)
						if ids[pid] {
							snapshots[p] = v
						}
					}
					for _, t := range uploaded {
						if ids[t.PathID] {
							tickets = append(tickets, t)
						}
					}
					if fe := flush(); fe != nil {
						return fe
					}
				}
				return nil
			}
		}
		if e != nil {
			for _, t := range tickets {
				_ = r.client.CancelUpload(ctx, r.folder, t.ID)
			}
			if isLimit(e) {
				for p, v := range snapshots {
					r.reject(p, v.Hash, e)
				}
			} else {
				record(e)
			}
			batch = nil
			snapshots = map[string]LocalFile{}
			tickets = nil
			return nil
		}
		for _, row := range d.Rows {
			if row.Deleted {
				if p, ok := r.byID[row.PathID]; ok {
					i := r.index[p]
					i.Deleted = true
					i.Hash = ""
					i.Version = row.Version
					if se := r.save(i); se != nil {
						return se
					}
				}
				continue
			}
			m, me := OpenMetadata(r.key, r.folder, row)
			if me != nil {
				return me
			}
			v := snapshots[m.Path]
			if se := r.save(IndexEntry{Path: m.Path, Local: v.Local, Hash: v.Hash, Version: row.Version, Directory: v.Directory, Mode: v.Mode}); se != nil {
				return se
			}
		}
		batch = nil
		snapshots = map[string]LocalFile{}
		tickets = nil
		return nil
	}
	for _, p := range paths {
		v := local[p]
		// Do not let a slow next transfer expire already-uploaded batch members.
		// Renewal protects the stream currently flowing, not idle sibling tickets.
		if len(batch) > 0 && (v.Size > ChunkSize || time.Now().Add(time.Second).After(tickets[0].Expires)) {
			if e = flush(); e != nil {
				return e
			}
		}
		r.mu.Lock()
		rej, blocked := r.rejected[p]
		r.mu.Unlock()
		if blocked && rej.Hash == v.Hash && time.Now().Before(rej.RetryAt) {
			continue
		}
		if !writable {
			r.reject(p, v.Hash, ErrDenied)
			continue
		}
		if v.Directory {
			if e = durableMkdirAll(r.root, v.Local); e != nil {
				record(e)
				continue
			}
		}
		base := r.index[p].Version
		pid, _ := PathID(r.key, r.folder, p)
		base = max(base, r.tombstones[pid])
		if pending, ok := r.quarantine[pid]; ok {
			base = max(base, pending.Version)
		}
		renameDelete := []Mutation{}
		if old, ok := renames[p]; ok {
			i := r.index[old]
			oldpid, _ := PathID(r.key, r.folder, old)
			renameDelete = append(renameDelete, Mutation{PathID: oldpid, BaseVersion: i.Version, Deleted: true})
		}
		preview, _ := SealMetadata(r.key, r.folder, pid, FileMetadata{Path: p, BlobID: strings.Repeat("0", 32), Size: v.Size, Mode: v.Mode, Directory: v.Directory, Hash: v.Hash})
		request := UploadRequest{SessionID: r.session, PathID: pid, BaseVersion: base, SealedSize: SealedSize(v.Size), MetadataBytes: int64(len(preview))}
		t, e := r.client.Reserve(ctx, r.folder, request)

		if e != nil {
			var l *LimitError
			if errors.As(e, &l) || errors.Is(e, ErrQuota) {
				r.reject(p, v.Hash, e)
				continue
			}
			if errors.Is(e, ErrBusy) {
				continue
			}
			if errors.Is(e, ErrConflict) {
				if ae := r.awaitWinner(p); ae != nil {
					return ae
				}
				if !v.Directory {
					if _, e = r.preserve(v.Local); e != nil {
						return e
					}
				}
				continue
			}
			record(e)
			continue
		}
		// Stream a verified handle; commit only when its streamed hash still matches the scan.
		var src io.ReadCloser = io.NopCloser(strings.NewReader(""))
		if !v.Directory {
			if e = r.safe(v.Local); e == nil {
				var before os.FileInfo
				before, e = r.root.Lstat(v.Local)
				if e == nil && !before.Mode().IsRegular() {
					e = fmt.Errorf("unsupported local entry: %s", v.Local)
				}
				if e == nil {
					var file *os.File
					file, e = openLocal(r.root, v.Local)
					if e == nil {
						var opened os.FileInfo
						opened, e = file.Stat()
						if e == nil && (!os.SameFile(before, opened) || opened.Size() != v.Size) {
							e = ErrBusy
						}
						if e != nil {
							file.Close()
						} else {
							src = file
						}
					}
				}
			}
		}
		if e != nil {
			src.Close()
			_ = r.client.CancelUpload(ctx, r.folder, t.ID)
			r.reject(p, v.Hash, e)
			record(e)
			continue
		}
		h := blake3.New()
		pr, pw := io.Pipe()
		sealedDone := make(chan error, 1)
		go func() {
			e := SealContent(pw, io.TeeReader(&contextReader{ctx, src}, h), r.key, r.folder, t.BlobID, pid)
			_ = pw.CloseWithError(e)
			sealedDone <- e
		}()
		e = r.client.Upload(ctx, r.folder, t, pr)
		_ = pr.CloseWithError(e)
		se := <-sealedDone
		src.Close()
		if e == nil {
			e = se
		}
		if e == nil && !v.Directory && fmt.Sprintf("%x", h.Sum(nil)) != v.Hash {
			e = ErrBusy
		}
		if e != nil {
			_ = r.client.CancelUpload(ctx, r.folder, t.ID)
			var pe *os.PathError
			if isTransferError(e) || errors.As(e, &pe) {
				r.reject(p, v.Hash, e)
			}
			record(e)
			continue
		}
		m := FileMetadata{Path: p, BlobID: t.BlobID, Size: v.Size, Mode: v.Mode, Directory: v.Directory, Hash: v.Hash}
		sealed, e := SealMetadata(r.key, r.folder, pid, m)
		if e != nil {
			return e
		}
		batch = append(batch, Mutation{PathID: pid, BaseVersion: base, TicketID: t.ID, Metadata: sealed})
		batch = append(batch, renameDelete...)
		snapshots[p] = v
		tickets = append(tickets, t)
		r.mu.Lock()
		delete(r.rejected, p)
		r.mu.Unlock()
		if len(batch) >= 16 || v.Size > ChunkSize {
			if e = flush(); e != nil {
				return e
			}
		}
	}
	if e = flush(); e != nil {
		return e
	}
	// Missing tracked files become CAS tombstones. A failed delete restores the winner on pull.
	deleted := []string{}
	for p, v := range r.index {
		if _, ok := local[p]; !ok && !v.Deleted && !v.Awaiting && !heldDeletes[p] && !r.localBlocked(p) && deleteErr == nil {
			if !writable || r.ignore(v.Local, v.Directory, r.patterns()) {
				continue
			}
			deleted = append(deleted, p)
		}
	}
	sort.Slice(deleted, func(i, j int) bool { return len(deleted[i]) > len(deleted[j]) })
	for _, p := range deleted {
		i := r.index[p]
		if r.exactExists(i.Local) {
			continue
		}
		pid, _ := PathID(r.key, r.folder, p)
		var tombstone Delta
		if e = r.validRoot(); e != nil {
			return e
		}
		tombstone, e = r.client.Commit(ctx, r.folder, []Mutation{{PathID: pid, BaseVersion: i.Version, Deleted: true}})
		if e != nil && !errors.Is(e, ErrConflict) {
			record(e)
			continue
		}
		if e == nil {
			i.Deleted = true
			i.Hash = ""
			i.Version = tombstone.Rows[0].Version
			if e = r.save(i); e != nil {
				return e
			}
		}
	}
	record(r.pull(ctx, local))
	return firstError
}

// pull reconciles exactly the delta supplied by the authority, including Full
// deltas that arrive after the preliminary GetFolder. No stability retry loop.
func (r *Replica) pull(ctx context.Context, local map[string]LocalFile) error {
	var firstError error
	delta, e := r.client.Changes(ctx, r.folder, r.version)
	if e != nil {
		return e
	}
	if delta.Full {
		r.reconcileFull(&delta)
	}
	for _, row := range delta.Rows {
		if old, ok := r.quarantine[row.PathID]; ok && old.Version != row.Version {
			delete(r.quarantine, row.PathID)
		}
	}
	for pid, row := range r.retryRows {
		if time.Now().Before(r.retryAt[pid]) {
			continue
		}
		delta.Rows = append(delta.Rows, row)
	}
	for pid, row := range r.ignoredRows {
		delta.Rows = append(delta.Rows, row)
		delete(r.ignoredRows, pid)
	}
	latest := map[string]Row{}
	for _, row := range delta.Rows {
		old, ok := latest[row.PathID]
		if !ok || row.Version > old.Version {
			latest[row.PathID] = row
		}
	}
	delta.Rows = nil
	for _, row := range latest {
		delta.Rows = append(delta.Rows, row)
	}
	var down int64
	for _, row := range delta.Rows {
		down += row.SealedSize
	}
	r.mu.Lock()
	r.status.PendingDownBytes = down
	r.mu.Unlock()
	// Materialize directory markers before children, including rows in the same batch.
	metadata := map[string]FileMetadata{}
	for _, row := range delta.Rows {
		if !row.Deleted {
			m, me := OpenMetadata(r.key, r.folder, row)
			if me != nil {
				r.quarantine[row.PathID] = row
				r.addError(fmt.Errorf("remote %s: %w", row.PathID, me))
				if firstError == nil {
					firstError = me
				}
				continue
			}
			metadata[row.PathID] = m
		}
	}
	sort.SliceStable(delta.Rows, func(i, j int) bool {
		a, b := delta.Rows[i], delta.Rows[j]
		if a.Deleted != b.Deleted {
			return a.Deleted
		}
		if a.Deleted {
			return len(r.byID[a.PathID]) > len(r.byID[b.PathID])
		}
		ma, mb := metadata[a.PathID], metadata[b.PathID]
		if ma.Directory != mb.Directory {
			return ma.Directory
		}
		depthA, depthB := strings.Count(ma.Path, "/"), strings.Count(mb.Path, "/")
		if depthA != depthB {
			return depthA < depthB
		}
		return ma.Path < mb.Path
	})
	pending := append([]Row(nil), delta.Rows...)
	for len(pending) > 0 {
		row := pending[0]
		pending = pending[1:]
		if prior, ok := r.retryRows[row.PathID]; ok && prior.Version == row.Version && time.Now().Before(r.retryAt[row.PathID]) {
			continue
		}
		delete(r.retryRows, row.PathID)
		delete(r.retryAt, row.PathID)
		if old, ok := r.quarantine[row.PathID]; ok && old.Version == row.Version {
			continue
		}
		if e = r.apply(ctx, row); e != nil {
			if errors.Is(e, ErrIntegrity) {
				r.quarantine[row.PathID] = row
			} else {
				r.retryRows[row.PathID] = row
				if isTransferError(e) {
					r.retryAt[row.PathID] = time.Now().Add(r.opts.RetryInterval)
				}
			}
			r.addError(fmt.Errorf("remote %s: %w", row.PathID, e))
			if firstError == nil {
				firstError = e
			}
			continue
		}
	}
	// Persist retry rows before advancing the cursor, including locally ignored rows.
	stateTx, se := r.db.Begin()
	if se != nil {
		return se
	}
	defer stateTx.Rollback()
	if _, e = stateTx.Exec("DELETE FROM pending"); e != nil {
		return e
	}
	for kind, set := range map[string]map[string]Row{"quarantine": r.quarantine, "ignored": r.ignoredRows, "retry": r.retryRows} {
		for pid, row := range set {
			b, me := json.Marshal(pendingRow{Row: row, Kind: kind, RetryAt: r.retryAt[pid]})
			if me != nil {
				return me
			}
			if _, me = stateTx.Exec("INSERT OR REPLACE INTO pending(id,data) VALUES(?,?)", pid, b); me != nil {
				return me
			}
		}
	}
	// Persist only this pull's tombstones, in bounded SQL batches. Rewriting the
	// complete cache on idle pulls makes a compactable large folder quadratic.
	tombs := []any{}
	flushTombs := func() error {
		if len(tombs) == 0 {
			return nil
		}
		q := "INSERT INTO tombstones(id,version) VALUES " + strings.TrimSuffix(strings.Repeat("(?,?),", len(tombs)/2), ",") + " ON CONFLICT(id) DO UPDATE SET version=excluded.version WHERE version!=excluded.version"
		_, e := stateTx.Exec(q, tombs...)
		tombs = tombs[:0]
		return e
	}
	for _, row := range delta.Rows {
		if row.Deleted {
			tombs = append(tombs, row.PathID, r.tombstones[row.PathID])
			if len(tombs) == 256 {
				if e = flushTombs(); e != nil {
					return e
				}
			}
		}
	}
	if e = flushTombs(); e != nil {
		return e
	}
	if _, e = stateTx.Exec("INSERT INTO config(key,value) VALUES('version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", delta.Version); e != nil {
		return e
	}
	if e = stateTx.Commit(); e != nil {
		return e
	}
	r.version = delta.Version
	r.mu.Lock()
	r.status.Version = r.version
	r.status.Quarantined = nil
	for _, row := range r.quarantine {
		r.status.Quarantined = append(r.status.Quarantined, QuarantinedRow{PathID: row.PathID, Version: row.Version, Path: func() string {
			m, e := OpenMetadata(r.key, r.folder, row)
			if e == nil {
				return m.Path
			}
			return ""
		}()})
	}
	sort.Slice(r.status.Quarantined, func(i, j int) bool { return r.status.Quarantined[i].PathID < r.status.Quarantined[j].PathID })
	r.status.LastSync = time.Now()
	remaining := int64(0)
	for p, v := range local {
		if i, ok := r.index[p]; !ok || !sameFile(v, i) {
			if _, e := r.root.Lstat(v.Local); e == nil {
				remaining += SealedSize(v.Size)
			}
		}
	}
	r.status.PendingUpBytes = remaining
	r.status.PendingDownBytes = 0
	r.mu.Unlock()
	return firstError
}
func (r *Replica) reject(p, h string, e error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejected[p] = Rejection{p, e.Error(), h, time.Now().Add(r.opts.RetryInterval)}
}
func (r *Replica) preserve(local string) (string, error) {
	if e := r.safe(local); e != nil {
		return "", e
	}
	if _, e := r.root.Lstat(local); errors.Is(e, os.ErrNotExist) {
		return "", nil
	} else if e != nil {
		return "", e
	}
	if f, e := openLocal(r.root, local); e == nil {
		info, se := f.Stat()
		if se == nil && info.Mode().IsRegular() {
			se = f.Sync()
		}
		f.Close()
		if se != nil {
			return "", se
		}
	} else {
		return "", e
	}
	name := variantPath(local, fmt.Sprintf(" (conflict from %s %s-%s)", r.opts.Name, time.Now().UTC().Format("20060102T150405.000000000Z"), randomID()))
	if _, e := NormalizePath(name); e != nil {
		return "", e
	}
	if e := r.root.Rename(local, name); e != nil {
		return "", e
	}
	if e := r.syncParent(name); e != nil {
		return "", e
	}
	r.mu.Lock()
	r.status.Conflicts = append(r.status.Conflicts, name)
	r.mu.Unlock()
	return name, nil
}
func (r *Replica) syncParent(p string) error {
	dir := path.Dir(p)
	f, e := r.root.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return syncDirectoryFile(f)
}
func (r *Replica) localPath(p, pid string) string {
	if v, ok := r.index[p]; ok && !v.Deleted {
		return v.Local
	}
	parts := strings.Split(p, "/")
	candidate := ""
	for n, part := range parts {
		remote := strings.Join(parts[:n+1], "/")
		if v, ok := r.index[remote]; ok && !v.Deleted && v.Directory {
			candidate = v.Local
			continue
		}
		desired := path.Join(candidate, part)
		isParent := n < len(parts)-1
		collision := func(c string) bool {
			if other, ok := r.byLocal[c]; ok && other != remote && !r.index[other].Deleted {
				return true
			}
			if other, ok := r.byFold[foldPath(c)]; ok && other != remote && !r.index[other].Deleted {
				return true
			}
			info, e := r.root.Lstat(c)
			if e == nil {
				if c != desired {
					return true
				}
				if info.Mode()&os.ModeSymlink != 0 {
					return false
				}
				if !r.exactExists(c) || (isParent && !info.IsDir()) {
					return true
				}
			}
			return false
		}
		candidate = desired
		for serial := 0; collision(candidate); serial++ {
			suffix := " (case conflict " + pid[:32] + ")"
			if serial > 0 {
				suffix = fmt.Sprintf(" (case conflict %s-%d)", pid[:32], serial)
			}
			candidate = variantPath(desired, suffix)
			// Never reuse an untracked existing alias. It may be another user's file.
			if _, e := r.root.Lstat(candidate); e == nil {
				continue
			}
		}
	}
	return candidate
}

func (r *Replica) apply(ctx context.Context, row Row) error {
	p, known := r.byID[row.PathID]
	i := r.index[p]
	if row.Deleted {
		r.tombstones[row.PathID] = max(r.tombstones[row.PathID], row.Version)
		if !known {
			return nil
		}
		if !r.exactExists(i.Local) {
			i.Deleted = true
			i.Hash = ""
			i.Version = row.Version
			return r.save(i)
		}
		if e := r.safe(i.Local); e != nil {
			if errors.Is(e, os.ErrNotExist) {
				i.Deleted = true
				i.Hash = ""
				i.Version = row.Version
				return r.save(i)
			}
			return e
		}
		v, e := r.inspect(p, i.Local)
		if e == nil && !sameFile(v, i) && !v.Directory {
			if _, e = r.preserve(i.Local); e != nil {
				return e
			}
		} else if e == nil {
			if e = r.root.Remove(i.Local); e != nil {
				if !v.Directory {
					return e
				}
				if e = r.removeIgnored(i.Local, r.patterns()); e != nil {
					return e
				}
				if e = r.root.Remove(i.Local); e != nil {
					for child, entry := range r.index {
						if child != p && strings.HasPrefix(entry.Local, i.Local+"/") && !entry.Deleted {
							return e
						}
					}
					// User-ignored contents survive. The directory becomes untracked.
					i.Deleted = true
					i.Hash = ""
					i.Version = row.Version
					return r.save(i)
				}
				if e = r.syncParent(i.Local); e != nil {
					return e
				}
			} else {
				if e = r.syncParent(i.Local); e != nil {
					return e
				}
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		i.Deleted = true
		i.Hash = ""
		i.Version = row.Version
		return r.save(i)
	}
	m, e := OpenMetadata(r.key, r.folder, row)
	if e != nil {
		return fmt.Errorf("%w: %v", ErrIntegrity, e)
	}
	p = m.Path
	for parent := p; parent != "."; parent = path.Dir(parent) {
		if old, ok := r.byFold[foldPath(parent)]; ok && old != parent {
			pid, _ := PathID(r.key, r.folder, old)
			if pending, ok := r.retryRows[pid]; ok && pending.Deleted {
				return ErrBusy
			}
		}
	}
	local := r.localPath(p, row.PathID)
	if _, e = NormalizePath(local); e != nil {
		return e
	}
	if e = r.safe(local); e != nil {
		return e
	}
	i, known = r.index[p]
	if known && i.Version >= row.Version && !i.Awaiting {
		return nil
	}
	// Ignore a remote path locally but keep its row unacknowledged in the index. It remains on the server.
	patterns := r.patterns()
	if r.ignore(m.Path, m.Directory, patterns) || r.ignore(local, m.Directory, patterns) {
		r.ignoredRows[row.PathID] = row
		return nil
	}
	if !m.Directory {
		if v, ve := r.inspect(p, local); ve == nil && !v.Directory && v.Hash == m.Hash && v.Size == m.Size {
			if e = durableMkdirAll(r.root, path.Dir(local)); e != nil {
				return e
			}
			f, oe := openLocal(r.root, local)
			if oe != nil {
				return oe
			}
			e = f.Chmod(os.FileMode(m.Mode | 0600))
			if e == nil {
				e = f.Sync()
			}
			f.Close()
			if e != nil {
				return e
			}
			if e = r.syncParent(local); e != nil {
				return e
			}
			return r.save(IndexEntry{Path: p, Local: local, Hash: m.Hash, Version: row.Version, Mode: m.Mode | 0600})
		}
	}
	if m.Directory {
		if m.Size != 0 || m.Hash != "directory" {
			return ErrIntegrity
		}
		stream, de := r.client.Download(ctx, r.folder, row.BlobID)
		if de != nil {
			return de
		}
		counter := &countWriter{Writer: io.Discard}
		received := &countReader{Reader: stream}
		de = OpenContent(counter, received, r.key, r.folder, row.BlobID, row.PathID)
		stream.Close()
		if de != nil {
			return storedContentError(de, received.n, row.SealedSize)
		}
		if counter.n != 0 {
			return ErrIntegrity
		}
		if v, ve := r.inspect(p, local); ve == nil && !v.Directory {
			if _, e = r.preserve(local); e != nil {
				return e
			}
		}
		if e = durableMkdirAll(r.root, local); e != nil {
			return e
		}
		if e = r.root.Chmod(local, os.FileMode(m.Mode|0700)); e != nil {
			return e
		}
		opened, oe := r.root.Open(local)
		if oe != nil {
			return oe
		}
		e = opened.Sync()
		opened.Close()
		if e != nil {
			return e
		}
		return r.save(IndexEntry{Path: p, Local: local, Hash: "directory", Version: row.Version, Directory: true, Mode: m.Mode | 0700})
	}
	if e = durableMkdirAll(r.root, path.Dir(local)); e != nil {
		return e
	}
	if e = r.safe(local); e != nil {
		return e
	}
	temp := path.Join(path.Dir(local), ".drivesync-tmp-"+randomID())
	f, e := r.root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer r.root.Remove(temp)
	stream, e := r.client.Download(ctx, r.folder, row.BlobID)
	if e != nil {
		f.Close()
		return e
	}
	h := blake3.New()
	counter := &countWriter{Writer: io.MultiWriter(f, h)}
	received := &countReader{Reader: stream}
	e = OpenContent(counter, received, r.key, r.folder, row.BlobID, row.PathID)
	stream.Close()
	e = storedContentError(e, received.n, row.SealedSize)
	if e == nil && (counter.n != m.Size || fmt.Sprintf("%x", h.Sum(nil)) != m.Hash) {
		e = ErrIntegrity
	}
	if e == nil {
		e = f.Chmod(os.FileMode(m.Mode | 0600))
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	// Inspect immediately before publishing: edits during download are preserved too.
	v, ve := r.inspect(p, local)
	if ve == nil {
		dirty := (!known || !sameFile(v, i)) && (v.Directory || v.Hash != m.Hash || v.Size != m.Size)
		if dirty || v.Directory {
			if _, e = r.preserve(local); e != nil {
				return e
			}
		}
	} else if !errors.Is(ve, os.ErrNotExist) {
		return ve
	}
	if e = r.safe(local); e != nil {
		return e
	}
	if e = r.root.Rename(temp, local); e != nil {
		return e
	}
	if e = r.syncParent(local); e != nil {
		return e
	}
	return r.save(IndexEntry{Path: p, Local: local, Hash: m.Hash, Version: row.Version, Mode: m.Mode | 0600})
}

type countWriter struct {
	io.Writer
	n int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, e := w.Writer.Write(p)
	w.n += int64(n)
	return n, e
}

func mustReadDir(p string) []os.DirEntry { entries, _ := os.ReadDir(p); return entries }
func (r *Replica) patterns() []string {
	patterns := append([]string(nil), r.opts.Ignore...)
	if info, e := r.root.Lstat(".drivesyncignore"); e != nil || info.Mode()&os.ModeSymlink != 0 {
		return patterns
	}
	if f, e := openLocal(r.root, ".drivesyncignore"); e == nil {
		b, _ := io.ReadAll(io.LimitReader(f, 64*1024))
		f.Close()
		patterns = append(patterns, strings.Split(string(b), "\n")...)
	}
	return patterns
}

func variantPath(p, suffix string) string {
	name := path.Base(p)
	ext := path.Ext(name)
	if len(ext) > 32 {
		ext = ""
	}
	stem := strings.TrimSuffix(name, ext)
	maximum := 255 - len(suffix) - len(ext)
	for len(stem) > maximum {
		_, size := utf8.DecodeLastRuneInString(stem)
		stem = stem[:len(stem)-size]
	}
	variant := path.Join(path.Dir(p), stem+suffix+ext)
	if len(variant) > 4096 {
		return stem + suffix + ext
	}
	return variant
}
func (r *Replica) persistStatus() error {
	b, e := json.MarshalIndent(r.Status(), "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(r.opts.StateDir, "status-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), filepath.Join(r.opts.StateDir, "status.json"))
}

func (r *Replica) unremember(p string) {
	if old, ok := r.index[p]; ok {
		if r.byLocal[old.Local] == p {
			delete(r.byLocal, old.Local)
		}
		folded := foldPath(old.Local)
		if r.byFold[folded] == p {
			delete(r.byFold, folded)
		}
		pid, _ := PathID(r.key, r.folder, p)
		delete(r.byID, pid)
		delete(r.index, p)
	}
}
func (r *Replica) Remember(v IndexEntry) {
	r.unremember(v.Path)
	r.index[v.Path] = v
	pid, _ := PathID(r.key, r.folder, v.Path)
	r.byID[pid] = v.Path
	if !v.Deleted {
		r.byLocal[v.Local] = v.Path
		r.byFold[foldPath(v.Local)] = v.Path
	}
}

func isLimit(e error) bool { var l *LimitError; return errors.As(e, &l) || errors.Is(e, ErrQuota) }
func (r *Replica) awaitWinner(p string) error {
	if i, ok := r.index[p]; ok {
		i.Awaiting = true
		return r.save(i)
	}
	return nil
}
func (r *Replica) exactExists(p string) bool {
	parts := strings.Split(p, "/")
	for i, part := range parts {
		dir := "."
		if i > 0 {
			dir = strings.Join(parts[:i], "/")
		}
		f, e := r.root.Open(dir)
		if e != nil {
			return false
		}
		entries, e := f.ReadDir(-1)
		f.Close()
		if e != nil {
			return false
		}
		found := false
		for _, v := range entries {
			if v.Name() == part {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (r *Replica) localBlocked(p string) bool {
	for {
		if r.blockedLocal[p] {
			return true
		}
		p = path.Dir(p)
		if p == "." {
			return false
		}
	}
}

func (r *Replica) blockLocal(local string) {
	r.blockedLocal[r.remotePath(local)] = true
	if old, ok := r.byFold[foldPath(local)]; ok {
		r.blockedLocal[old] = true
	}
	for parent := path.Dir(local); parent != "."; parent = path.Dir(parent) {
		if old, ok := r.byFold[foldPath(parent)]; ok {
			r.blockedLocal[old+strings.TrimPrefix(local, parent)] = true
			break
		}
	}
}
