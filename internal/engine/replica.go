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
	"strconv"
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

	client            Client
	folder            string
	session           string
	key               FolderKey
	dir               string
	opts              Options
	root              *os.Root
	db                *sql.DB
	lock              *os.File
	rootLock          *os.File
	generation        uint64
	missingDeferred   map[string]uint64
	ignoreFingerprint string
	ignoredChanged    bool
	ruleSnap          []string
	baselineFiles     int64
	baselineBytes     int64
	adoption          bool
	index             map[string]IndexEntry
	byID              map[string]string
	byLocal           map[string]string
	byFold            map[string]string
	version           uint64
	syncMu            sync.Mutex
	mu                sync.Mutex
	status            Status
	quarantine        map[string]Row
	ignoredRows       map[string]Row
	retryRows         map[string]Row
	blockedLocal      map[string]bool
	scanBounds        *dirBounds
	rejected          map[string]Rejection
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	wake              chan struct{}
	watcherClose      func()
	closed            sync.Once
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
	release, e := attachmentAdmission(dir)
	if e != nil {
		return nil, e
	}
	defer release()
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
	tightenState(o.StateDir)
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
	r := &Replica{missingDeferred: map[string]uint64{}, tombstones: map[string]uint64{}, retryAt: map[string]time.Time{}, session: session, quarantine: map[string]Row{}, ignoredRows: map[string]Row{}, retryRows: map[string]Row{}, blockedLocal: map[string]bool{}, client: c, folder: id, key: k, dir: dir, opts: o, root: root, db: db, lock: lock, index: map[string]IndexEntry{}, byID: map[string]string{}, byLocal: map[string]string{}, byFold: map[string]string{}, rejected: map[string]Rejection{}, ctx: rctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	if _, me := os.Lstat(filepath.Join(dir, rootMarker)); errors.Is(me, os.ErrNotExist) {
		b, _ := json.Marshal(rootIdentity{Folder: id, Token: randomID()})
		e = writeMarker(filepath.Join(dir, rootMarker), b)
	}
	if e == nil {
		e = ctx.Err()
	}
	if e == nil {
		e = r.bindRoot()
	}
	if e == nil {
		r.rootLock, e = lockState(filepath.Join(dir, rootMarker))
	}
	if e != nil {
		cancel()
		cleanup()
		return nil, e
	}
	cleanupBase := cleanup
	cleanup = func() { r.rootLock.Close(); cleanupBase() }
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
	_ = db.QueryRow("SELECT value FROM config WHERE key='ignore'").Scan(&r.ignoreFingerprint)
	var adoption, baseFiles, baseBytes string
	_ = db.QueryRow("SELECT value FROM config WHERE key='adoption'").Scan(&adoption)
	_ = db.QueryRow("SELECT value FROM config WHERE key='baseline-files'").Scan(&baseFiles)
	_ = db.QueryRow("SELECT value FROM config WHERE key='baseline-bytes'").Scan(&baseBytes)
	r.adoption = adoption == "1"
	r.baselineFiles, _ = strconv.ParseInt(baseFiles, 10, 64)
	r.baselineBytes, _ = strconv.ParseInt(baseBytes, 10, 64)
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
		if r.rootLock != nil {
			r.rootLock.Close()
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
	if r.validRoot() != nil {
		if e := r.rebindRoot(); e != nil {
			r.addError(e)
		}
	}
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
func pathIgnored(p string, dir bool, patterns []string) bool {
	return (&Replica{}).ignore(p, dir, patterns)
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
	// Repositories move through git. A drive copy of .git breaks concurrent commits.
	if strings.Contains("/"+p+"/", "/.git/") {
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
		anchored := strings.HasPrefix(pattern, "/")
		pattern = strings.TrimPrefix(pattern, "/")
		if globPath(strings.Split(pattern, "/"), strings.Split(p, "/")) {
			return true
		}
		if !anchored && !strings.Contains(pattern, "/") {
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
	rootInfo, e := r.root.Stat(".")
	if e != nil {
		return nil, e
	}
	// One root device for this walk. Marker and mount checks run once per
	// directory; files reuse that result instead of stating the root again.
	r.scanBounds = &dirBounds{dev: device(rootInfo), seen: map[string]error{}}
	defer func() { r.scanBounds = nil }()
	patterns := r.patterns()
	skipped := []string{}
	for _, rule := range patterns {
		raw := strings.TrimSpace(rule)
		_, err := path.Match(strings.ReplaceAll(raw, "**", "*"), "check")
		if strings.HasPrefix(raw, "!") || strings.Contains(raw, "\\") || err != nil {
			skipped = append(skipped, "unsupported ignore rule: "+raw)
		}
	}
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
			r.reject(local, r.hashUnportable(local), fmt.Errorf("name is not portable; remove reserved characters/names or shorten components (255 bytes): %w", err))
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if prior, ok := r.index[p]; d.IsDir() && ok && prior.Deleted && prior.Directory {
			return nil
		}
		v, err := r.inspect(p, local)
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
			r.reject(local, v.Hash, fmt.Errorf("ambiguous remote path %s", p))
			r.reject(prior.Local, prior.Hash, fmt.Errorf("ambiguous remote path %s", p))
			return nil
		}
		if !r.localBlocked(p) {
			out[p] = v
		}
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
		if s.IsDir() {
			if e := r.directoryBoundary(strings.Join(parts[:i+1], "/"), s); e != nil {
				return fmt.Errorf("%w: %v", errLocalObstacle, e)
			}
		}
		if i < len(parts)-1 && !s.IsDir() {
			return errLocalObstacle
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
	if _, e = io.CopyN(h, f, v.Size); e != nil {
		return v, e
	}
	v.Hash = fmt.Sprintf("%x", h.Sum(nil))
	return v, nil
}
func sameFile(v LocalFile, i IndexEntry) bool {
	return !i.Deleted && v.Hash == i.Hash && v.Directory == i.Directory && v.Mode&0100 == i.Mode&0100
}
func (r *Replica) Sync(ctx context.Context) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	r.generation++
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
		r.status.LastSync = time.Now()
		r.mu.Unlock()
	}
	if pe := r.persistStatus(); pe != nil && e == nil {
		e = pe
		r.addError(pe)
	}
	tightenState(r.opts.StateDir)
	return e
}
func (r *Replica) sync(ctx context.Context) error {
	if e := r.validRoot(); e != nil {
		if r.reopenSameRoot() != nil {
			return e
		}
		if e = r.validRoot(); e != nil {
			return e
		}
	}
	rules, re := r.readRules()
	if re != nil {
		return re
	}
	if rules == nil {
		rules = []string{}
	}
	r.ruleSnap = rules
	defer func() { r.ruleSnap = nil }()
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
	record(r.pull(ctx, nil))
	local, e := r.ScanLocal()
	if e != nil {
		return e
	}
	deleteErr := r.deleteSafety(local)
	record(deleteErr)

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
		oldPID, _ := PathID(r.key, r.folder, old)
		_, remotePending := r.retryRows[oldPID]
		if deleteErr != nil || i.Deleted || i.Awaiting || remotePending || r.localBlocked(old) || presentLocal[i.Local] || r.ignore(i.Local, i.Directory, r.patterns()) {
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
						if e == nil && (!os.SameFile(before, opened) || opened.Size() < v.Size) {
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
			e := SealContent(pw, io.TeeReader(io.LimitReader(&contextReader{ctx, src}, v.Size), h), r.key, r.folder, t.BlobID, pid)
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
			if !writable || r.ignore(v.Local, v.Directory, r.patterns()) || r.rejectedSameHash(v.Hash) {
				continue
			}
			deleted = append(deleted, p)
		}
	}
	sort.Slice(deleted, func(i, j int) bool { return len(deleted[i]) > len(deleted[j]) })

	for len(deleted) > 0 {
		n := min(len(deleted), 256)
		group := deleted[:n]
		deleted = deleted[n:]
		mutations := []Mutation{}
		for _, p := range group {
			i := r.index[p]
			pid, _ := PathID(r.key, r.folder, p)
			if r.exactExists(i.Local) {
				continue
			}
			if _, pending := r.retryRows[pid]; pending {
				continue
			}
			mutations = append(mutations, Mutation{PathID: pid, BaseVersion: i.Version, Deleted: true})
		}
		if len(mutations) == 0 {
			continue
		}
		if e = r.validRoot(); e != nil {
			return e
		}
		d, err := r.client.Commit(ctx, r.folder, mutations)
		if errors.Is(err, ErrConflict) {
			var conflict *ConflictError
			if errors.As(err, &conflict) {
				bad := map[string]bool{}
				for _, pid := range conflict.Paths {
					bad[pid] = true
				}
				retry := mutations[:0]
				for _, m := range mutations {
					if !bad[m.PathID] {
						retry = append(retry, m)
					}
				}
				if len(retry) > 0 {
					d, err = r.client.Commit(ctx, r.folder, retry)
				}
			}
		}
		if err != nil {
			if !errors.Is(err, ErrConflict) {
				record(err)
			}
			continue
		}
		for _, row := range d.Rows {
			p := r.byID[row.PathID]
			i := r.index[p]
			i.Deleted = true
			i.Hash = ""
			i.Version = row.Version
			if e = r.save(i); e != nil {
				return e
			}
		}
	}
	record(r.pull(ctx, local))
	if deleteErr == nil && r.allowMassDelete {
		files, bytes := r.presentContent(local)
		r.baselineFiles = int64(files)
		r.baselineBytes = bytes
		if e := r.storeBaseline(); e != nil {
			return e
		}
	}
	if firstError == nil {
		if deleteErr == nil {
			r.allowMassDelete = false
		}
		if r.adoption {
			if _, e := r.db.Exec("DELETE FROM config WHERE key='adoption'"); e != nil {
				return e
			}
		}
		r.adoption = false
	}
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
	affected := map[string]bool{}
	if delta.Full {
		for _, set := range []map[string]Row{r.retryRows, r.ignoredRows, r.quarantine} {
			for pid := range set {
				affected[pid] = true
			}
		}
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
	r.patterns()
	if r.ignoredChanged {
		r.ignoredChanged = false
		for pid, row := range r.ignoredRows {
			delta.Rows = append(delta.Rows, row)
			delete(r.ignoredRows, pid)
		}
	}
	if len(delta.Rows) == 0 && !delta.Full {
		return nil
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
	// Path-bound ciphertext needs a new upload for a rename, but a peer can
	// move its verified plaintext instead of downloading the same bytes again.
	moved := map[string]bool{}
	sources := map[string][]Row{}
	for _, row := range delta.Rows {
		if row.Deleted {
			if p, ok := r.byID[row.PathID]; ok {
				i := r.index[p]
				if !i.Deleted && !i.Directory && !r.ignore(i.Local, false, r.patterns()) {
					sources[i.Hash] = append(sources[i.Hash], row)
				}
			}
		}
	}
	for _, row := range delta.Rows {
		m, ok := metadata[row.PathID]
		if !ok || m.Directory {
			continue
		}
		for len(sources[m.Hash]) > 0 {
			list := sources[m.Hash]
			old := list[len(list)-1]
			sources[m.Hash] = list[:len(list)-1]
			if old.Version != row.Version {
				continue
			}
			i := r.index[r.byID[old.PathID]]
			if e = r.moveRemote(row, m, i); e != nil {
				continue
			}
			moved[old.PathID] = true
			moved[row.PathID] = true
			break
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
		if moved[row.PathID] {
			delete(r.retryRows, row.PathID)
			delete(r.retryAt, row.PathID)
			continue
		}
		if prior, ok := r.retryRows[row.PathID]; ok && prior.Version == row.Version && time.Now().Before(r.retryAt[row.PathID]) {
			continue
		}
		delete(r.retryRows, row.PathID)
		delete(r.retryAt, row.PathID)
		if old, ok := r.quarantine[row.PathID]; ok && old.Version == row.Version {
			continue
		}
		if e = r.apply(ctx, row); e != nil {
			if errors.Is(e, errDirectoryPending) {
				r.retryRows[row.PathID] = row
				continue
			}
			if errors.Is(e, ErrIntegrity) {
				r.quarantine[row.PathID] = row
			} else {
				r.retryRows[row.PathID] = row
				if isTransferError(e) || errors.Is(e, errAfterDownload) {
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
	for _, row := range delta.Rows {
		affected[row.PathID] = true
	}
	for pid := range affected {
		if _, e = stateTx.Exec("DELETE FROM pending WHERE id=?", pid); e != nil {
			return e
		}
		for kind, set := range map[string]map[string]Row{"quarantine": r.quarantine, "ignored": r.ignoredRows, "retry": r.retryRows} {
			if row, ok := set[pid]; ok {
				b, me := json.Marshal(pendingRow{Row: row, Kind: kind, RetryAt: r.retryAt[pid]})
				if me != nil {
					return me
				}
				if _, me = stateTx.Exec("INSERT OR REPLACE INTO pending(id,data) VALUES(?,?)", pid, b); me != nil {
					return me
				}
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
	if _, e = stateTx.Exec("INSERT OR REPLACE INTO config(key,value) VALUES('ignore',?)", r.ignoreFingerprint); e != nil {
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
	if e := renameIfAbsent(r.root, local, name); e != nil {
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

var errAfterDownload = errors.New("local failure after download")

// A directory tombstone waits while tracked children are still live.
var errDirectoryPending = errors.New("directory delete waits for tracked children")

func (r *Replica) apply(ctx context.Context, row Row) (err error) {
	downloaded := false
	defer func() {
		if err != nil && downloaded && !errors.Is(err, ErrIntegrity) && !errors.Is(err, errLocalObstacle) && !isTransferError(err) {
			err = fmt.Errorf("%w: %w", errAfterDownload, err)
		}
	}()
	p, known := r.byID[row.PathID]
	i := r.index[p]
	if row.Deleted {
		r.tombstones[row.PathID] = max(r.tombstones[row.PathID], row.Version)
		if !known && len(row.Metadata) > 0 {
			m, e := OpenMetadata(r.key, r.folder, row)
			if e != nil {
				return e
			}
			p = m.Path
			i = IndexEntry{Path: p, Local: p, Hash: m.Hash, Directory: m.Directory, Mode: m.Mode}
			v, err := r.inspect(p, p)
			if err == nil && (v.Hash != m.Hash || r.adoption) {
				i.Deleted = true
				i.Version = row.Version
				return r.save(i)
			}
			if err == nil {
				i.Mode = v.Mode
			}
			known = true
		}
		if !known {
			return nil
		}
		if r.ignore(i.Local, i.Directory, r.patterns()) {
			i.Deleted = true
			i.Version = row.Version
			return r.save(i)
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
							return errDirectoryPending
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
	if prior, ok := r.index[p]; ok && !prior.Deleted && prior.Local != p && !r.exactExists(prior.Local) && r.exactExists(p) {
		if actual, err := r.inspect(p, p); err == nil && actual.Hash == prior.Hash {
			prior.Local = p
			if err = r.save(prior); err != nil {
				return err
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
	if known && !i.Deleted && !i.Awaiting && !r.exactExists(i.Local) {
		generation, seen := r.missingDeferred[row.PathID]
		if !seen {
			r.missingDeferred[row.PathID] = r.generation
			return ErrBusy
		}
		if generation == r.generation {
			return ErrBusy
		}
	}
	delete(r.missingDeferred, row.PathID)
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
			e = applyMode(f, m.Mode, false, r.localMode(local, false))
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
			return r.save(IndexEntry{Path: p, Local: local, Hash: m.Hash, Version: row.Version, Mode: r.localMode(local, false)})
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
		if e = r.chmod(local, m.Mode, true); e != nil {
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
		return r.save(IndexEntry{Path: p, Local: local, Hash: "directory", Version: row.Version, Directory: true, Mode: r.localMode(local, true)})
	}
	if e = durableMkdirAll(r.root, path.Dir(local)); e != nil {
		return e
	}
	if e = r.safe(local); e != nil {
		return e
	}
	if _, ve := r.inspect(p, local); ve != nil && !errors.Is(ve, os.ErrNotExist) {
		return ve
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
	downloaded = true
	e = OpenContent(counter, received, r.key, r.folder, row.BlobID, row.PathID)
	stream.Close()
	e = storedContentError(e, received.n, row.SealedSize)
	if e == nil && (counter.n != m.Size || fmt.Sprintf("%x", h.Sum(nil)) != m.Hash) {
		e = ErrIntegrity
	}
	if e == nil {
		e = applyMode(f, m.Mode, false, r.localMode(local, false))
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
	// A no-overwrite rename is used when the destination should be absent. Replacing
	// the indexed bytes is the one path where the destination is expected to exist.
	v, ve := r.inspect(p, local)
	replaceExisting := false
	if ve == nil {
		dirty := (!known || !sameFile(v, i)) && (v.Directory || v.Hash != m.Hash || v.Size != m.Size)
		if dirty && !v.Directory && r.adoption {
			conflict := variantPath(local, " (conflict from authority "+randomID()+")")
			if e = renameIfAbsent(r.root, temp, conflict); e != nil {
				return e
			}
			if e = r.syncParent(conflict); e != nil {
				return e
			}
			r.mu.Lock()
			r.status.Conflicts = append(r.status.Conflicts, conflict)
			r.mu.Unlock()
			return r.save(IndexEntry{Path: p, Local: local, Hash: m.Hash, Version: row.Version, Mode: r.localMode(local, false)})
		}
		if dirty || v.Directory {
			if _, e = r.preserve(local); e != nil {
				return e
			}
		} else {
			replaceExisting = true
		}
	} else if !errors.Is(ve, os.ErrNotExist) {
		return ve
	}
	if e = r.safe(local); e != nil {
		return e
	}
	if replaceExisting {
		if e = r.root.Rename(temp, local); e != nil {
			return e
		}
	} else if e = renameIfAbsent(r.root, temp, local); e != nil {
		return e
	}
	if e = r.syncParent(local); e != nil {
		return e
	}
	return r.save(IndexEntry{Path: p, Local: local, Hash: m.Hash, Version: row.Version, Mode: r.localMode(local, false)})
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
	if r.ruleSnap != nil {
		return r.ruleSnap
	}
	rules, e := r.readRules()
	if e != nil {
		return append([]string(nil), r.opts.Ignore...)
	}
	return rules
}

// readRules loads ignore rules once. The caller of Sync keeps the result for
// the whole pass, so an editor replacing .drivesyncignore cannot open a window
// with no rules. A file that exists but is not a readable regular file pauses.
func (r *Replica) readRules() ([]string, error) {
	patterns := append([]string(nil), r.opts.Ignore...)
	info, e := r.root.Lstat(".drivesyncignore")
	if e == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		f, oe := openLocal(r.root, ".drivesyncignore")
		if oe != nil {
			return nil, fmt.Errorf(".drivesyncignore unreadable; sync paused: %w", oe)
		}
		b, re := io.ReadAll(io.LimitReader(f, 64*1024))
		f.Close()
		if re != nil {
			return nil, fmt.Errorf(".drivesyncignore unreadable; sync paused: %w", re)
		}
		patterns = append(patterns, strings.Split(string(b), "\n")...)
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, fmt.Errorf(".drivesyncignore must be a readable regular file; sync paused")
	}
	fingerprint := fmt.Sprintf("%x", blake3.Sum256([]byte(strings.Join(patterns, "\n"))))
	if fingerprint != r.ignoreFingerprint {
		r.ignoredChanged = true
		r.ignoreFingerprint = fingerprint
	}
	return patterns, nil
}

func (r *Replica) presentContent(local map[string]LocalFile) (int, int64) {
	files := 0
	var bytes int64
	patterns := r.patterns()
	for p, entry := range r.index {
		if entry.Deleted || entry.Directory || r.localBlocked(p) || r.ignore(entry.Local, entry.Directory, patterns) {
			continue
		}
		v, ok := local[p]
		if !ok || v.Directory {
			continue
		}
		files++
		bytes += v.Size
	}
	return files, bytes
}

func (r *Replica) storeBaseline() error {
	_, e := r.db.Exec(`INSERT INTO config(key,value) VALUES('baseline-files',?),('baseline-bytes',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(r.baselineFiles, 10), strconv.FormatInt(r.baselineBytes, 10))
	return e
}

func (r *Replica) rejectedSameHash(hash string) bool {
	if hash == "" || hash == "directory" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rej := range r.rejected {
		if rej.Hash == hash {
			return true
		}
	}
	return false
}

func (r *Replica) hashUnportable(local string) string {
	f, e := openLocal(r.root, local)
	if e != nil {
		f, e = os.Open(filepath.Join(r.dir, filepath.FromSlash(local)))
		if e != nil {
			return ""
		}
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		return ""
	}
	h := blake3.New()
	if _, e = io.Copy(h, f); e != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func tightenState(dir string) {
	entries, e := os.ReadDir(dir)
	if e != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		_ = os.Chmod(filepath.Join(dir, entry.Name()), 0600)
	}
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
			return !errors.Is(e, os.ErrNotExist)
		}
		entries, e := f.ReadDir(-1)
		f.Close()
		if e != nil {
			return !errors.Is(e, os.ErrNotExist)
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
			return r.blockedLocal["."]
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

func globPath(pattern, names []string) bool {
	if len(pattern) == 0 {
		return len(names) == 0
	}
	if pattern[0] == "**" {
		for i := 0; i <= len(names); i++ {
			if globPath(pattern[1:], names[i:]) {
				return true
			}
		}
		return false
	}
	if len(names) == 0 {
		return false
	}
	ok, _ := path.Match(pattern[0], names[0])
	return ok && globPath(pattern[1:], names[1:])
}

func (r *Replica) localMode(local string, directory bool) uint32 {
	if info, e := r.root.Lstat(local); e == nil {
		return uint32(info.Mode().Perm())
	}
	if directory {
		return 0700
	}
	return 0600
}
func applyMode(f *os.File, remote uint32, directory bool, local uint32) error {
	mode := local&^0100 | remote&0100 | 0600
	if directory {
		mode |= 0100
	}
	return f.Chmod(os.FileMode(mode))
}
func (r *Replica) chmod(local string, remote uint32, directory bool) error {
	f, e := openLocal(r.root, local)
	if e != nil {
		return e
	}
	defer f.Close()
	return applyMode(f, remote, directory, r.localMode(local, directory))
}

func (r *Replica) moveRemote(row Row, m FileMetadata, old IndexEntry) error {
	if foldPath(old.Local) == foldPath(m.Path) && old.Local != m.Path {
		return ErrBusy
	}
	local := r.localPath(m.Path, row.PathID)
	if known, ok := r.index[m.Path]; ok && !known.Deleted && !r.exactExists(known.Local) {
		return ErrBusy
	}
	if r.ignore(local, false, r.patterns()) {
		return ErrBusy
	}
	v, e := r.inspect(old.Path, old.Local)
	if e != nil {
		return e
	}
	if v.Hash != m.Hash || v.Size != m.Size {
		return ErrBusy
	}
	if r.exactExists(local) && local != old.Local {
		return ErrBusy
	}
	if e = r.safe(local); e != nil {
		return e
	}
	if e = durableMkdirAll(r.root, path.Dir(local)); e != nil {
		return e
	}
	if local != old.Local {
		if e = renameIfAbsent(r.root, old.Local, local); e != nil {
			return e
		}
	}
	if e = r.chmod(local, m.Mode, false); e != nil {
		return e
	}
	f, e := openLocal(r.root, local)
	if e != nil {
		return e
	}
	e = f.Sync()
	f.Close()
	if e != nil {
		return e
	}
	if e = r.syncParent(local); e != nil {
		return e
	}
	if e = r.syncParent(old.Local); e != nil {
		return e
	}
	old.Deleted = true
	old.Version = row.Version
	if e = r.save(old); e != nil {
		return e
	}
	pid, _ := PathID(r.key, r.folder, old.Path)
	r.tombstones[pid] = row.Version
	return r.save(IndexEntry{Path: m.Path, Local: local, Hash: m.Hash, Version: row.Version, Mode: r.localMode(local, false)})
}
