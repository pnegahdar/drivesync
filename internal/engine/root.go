package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	"github.com/zeebo/blake3"
)

const rootMarker = ".drivesync-root"
const stateMarker = ".drivesync-state"

type rootIdentity struct {
	Folder, Token string
	Inode         uint64
}

func identifyRoot(dir string) (rootIdentity, error) {
	marker := filepath.Join(dir, rootMarker)
	info, e := os.Lstat(marker)
	if e != nil {
		return rootIdentity{}, e
	}
	if !info.Mode().IsRegular() {
		return rootIdentity{}, ErrInvalid
	}
	b, e := os.ReadFile(marker)
	if e != nil {
		return rootIdentity{}, e
	}
	var id rootIdentity
	if e = json.Unmarshal(b, &id); e != nil || id.Token == "" {
		return id, ErrInvalid
	}
	info, e = os.Lstat(dir)
	if e != nil || !info.IsDir() {
		return id, fmt.Errorf("root is not a real directory: %w", ErrInvalid)
	}
	v := reflect.Indirect(reflect.ValueOf(info.Sys()))
	if v.IsValid() && v.Kind() == reflect.Struct {
		for name, dst := range map[string]*uint64{"Ino": &id.Inode} {
			field := v.FieldByName(name)
			if field.IsValid() && field.CanUint() {
				*dst = field.Uint()
			} else if field.IsValid() && field.CanInt() {
				*dst = uint64(field.Int())
			}
		}
	}
	return id, nil
}

// Markers survive Close: nesting remains unsafe while state exists elsewhere.
func checkAttachments(ctx context.Context, dir, folder string) error {
	for p := filepath.Dir(dir); ; p = filepath.Dir(p) {
		if _, e := os.Lstat(filepath.Join(p, rootMarker)); e == nil {
			return fmt.Errorf("nested attachment: %w", ErrInvalid)
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	rootInfo, e := os.Lstat(dir)
	if e != nil {
		return e
	}
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if ce := ctx.Err(); ce != nil {
			return ce
		}
		if e != nil {
			if p == dir {
				return e
			}
			return filepath.SkipDir
		}
		if d.IsDir() && p != dir {
			info, err := d.Info()
			if err != nil || device(info) != device(rootInfo) {
				return filepath.SkipDir
			}
		}
		if d.Name() != rootMarker && d.Name() != stateMarker {
			return nil
		}
		if filepath.Dir(p) != dir {
			return fmt.Errorf("attachment contains another root: %w", ErrInvalid)
		}
		id, e := identifyRoot(dir)
		if e != nil {
			return e
		}
		if id.Folder != folder {
			return fmt.Errorf("root belongs to another folder: %w", ErrInvalid)
		}
		return nil
	})
}

func prepareState(dir, folder, state string) (string, error) {
	var e error
	dir, e = filepath.EvalSymlinks(dir)
	if e != nil {
		return "", e
	}
	if state == "" {
		state, e = os.UserCacheDir()
		if e != nil {
			return "", e
		}
		sum := blake3.Sum256([]byte(dir))
		state = filepath.Join(state, "drivesync", folder, fmt.Sprintf("%x", sum[:16]))
	}
	state, e = filepath.Abs(state)
	if e != nil {
		return "", e
	}
	// Resolve existing ancestors before validating or creating anything.
	base, tail := state, []string{}
	for {
		resolved, re := filepath.EvalSymlinks(base)
		if re == nil {
			state = resolved
			for i := len(tail) - 1; i >= 0; i-- {
				state = filepath.Join(state, tail[i])
			}
			break
		}
		if !errors.Is(re, os.ErrNotExist) || base == filepath.Dir(base) {
			return "", re
		}
		tail = append(tail, filepath.Base(base))
		base = filepath.Dir(base)
	}
	rel, e := filepath.Rel(dir, state)
	if e != nil || rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return "", ErrInvalid
	}
	for p := filepath.Dir(state); ; p = filepath.Dir(p) {
		if _, e := os.Lstat(filepath.Join(p, rootMarker)); e == nil {
			return "", fmt.Errorf("state inside an attachment: %w", ErrInvalid)
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	// Refuse unrelated files before chmod or cleanup, even when explicitly set.
	entries, e := os.ReadDir(state)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return "", e
	}
	if len(entries) > 0 {
		b, e := os.ReadFile(filepath.Join(state, stateMarker))
		if e != nil || string(b) != folder+"/"+dir {
			return "", fmt.Errorf("state directory is not dedicated to this replica: %w", ErrInvalid)
		}
	}
	if e = durableDirectory(state); e != nil {
		return "", e
	}
	state, e = filepath.EvalSymlinks(state)
	if e != nil {
		return "", e
	}
	if _, e = os.Lstat(filepath.Join(state, stateMarker)); errors.Is(e, os.ErrNotExist) {
		if e = writeMarker(filepath.Join(state, stateMarker), []byte(folder+"/"+dir)); e != nil {
			return "", e
		}
	}
	return state, nil
}

func writeMarker(name string, b []byte) error {
	f, e := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
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
	d, e := os.Open(filepath.Dir(name))
	if e != nil {
		return e
	}
	defer d.Close()
	return syncDirectoryFile(d)
}

func (r *Replica) bindRoot() error {
	var saved string
	_ = r.db.QueryRow("SELECT value FROM config WHERE key='root'").Scan(&saved)
	if saved != "" {
		if e := json.Unmarshal([]byte(saved), &r.identity); e != nil {
			return e
		}
		return nil
	}
	if _, e := os.Lstat(filepath.Join(r.dir, rootMarker)); errors.Is(e, os.ErrNotExist) {
		id := rootIdentity{Folder: r.folder, Token: randomID()}
		b, _ := json.Marshal(id)
		if e = writeMarker(filepath.Join(r.dir, rootMarker), b); e != nil {
			return e
		}
	}
	id, e := identifyRoot(r.dir)
	if e != nil {
		return e
	}
	d, e := os.Open(r.dir)
	if e != nil {
		return e
	}
	e = syncDirectoryFile(d)
	d.Close()
	if e != nil {
		return e
	}
	r.identity = id
	b, _ := json.Marshal(id)
	_, e = r.db.Exec("INSERT INTO config(key,value) VALUES('root',?)", string(b))
	return e
}

func (r *Replica) validRoot() error {
	id, e := identifyRoot(r.dir)
	info, le := os.Lstat(r.dir)
	held, he := r.root.Stat(".")
	if e != nil || le != nil || he != nil || !info.IsDir() || !os.SameFile(info, held) || id != r.identity {
		return fmt.Errorf("attachment root identity missing or changed; sync paused")
	}
	return nil
}
func (r *Replica) deleteSafety(local map[string]LocalFile) error {
	if e := r.validRoot(); e != nil {
		return e
	}
	tracked, missing := 0, 0
	moved := map[string]int{}
	for p, v := range local {
		if old, known := r.index[p]; !known || old.Deleted {
			moved[v.Hash]++
		}
	}
	patterns := r.patterns()
	for p, entry := range r.index {
		if entry.Deleted || r.localBlocked(p) || r.ignore(entry.Local, entry.Directory, patterns) {
			continue
		}
		tracked++
		if _, ok := local[p]; !ok {
			if moved[entry.Hash] > 0 {
				moved[entry.Hash]--
			} else {
				missing++
			}
		}
	}
	if !r.allowMassDelete && tracked >= 5 && missing*5 >= tracked*4 {
		return fmt.Errorf("%d of %d tracked entries disappeared; deletes paused (Retry acknowledges intentional removal)", missing, tracked)
	}
	return nil
}

// Device numbers are ephemeral across remounts. They bound each traversal, but
// are deliberately not part of the persistent root identity.
func device(info os.FileInfo) uint64 {
	if info == nil || info.Sys() == nil {
		return 0
	}
	switch s := info.Sys().(type) {
	case *syscall.Stat_t:
		return uint64(s.Dev)
	case syscall.Stat_t:
		return uint64(s.Dev)
	}
	v := reflect.Indirect(reflect.ValueOf(info.Sys()))
	if v.IsValid() && v.Kind() == reflect.Struct {
		f := v.FieldByName("Dev")
		if f.CanUint() {
			return f.Uint()
		}
		if f.CanInt() {
			return uint64(f.Int())
		}
	}
	return 0
}

// dirBounds is one traversal's root device and the directories already checked
// for a mount crossing or a foreign drivesync marker.
type dirBounds struct {
	dev  uint64
	seen map[string]error
}

func (r *Replica) directoryBoundary(p string, info os.FileInfo) error {
	if info == nil {
		return fmt.Errorf("mount boundary: %s", p)
	}
	rootDev := uint64(0)
	cached := false
	if r.scanBounds != nil {
		rootDev = r.scanBounds.dev
		cached = true
	} else {
		root, e := r.root.Stat(".")
		if e != nil {
			return e
		}
		rootDev = device(root)
	}
	// Compare the device already on this stat. Do not stat the root again.
	if device(info) != rootDev {
		return fmt.Errorf("mount boundary: %s", p)
	}
	if !info.IsDir() {
		return nil
	}
	if cached {
		if e, ok := r.scanBounds.seen[p]; ok {
			return e
		}
	}
	var e error
	for _, name := range []string{rootMarker, stateMarker} {
		_, le := r.root.Lstat(pathJoin(p, name))
		if le == nil {
			e = fmt.Errorf("foreign drivesync marker: %s", p)
			break
		}
		if !errors.Is(le, os.ErrNotExist) {
			e = le
			break
		}
	}
	if cached {
		r.scanBounds.seen[p] = e
	}
	return e
}
func pathJoin(p, name string) string { return filepath.ToSlash(filepath.Join(p, name)) }

// Acknowledging a changed root starts fresh adoption. No entry from the old
// index can contribute a delete, even when the restored tree is empty.
func (r *Replica) rebindRoot() error {
	info, e := os.Lstat(r.dir)
	if e != nil || !info.IsDir() {
		return fmt.Errorf("root must be a real directory")
	}
	release, e := attachmentAdmission(r.dir)
	if e != nil {
		return e
	}
	defer release()
	if e = checkAttachments(context.Background(), r.dir, r.folder); e != nil {
		return e
	}
	root, e := os.OpenRoot(r.dir)
	if e != nil {
		return e
	}
	defer func() {
		if root != nil {
			root.Close()
		}
	}()
	marker := filepath.Join(r.dir, rootMarker)
	if _, e = os.Lstat(marker); errors.Is(e, os.ErrNotExist) {
		b, _ := json.Marshal(rootIdentity{Folder: r.folder, Token: randomID()})
		if e = writeMarker(marker, b); e != nil {
			return e
		}
	}
	id, e := identifyRoot(r.dir)
	if e != nil || id.Folder != r.folder {
		return ErrInvalid
	}
	oldMarker, _ := r.rootLock.Stat()
	newMarker, e := os.Lstat(marker)
	if e != nil {
		return e
	}
	var lock *os.File
	if oldMarker == nil || !os.SameFile(oldMarker, newMarker) {
		lock, e = lockState(marker)
		if e != nil {
			return e
		}
		defer func() {
			if lock != nil {
				lock.Close()
			}
		}()
	}
	tx, e := r.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, table := range []string{"entries", "pending", "tombstones"} {
		if _, e = tx.Exec("DELETE FROM " + table); e != nil {
			return e
		}
	}
	b, _ := json.Marshal(id)
	if _, e = tx.Exec("INSERT OR REPLACE INTO config(key,value) VALUES('root',?),('version','0')", string(b)); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	r.root.Close()
	r.root = root
	root = nil
	if lock != nil {
		r.rootLock.Close()
		r.rootLock = lock
		lock = nil
	}
	r.identity = id
	r.version = 0
	r.index = map[string]IndexEntry{}
	r.byID = map[string]string{}
	r.byLocal = map[string]string{}
	r.byFold = map[string]string{}
	r.tombstones = map[string]uint64{}
	r.missingDeferred = map[string]uint64{}
	r.quarantine = map[string]Row{}
	r.retryRows = map[string]Row{}
	r.ignoredRows = map[string]Row{}
	r.adoption = true
	return nil
}
