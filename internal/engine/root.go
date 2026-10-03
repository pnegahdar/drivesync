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

	"github.com/zeebo/blake3"
)

const rootMarker = ".drivesync-root"
const stateMarker = ".drivesync-state"

type rootIdentity struct {
	Folder, Token string
	Device, Inode uint64
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
	info, e = os.Stat(dir)
	if e != nil {
		return id, e
	}
	v := reflect.Indirect(reflect.ValueOf(info.Sys()))
	if v.IsValid() && v.Kind() == reflect.Struct {
		for name, dst := range map[string]*uint64{"Dev": &id.Device, "Ino": &id.Inode} {
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
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, e error) error {
		if ce := ctx.Err(); ce != nil {
			return ce
		}
		if e != nil {
			return e
		}
		if d.Name() != rootMarker {
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
	if e != nil || id != r.identity {
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
