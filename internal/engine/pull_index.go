package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
)

const pullIndexBatchSize = 1000

var errPullIndex = errors.New("local publication batch failed")

// pullIndexBatch keeps publication order: files in place, their directories
// synced, then one index transaction. Only syncMu's owner sees tentative entries.
// Remembering them before commit reserves local spellings for case collisions.
// A failed barrier or transaction restores the previous in-memory index too.
//
// Files are created through verified parent handles in dirs. flush reopens
// every directory the batch used without following components, requires the
// identity its handle had, and only then checks placeholders in it, syncs it,
// and commits. written is what flush must sync: each directory that received
// an entry, and each ancestor of one not already synced in this pull.
type pullIndexBatch struct {
	replica   *Replica
	ctx       context.Context
	entries   []IndexEntry
	positions map[string]int
	prior     map[string]*IndexEntry
	dirs      *dirCache
	written   map[string]bool
	durable   map[string]placeholderID
	err       error
}

func newPullIndexBatch(r *Replica, ctx context.Context) *pullIndexBatch {
	b := &pullIndexBatch{replica: r, ctx: ctx, dirs: r.newDirCache(), durable: map[string]placeholderID{}}
	b.clear()
	return b
}

// mark records what a verified walk passed: a new component dirties its
// parent, and an ancestor not yet synced under this identity is synced too,
// since an earlier failed batch may have created it.
func (b *pullIndexBatch) mark(dir string, info os.FileInfo, created bool) {
	if created {
		b.written[path.Dir(dir)] = true
	}
	if b.durable[dir] != (placeholderID{device(info), inodeOf(info)}) {
		b.written[dir] = true
	}
}

// prepareParent returns the verified handle of local's parent, creating
// missing directories. The batch owns the handle until the next flush or
// discard. Address children by their base name only.
func (b *pullIndexBatch) prepareParent(local string) (*verifiedDir, error) {
	parent := path.Dir(local)
	d, e := b.dirs.dir(parent, true, b.mark)
	if e != nil {
		return nil, e
	}
	b.written[parent] = true
	return d, nil
}

// directory makes local and its missing parents as 0700 directories and
// returns local's verified handle, owned by the batch. Nothing is synced now:
// flush syncs local and its ancestors before the index acknowledges it.
func (b *pullIndexBatch) directory(local string) (*verifiedDir, error) {
	d, e := b.dirs.dir(local, true, b.mark)
	if e != nil {
		return nil, e
	}
	b.written[local] = true
	if local != "." {
		b.written[path.Dir(local)] = true
	}
	return d, nil
}

// lookup is a no-follow stat of local as one name in its parent's verified
// handle, opened (never created) once per batch. A missing parent is
// os.ErrNotExist. The parent is re-verified by flush like any other.
func (b *pullIndexBatch) lookup(local string) (os.FileInfo, error) {
	return b.dirs.Lstat(local)
}

func (b *pullIndexBatch) save(entry IndexEntry) error {
	if b.err != nil {
		return b.err
	}
	if n, ok := b.positions[entry.Path]; ok {
		b.entries[n] = entry
	} else {
		b.positions[entry.Path] = len(b.entries)
		b.entries = append(b.entries, entry)
		if old, ok := b.replica.index[entry.Path]; ok {
			b.prior[entry.Path] = &old
		} else {
			b.prior[entry.Path] = nil
		}
	}
	b.replica.Remember(entry)
	if len(b.entries) >= pullIndexBatchSize {
		return b.flush()
	}
	return nil
}
func (b *pullIndexBatch) discard() {
	for p, old := range b.prior {
		b.replica.unremember(p)
		if old != nil {
			b.replica.Remember(*old)
		}
	}
	b.clear()
}
func (b *pullIndexBatch) clear() {
	b.dirs.Close()
	b.entries = nil
	b.positions = map[string]int{}
	b.prior = map[string]*IndexEntry{}
	b.written = map[string]bool{}
}
func (b *pullIndexBatch) flush() (err error) {
	if b.err != nil {
		return b.err
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", errPullIndex, err)
			b.err = err
			b.discard()
		}
	}()
	if len(b.entries) == 0 {
		b.clear()
		return nil
	}
	if err = b.ctx.Err(); err != nil {
		return err
	}
	if err = b.replica.validRoot(); err != nil {
		return err
	}
	// A missing or exchanged placeholder must not be acknowledged as a local
	// delete merely because it disappeared during a long publication sweep.
	// Each directory is reopened once without following components; its
	// placeholders are single-component stats inside that handle.
	b.dirs.closeHandles()
	children := map[string][]IndexEntry{}
	for _, entry := range b.entries {
		if physicalPlaceholder(entry) {
			parent := path.Dir(entry.Local)
			children[parent] = append(children[parent], entry)
		}
	}
	set := map[string]bool{}
	for dir := range b.written {
		set[dir] = true
	}
	for dir := range b.dirs.ids {
		set[dir] = true
	}
	for dir := range children {
		set[dir] = true
	}
	dirs := make([]string, 0, len(set))
	for dir := range set {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	for _, dir := range dirs {
		if err = b.verifyDirectory(dir, children[dir]); err != nil {
			return err
		}
	}
	tx, err := b.replica.db.BeginTx(b.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	w := newIndexWriter(b.ctx, tx)
	for _, entry := range b.entries {
		if err = w.store(entry); err != nil {
			return err
		}
	}
	if err = w.close(); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	b.clear()
	return nil
}

// verifyDirectory reopens dir, checks the identity its handle had, checks
// each placeholder in it, and syncs dir when this batch wrote into it.
func (b *pullIndexBatch) verifyDirectory(dir string, children []IndexEntry) error {
	d, err := b.replica.openVerifiedDir(dir, false, nil)
	if errors.Is(err, os.ErrNotExist) {
		return ErrBusy
	}
	if err != nil {
		return err
	}
	defer d.Close()
	if id, ok := b.dirs.ids[dir]; ok && d.id() != id {
		return ErrBusy
	}
	for _, entry := range children {
		info, err := d.root.Lstat(path.Base(entry.Local))
		if errors.Is(err, os.ErrNotExist) {
			return ErrBusy
		}
		if err != nil {
			return err
		}
		if !b.replica.ownsPlaceholder(entry, info) {
			return ErrBusy
		}
	}
	if !b.written[dir] {
		return nil
	}
	if err = syncDirectoryFile(d.file); err != nil {
		return err
	}
	b.durable[dir] = d.id()
	return nil
}
