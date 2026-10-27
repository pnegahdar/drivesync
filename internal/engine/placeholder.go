package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"time"

	"github.com/zeebo/blake3"
)

type placeholderID struct{ dev, ino uint64 }

func (r *Replica) ownsPlaceholder(i IndexEntry, info os.FileInfo) bool {
	return i.Placeholder && !i.RemoteOnly && !i.Deleted && info.Mode().IsRegular() && info.Size() == 0 && device(info) == i.Dev && inodeOf(info) == i.Inode
}
func (r *Replica) placeholderAt(i IndexEntry, local string) bool {
	info, e := r.root.Lstat(local)
	return e == nil && r.ownsPlaceholder(i, info)
}

// Empty 0000 files have no local contents to lose. An authenticated incoming
// row can recover one left behind before its index transaction committed.
func emptyPlaceholder(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Size() == 0 && info.Mode().Perm() == 0
}
func (r *Replica) writePlaceholder(row Row, m FileMetadata, local string, old IndexEntry) error {
	if m.Size < 0 || m.Size > MaxRequestBytes || SealedSize(m.Size) != row.SealedSize {
		return ErrIntegrity
	}
	hash, he := parseHash(m.Hash)
	blob, be := parseBlob(row.BlobID)
	if he != nil || be != nil || m.Hash == "" || row.BlobID == "" {
		return ErrIntegrity
	}
	var d *verifiedDir
	var e error
	if r.pullIndex != nil {
		// The batch owns this handle and re-verifies it before acknowledging.
		if d, e = r.pullIndex.prepareParent(local); e != nil {
			return e
		}
	} else {
		if e = durableMkdirAll(r.root, path.Dir(local)); e != nil {
			return e
		}
		if d, e = r.openVerifiedDir(path.Dir(local), false, nil); e != nil {
			return e
		}
		defer d.Close()
	}
	info, e := placeholderIn(d, path.Base(local), func(info os.FileInfo) bool {
		return r.ownsPlaceholder(old, info) || emptyPlaceholder(info)
	})
	if e != nil {
		return e
	}
	// Empty files need no data fsync. The parent barrier makes their directory
	// entries durable before the index can acknowledge them.
	if r.pullIndex == nil {
		if e = syncDirectoryFile(d.file); e != nil {
			return e
		}
	}
	return r.save(IndexEntry{Path: m.Path, Local: local, Hash: hash, Blob: blob, Version: row.Version, Mode: m.Mode, Placeholder: true, Size: m.Size, Dev: device(info), Inode: inodeOf(info)})
}

// placeholderIn publishes the empty, locked file through an exclusive temp
// name. Only single-component names are used inside the verified directory.
func placeholderIn(d *verifiedDir, name string, reusable func(os.FileInfo) bool) (os.FileInfo, error) {
	if info, e := d.root.Lstat(name); e == nil {
		if reusable(info) {
			return info, nil
		}
		return nil, ErrBusy
	} else if !errors.Is(e, os.ErrNotExist) {
		return nil, e
	}
	temp := ".drivesync-tmp-" + randomID()
	f, e := d.root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	defer d.root.Remove(temp)
	e = f.Chmod(0)
	info, se := f.Stat()
	if e == nil {
		e = se
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return nil, e
	}
	if e = renameIfAbsent(d.root, temp, name); e != nil {
		return nil, e
	}
	if !emptyPlaceholder(info) {
		return nil, ErrBusy
	}
	return info, nil
}

func (r *Replica) inspectPlaceholder(p, local string, info os.FileInfo) (LocalFile, bool, error) {
	v := LocalFile{Path: p, Local: local}
	if i, ok := r.index[p]; ok && i.Local == local && r.ownsPlaceholder(i, info) {
		v.Hash = i.hashString()
		v.Size = i.Size
		v.Mode = i.Mode
		return v, true, nil
	}
	if info.Mode().IsRegular() && info.Size() == 0 && info.Mode().Perm() == 0 {
		if r.placeholderSources == nil && r.scanBounds != nil {
			r.placeholderSourcesNow()
		}
		id := placeholderID{device(info), inodeOf(info)}
		if i, ok := r.placeholderSources[id]; ok {
			r.placeholderMoved[p] = i
			v.Hash = i.hashString()
			v.Size = i.Size
			v.Mode = i.Mode
			return v, true, nil
		}
		return v, false, errors.New("placeholder copied from elsewhere; fetch the original first")
	}
	return v, false, nil
}

// scanPlaceholders starts a scan's placeholder state. Sources, the tracked
// placeholders whose files are gone, are found only when an untracked empty
// 0000 file needs them, or else from the walk's own listings at its end.
func (r *Replica) scanPlaceholders() {
	r.placeholderSources = nil
	r.placeholderMoved = map[string]IndexEntry{}
}

// publishPlaceholder never replaces a file which has been written or exchanged
// while the sealed transfer was in flight. The original index path remains
// the content AEAD binding; its authenticated hash and size guard publication.
func (r *Replica) publishPlaceholder(ctx context.Context, i IndexEntry, local string, staged stagedDownload) (err error) {
	downloaded := false
	defer func() {
		if err != nil && downloaded && !errors.Is(err, ErrIntegrity) && !errors.Is(err, errLocalObstacle) && !isTransferError(err) {
			err = fmt.Errorf("%w: %w", errAfterDownload, err)
		}
	}()
	if e := ctx.Err(); e != nil {
		return e
	}
	if e := r.ctx.Err(); e != nil {
		return e
	}
	if e := r.validRoot(); e != nil {
		return e
	}
	current, ok := r.index[i.Path]
	if !ok || current.Version != i.Version || !current.Placeholder || current.Dev != i.Dev || current.Inode != i.Inode || current.Blob != i.Blob || current.Hash != i.Hash || current.Size != i.Size || current.RemoteOnly != i.RemoteOnly {
		return ErrBusy
	}
	if i.RemoteOnly {
		if e := r.materializeParents(local); e != nil {
			return e
		}
	} else if e := r.safe(local); e != nil {
		return e
	}
	if !r.fetchTargetUnchanged(i, local) {
		return ErrBusy
	}
	if i.Directory || i.Size < 0 || i.Size > MaxRequestBytes {
		return ErrIntegrity
	}
	row := r.contentRow(i)
	if staged.err != nil {
		return staged.err
	}
	temp := path.Join(path.Dir(local), ".drivesync-tmp-"+randomID())
	f, e := r.root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer r.root.Remove(temp)
	var stream io.ReadCloser
	if staged.name != "" {
		stream, e = os.Open(staged.name)
	} else {
		stream, e = r.download(ctx, row)
	}
	if e != nil {
		f.Close()
		return e
	}
	downloaded = true
	h := blake3.New()
	written := &countWriter{Writer: io.MultiWriter(f, h)}
	received := &countReader{Reader: &contextReader{ctx, stream}}
	e = OpenContent(written, received, r.key, r.folder, row.BlobID, row.PathID)
	stream.Close()
	e = storedContentError(e, received.n, row.SealedSize)
	if e == nil && (written.n != i.Size || !equalDigest(h.Sum(nil), i.Hash)) {
		e = ErrIntegrity
	}
	if e == nil {
		e = applyMode(f, i.Mode, false, 0600)
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
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = r.safe(local); e != nil {
		return e
	}
	if !r.fetchTargetUnchanged(i, local) {
		return ErrBusy
	}
	if i.RemoteOnly {
		e = renameIfAbsent(r.root, temp, local)
	} else {
		e = r.root.Rename(temp, local)
	}
	if e != nil {
		return e
	}
	if e = r.syncParent(local); e != nil {
		return e
	}
	if local != i.Local {
		// Retain the old path/version until the normal atomic rename commit succeeds.
		return nil
	}
	r.guidesDirty = true
	if e = r.save(IndexEntry{Path: i.Path, Local: local, Hash: i.Hash, Version: i.Version, Mode: r.localMode(local, false), Size: i.Size}); e != nil {
		return e
	}
	delete(r.retryRows, row.PathID)
	delete(r.retryAt, row.PathID)
	delete(r.quarantine, row.PathID)
	_, e = r.db.Exec("DELETE FROM pending WHERE id=?", row.PathID)
	return e
}
func (r *Replica) placeholderRetry(i IndexEntry, e error) {
	row := r.contentRow(i)
	if errors.Is(e, ErrIntegrity) {
		r.quarantine[row.PathID] = row
	} else {
		r.retryRows[row.PathID] = row
		if isTransferError(e) || errors.Is(e, errAfterDownload) {
			r.retryAt[row.PathID] = time.Now().Add(r.opts.RetryInterval)
		}
	}
	kind := "retry"
	if errors.Is(e, ErrIntegrity) {
		kind = "quarantine"
	}
	data, me := json.Marshal(pendingRow{Row: row, Kind: kind, RetryAt: r.retryAt[row.PathID]})
	if me == nil {
		_, me = r.db.Exec("INSERT INTO pending(id,data) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", row.PathID, data)
	}
	if me != nil {
		r.addError(me)
	}
	r.addError(fmt.Errorf("fetch %s: %w", i.Path, e))
}
func (r *Replica) hydrateRequested(ctx context.Context) error {
	present := map[string]bool{".": true}
	if !r.opts.OnDemand {
		dirs := []IndexEntry{}
		for _, i := range r.index {
			if i.RemoteOnly && i.Directory && !i.Deleted && r.remoteOnlyContainerPresent(i, present) {
				dirs = append(dirs, i)
			}
		}
		sort.Slice(dirs, func(a, b int) bool { return len(dirs[a].Local) < len(dirs[b].Local) })
		for _, i := range dirs {
			if e := r.materializeDirectory(i); e != nil {
				return e
			}
		}
		if len(r.collapsed) > 0 {
			r.guidesDirty = true
		}
	}
	entries := []IndexEntry{}
	dirs := r.newDirCache()
	defer dirs.Close()
	ordered := make([]IndexEntry, 0)
	for _, i := range r.index {
		if i.Placeholder && !i.Deleted && (!i.RemoteOnly || !r.opts.OnDemand) {
			ordered = append(ordered, i)
		}
	}
	// Keep each parent contiguous so a scan of more than 64 folders does not
	// churn the bounded handle cache in map iteration order.
	sort.Slice(ordered, func(a, b int) bool { return ordered[a].Local < ordered[b].Local })
	for _, i := range ordered {
		if !i.Placeholder || i.Deleted || r.ignore(i.Local, false, r.patterns()) {
			continue
		}
		row := r.contentRow(i)
		if _, busy := r.fetching[i.Path]; busy {
			continue
		}
		if q, ok := r.quarantine[row.PathID]; ok && q.Version == row.Version {
			continue
		}
		if time.Now().Before(r.retryAt[row.PathID]) {
			continue
		}
		if i.RemoteOnly {
			if !r.opts.OnDemand && r.remoteOnlyContainerPresent(i, present) {
				entries = append(entries, i)
			}
			continue
		}
		if info, e := dirs.Lstat(i.Local); e == nil && r.ownsPlaceholder(i, info) && (!r.opts.OnDemand || info.Mode().Perm() != 0) {
			entries = append(entries, i)
		}
	}
	var first error
	r.hydrateBatches(ctx, entries, false, func(i IndexEntry, s stagedDownload) {
		if e := r.publishPlaceholder(ctx, i, i.Local, s); e != nil {
			r.placeholderRetry(i, e)
			if first == nil {
				first = e
			}
		}
	})
	return first
}
func (r *Replica) hydrateBatches(ctx context.Context, entries []IndexEntry, wait bool, publish func(IndexEntry, stagedDownload)) {
	for len(entries) > 0 && ctx.Err() == nil {
		count := 0
		var bytes int64
		rows := []Row{}
		for _, i := range entries {
			if count >= r.opts.Downloads {
				break
			}
			size := SealedSize(i.Size)
			if size > downloadWindowBytes {
				if count == 0 {
					count = 1
				}
				break
			}
			if bytes > downloadWindowBytes-size {
				break
			}
			rows = append(rows, r.contentRow(i))
			bytes += size
			count++
		}
		var staged map[string]stagedDownload
		if wait {
			staged = r.stageFetchRows(ctx, rows)
		} else {
			staged = r.stageRows(ctx, rows)
		}
		for _, i := range entries[:count] {
			blob := r.contentRow(i).BlobID
			publish(i, staged[blob])
			staged[blob].cleanup()
		}
		cleanupDownloads(staged)
		entries = entries[count:]
	}
}
func (r *Replica) hydrateMoves(ctx context.Context, local map[string]LocalFile) {
	for p, i := range r.placeholderMoved {
		v, ok := local[p]
		if !ok {
			continue
		}
		var e error
		row := r.contentRow(i)
		if time.Now().Before(r.retryAt[row.PathID]) {
			e = ErrBusy
		} else if _, bad := r.quarantine[row.PathID]; bad {
			e = ErrIntegrity
		} else {
			staged := r.stage(ctx, row)
			e = r.publishPlaceholder(ctx, i, v.Local, staged)
			staged.cleanup()
		}
		if e == nil {
			local[p], e = r.inspect(p, v.Local)
		}
		if e != nil {
			// Neither the source nor any missing ancestor may become a delete-only.
			for old := i.Path; old != "."; old = path.Dir(old) {
				r.blockLocal(old)
			}
			r.blockLocal(v.Local)
			delete(local, p)
			r.placeholderRetry(i, e)
		}
	}
}
func (r *Replica) movePlaceholder(row Row, m FileMetadata, old IndexEntry, local string) error {
	if old.hashString() != m.Hash || old.Size != m.Size {
		return ErrBusy
	}
	if r.exactExists(local) && local != old.Local {
		return ErrBusy
	}
	if e := r.safe(local); e != nil {
		return e
	}
	if e := durableMkdirAll(r.root, path.Dir(local)); e != nil {
		return e
	}
	if !r.placeholderAt(old, old.Local) {
		return ErrBusy
	}
	if local != old.Local {
		if e := renameIfAbsent(r.root, old.Local, local); e != nil {
			return e
		}
	}
	if e := r.syncParent(local); e != nil {
		return e
	}
	if e := r.syncParent(old.Local); e != nil {
		return e
	}
	old.Deleted = true
	old.Version = row.Version
	if e := r.save(old); e != nil {
		return e
	}
	pid, _ := PathID(r.key, r.folder, old.Path)
	r.tombstones[pid] = row.Version
	old.Path = m.Path
	old.Local = local
	old.Deleted = false
	old.Blob, _ = parseBlob(row.BlobID)
	old.Mode = m.Mode
	return r.save(old)
}
