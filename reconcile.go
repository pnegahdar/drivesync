package drivesync

import (
	"context"
	"path"
)

// Reconcile before publishing an old replica's writes. Require a stable full
// view so pagination concurrent with updates cannot invent a missing remote row.
func (r *Replica) reconcile(ctx context.Context, local map[string]localFile, stale bool) error {
	var d Delta
	for attempt := 0; ; attempt++ {
		var e error
		d, e = r.client.Changes(ctx, r.folder, 0)
		if e != nil {
			return e
		}
		f, e := r.client.GetFolder(ctx, r.folder)
		if e != nil {
			return e
		}
		if f.Version == d.Version {
			break
		}
		if attempt == 2 {
			return ErrBusy
		}
	}
	present := map[string]bool{}
	for _, row := range d.Rows {
		present[row.PathID] = true
		if row.Deleted {
			continue
		}
		m, e := OpenMetadata(r.key, r.folder, row)
		if e != nil {
			continue
		}
		v, ok := local[m.Path]
		if !ok || v.Directory != m.Directory || v.Hash != m.Hash || v.Size != m.Size || r.ignore(m.Path, m.Directory, r.patterns()) {
			continue
		}
		if e = r.safe(v.Local); e != nil {
			continue
		}
		if v.Directory {
			e = durableMkdirAll(r.root, v.Local)
		} else {
			e = durableMkdirAll(r.root, path.Dir(v.Local))
			if e == nil {
				f, oe := openLocal(r.root, v.Local)
				e = oe
				if e == nil {
					e = f.Sync()
					f.Close()
				}
			}
		}
		if e != nil {
			return e
		}
		if e = r.syncParent(v.Local); e != nil {
			return e
		}
		if e = r.save(indexEntry{Path: m.Path, Local: v.Local, Hash: v.Hash, Version: row.Version, Directory: v.Directory, Mode: v.Mode}); e != nil {
			return e
		}
	}
	if !stale {
		return nil
	}
	for p, i := range r.index {
		pid, e := PathID(r.key, r.folder, p)
		if e != nil {
			return e
		}
		if present[pid] {
			continue
		}
		delete(r.retryRows, pid)
		delete(r.ignoredRows, pid)
		delete(r.quarantine, pid)
		if !i.Deleted {
			if r.localBlocked(p) {
				r.retryRows[pid] = Row{FolderID: r.folder, PathID: pid, Version: 0, Deleted: true}
				continue
			}
			if e = r.apply(ctx, Row{FolderID: r.folder, PathID: pid, Version: 0, Deleted: true}); e != nil {
				return e
			}
		}
		i = r.index[p]
		i.Deleted = true
		i.Version = 0
		i.Hash = ""
		if e = r.save(i); e != nil {
			return e
		}
	}
	return nil
}
