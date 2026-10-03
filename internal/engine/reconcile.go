package engine

import (
	"path"
)

// A Full pull replaces pending state with the current snapshot. Missing indexed
// paths get synthetic deletes with base zero, accepted below the horizon.
func (r *Replica) reconcileFull(d *Delta) {
	present := map[string]Row{}
	for _, row := range d.Rows {
		present[row.PathID] = row
	}
	for _, set := range []map[string]Row{r.retryRows, r.ignoredRows, r.quarantine} {
		for pid, row := range set {
			current, ok := present[pid]
			if !ok || current.Version != row.Version {
				delete(set, pid)
			}
		}
	}
	for p, i := range r.index {
		pid, _ := PathID(r.key, r.folder, p)
		if _, ok := present[pid]; ok {
			continue
		}
		if i.Deleted {
			i.Version = 0
			if e := r.save(i); e != nil {
				r.addError(e)
			}
			continue
		}
		d.Rows = append(d.Rows, Row{FolderID: r.folder, PathID: pid, Deleted: true})
	}
}

// Only ignored descendants are disposable. Unignored/tracked children keep the
// directory tombstone pending, including case-only renames arriving in pieces.
func (r *Replica) removeIgnored(dir string, patterns []string) error {
	if e := r.safe(dir); e != nil {
		return e
	}
	f, e := r.root.Open(dir)
	if e != nil {
		return e
	}
	entries, e := f.ReadDir(-1)
	f.Close()
	if e != nil {
		return e
	}
	for _, entry := range entries {
		p := path.Join(dir, entry.Name())
		if !r.ignore(p, entry.IsDir(), patterns) {
			continue
		}
		if e = r.safe(p); e != nil {
			return e
		}
		if entry.IsDir() {
			if e = r.removeIgnored(p, patterns); e != nil {
				return e
			}
		}
		if e = r.root.Remove(p); e != nil {
			return e
		}
		if e = r.syncParent(p); e != nil {
			return e
		}
	}
	return nil
}
