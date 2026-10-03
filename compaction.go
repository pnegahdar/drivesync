package drivesync

import (
	"context"
)

// CompactTombstones frees deleted rows past the retention horizon. Replicas
// behind Horizon must reconcile the full current set before publishing writes.
// A zero TTL disables compaction. CollectGarbage calls this in maintenance only.
func (s *Server) CompactTombstones(ctx context.Context) error {
	if s.TombstoneTTL <= 0 {
		return nil
	}
	cutoff := s.now().Add(-s.TombstoneTTL)
	var folders []string
	if m, ok := s.Meta.(*SQLiteMetaStore); ok {
		rows, e := m.db.QueryContext(ctx, `SELECT DISTINCT folder FROM files WHERE json_extract(data,'$.Deleted')=1 AND json_extract(data,'$.DeletedAt')<?`, cutoff.UnixNano())
		if e != nil {
			return e
		}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return e
			}
			folders = append(folders, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
	} else {
		e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{GC: true}), func(m *Metadata) error {
			for id := range m.Folders {
				folders = append(folders, id)
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	for _, id := range folders {
		e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{Folder: id}), func(m *Metadata) error {
			f, ok := m.Folders[id]
			if !ok || f.Deleted {
				return nil
			}
			changed := false
			for pid, row := range m.Files[id] {
				if row.Deleted && row.DeletedAt > 0 && row.DeletedAt < cutoff.UnixNano() {
					delete(m.Files[id], pid)
					f.Folder.Horizon = max(f.Folder.Horizon, row.Version)
					changed = true
				}
			}
			if changed {
				f.Folder.Version++
				m.Folders[id] = f
			}
			return nil
		})
		if e != nil {
			return e
		}
		s.notify(id)
	}
	return nil
}
