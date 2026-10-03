package engine

import (
	"context"
	"time"
)

// CompactTombstones selects on the read pool and retires bounded path batches.
func (s *Server) CompactTombstones(ctx context.Context) error {
	if s.TombstoneTTL <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cutoff := s.now().Add(-s.TombstoneTTL).UnixNano()
	candidates := map[string][]string{}
	if db, ok := s.Meta.(*SQLiteMetaStore); ok {
		rows, e := db.reads.QueryContext(ctx, `SELECT folder,path FROM files WHERE json_extract(data,'$.Deleted')=1 AND json_extract(data,'$.BlobID')='' AND json_extract(data,'$.DeletedAt')>0 AND json_extract(data,'$.DeletedAt')<?`, cutoff)
		if e != nil {
			return e
		}
		for rows.Next() {
			var folder, pid string
			if e = rows.Scan(&folder, &pid); e != nil {
				rows.Close()
				return e
			}
			candidates[folder] = append(candidates[folder], pid)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
	} else {
		e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{ReadOnly: true}), func(m *Metadata) error {
			for folder, rows := range m.Files {
				for pid, row := range rows {
					if row.Deleted && row.BlobID == "" && row.DeletedAt > 0 && row.DeletedAt < cutoff {
						candidates[folder] = append(candidates[folder], pid)
					}
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	for folder, paths := range candidates {
		for len(paths) > 0 {
			batch := paths[:min(len(paths), maintenanceBatch)]
			paths = paths[len(batch):]
			e := s.Meta.Transaction(maintenanceScope(ctx, folder, batch, []string{}, []string{}), func(m *Metadata) error {
				f, ok := m.Folders[folder]
				if !ok || f.Deleted {
					return nil
				}
				changed := false
				for pid, row := range m.Files[folder] {
					if row.Deleted && row.BlobID == "" && row.DeletedAt > 0 && row.DeletedAt < cutoff {
						delete(m.Files[folder], pid)
						f.Folder.Horizon = max(f.Folder.Horizon, row.Version)
						changed = true
					}
				}
				if changed {
					f.Folder.Version++
					m.Folders[folder] = f
				}
				return nil
			})
			if e != nil {
				return e
			}
			s.notify(folder)
		}
	}
	return nil
}
