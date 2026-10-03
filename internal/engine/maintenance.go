package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

// External hooks are bounded, even if they ignore cancellation. The semaphore
// limits leaked workers from a broken embedding hook.
var hookWorkers = make(chan struct{}, 32)

func bounded[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case hookWorkers <- struct{}{}:
	case <-c.Done():
		return zero, c.Err()
	}
	type result struct {
		v T
		e error
	}
	ch := make(chan result, 1)
	go func() { defer func() { <-hookWorkers }(); v, e := fn(c); ch <- result{v, e} }()
	select {
	case r := <-ch:
		return r.v, r.e
	case <-c.Done():
		return zero, c.Err()
	}
}

type noFilesKey struct{}
type pathsKey struct{}
type readKey struct{}
type ownerKey struct{}
type ticketIDsKey struct{}
type ticketPathsKey struct{}
type garbageIDsKey struct{}

func (s *Server) read(ctx context.Context, p Principal, id string, fn func(*Metadata) error) error {
	return s.transaction(context.WithValue(ctx, readKey{}, true), p, id, false, fn)
}

func (s *Server) transaction(ctx context.Context, p Principal, id string, create bool, fn func(*Metadata) error) error {
	if !p.valid() {
		return ErrDenied
	}
	txctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	scoped := context.WithValue(txctx, scopeKey{}, Scope{Principal: p, Folder: id, Create: create && id == "", NoFiles: (id == "" && !create) || ctx.Value(noFilesKey{}) == true, ReadOnly: ctx.Value(readKey{}) == true, Write: ctx.Value(readKey{}) != true, Owner: ctx.Value(ownerKey{}) == true, TicketPaths: func() []string { v, _ := ctx.Value(ticketPathsKey{}).([]string); return v }(), Tickets: func() []string { v, _ := ctx.Value(ticketIDsKey{}).([]string); return v }(), Garbage: func() []string { v, _ := ctx.Value(garbageIDsKey{}).([]string); return v }(), Paths: func() []string { v, _ := ctx.Value(pathsKey{}).([]string); return v }()})
	return s.Meta.Transaction(scoped, func(m *Metadata) error { m.ctx = txctx; return fn(m) })
}
func (s *Server) retireTicket(m *Metadata, t Ticket) {
	if !t.Uploaded && !t.Writing {
		return
	}
	f := m.Folders[t.FolderID]
	m.Garbage[t.FolderID+"/"+t.BlobID] = Garbage{FolderID: t.FolderID, BlobID: t.BlobID, Owner: f.Folder.Owner, Size: sat(t.SealedSize, RowCost), Writing: t.Writing}
}
func (s *Server) retireRow(m *Metadata, r Row) {
	if r.BlobID != "" {
		m.Garbage[r.FolderID+"/"+r.BlobID] = Garbage{FolderID: r.FolderID, BlobID: r.BlobID, Owner: m.Folders[r.FolderID].Folder.Owner, Size: sat(r.SealedSize, RowCost)}
	}
}

// CollectGarbage expires tickets and durably queues and collects obsolete blobs.
// Run periodically even when no clients are active. Failed deletions remain charged.
func (s *Server) CollectGarbage(ctx context.Context) error {
	gcctx := context.WithValue(ctx, scopeKey{}, Scope{GC: true})
	return errors.Join(s.collect(gcctx), s.CompactTombstones(ctx))
}

// RunGC runs maintenance in a background worker owned by the embedder. Start
// it once per authority and cancel its context before closing stores.
func (s *Server) RunGC(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = s.CollectGarbage(ctx)
		}
	}
}

func (s *Server) surveyCollect(ctx context.Context) (map[string]Garbage, map[string][]Ticket, map[string]bool, error) {
	if db, ok := s.Meta.(*SQLiteMetaStore); ok {
		return s.surveyCollectSQL(ctx, db)
	}
	pending := map[string]Garbage{}
	expired := map[string][]Ticket{}
	deleted := map[string]bool{}
	e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{GC: true, ReadOnly: true}), func(m *Metadata) error {
		for id, f := range m.Folders {
			if f.Deleted {
				deleted[id] = true
			}
		}
		for _, t := range m.Tickets {
			if deleted[t.FolderID] || t.AuthEpoch != m.Folders[t.FolderID].AuthEpoch || !t.Expires.After(s.now()) || (t.Writing && !s.wakes.live(t.FolderID+"/"+t.BlobID)) {
				expired[t.FolderID] = append(expired[t.FolderID], t)
			}
		}
		for key, g := range m.Garbage {
			if !g.Writing || !s.wakes.live(key) {
				pending[key] = g
			}
		}
		return nil
	})
	return pending, expired, deleted, e
}

func (s *Server) surveyCollectSQL(ctx context.Context, db *SQLiteMetaStore) (map[string]Garbage, map[string][]Ticket, map[string]bool, error) {
	folders := map[string]FolderRecord{}
	rows, e := db.reads.QueryContext(ctx, "SELECT id, data FROM folders")
	if e != nil {
		return nil, nil, nil, e
	}
	for rows.Next() {
		var id string
		var data []byte
		var f FolderRecord
		if e = rows.Scan(&id, &data); e == nil {
			e = json.Unmarshal(data, &f)
		}
		if e != nil {
			rows.Close()
			return nil, nil, nil, e
		}
		folders[id] = f
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return nil, nil, nil, e
	}
	rows.Close()
	deleted := map[string]bool{}
	for id, f := range folders {
		if f.Deleted {
			deleted[id] = true
		}
	}
	expired := map[string][]Ticket{}
	rows, e = db.reads.QueryContext(ctx, "SELECT data FROM tickets")
	if e != nil {
		return nil, nil, nil, e
	}
	for rows.Next() {
		var data []byte
		var t Ticket
		if e = rows.Scan(&data); e == nil {
			e = json.Unmarshal(data, &t)
		}
		if e != nil {
			rows.Close()
			return nil, nil, nil, e
		}
		if deleted[t.FolderID] || t.AuthEpoch != folders[t.FolderID].AuthEpoch || !t.Expires.After(s.now()) || (t.Writing && !s.wakes.live(t.FolderID+"/"+t.BlobID)) {
			expired[t.FolderID] = append(expired[t.FolderID], t)
		}
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return nil, nil, nil, e
	}
	rows.Close()
	// Garbage is paged by primary key during deletion. Unmarshalling the whole
	// backlog here used up the collect budget before a row was removed.
	return map[string]Garbage{}, expired, deleted, nil
}

const maintenanceBatch = 128

// Each garbage batch is its own transaction: the writer is released between
// batches, and the delete is one statement rather than one round trip per blob.
// SQLite pages a larger batch so a backlog costs few commits on a slow disk.
const garbageBatch = 64
const garbagePage = 512

func maintenanceScope(ctx context.Context, folder string, paths, tickets, garbage []string) context.Context {
	return context.WithValue(ctx, scopeKey{}, Scope{Folder: folder, Paths: paths, Tickets: tickets, Garbage: garbage})
}

func (s *Server) collect(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pending, expired, deleted, e := s.surveyCollect(ctx)
	if e != nil {
		return e
	}
	for folder, tickets := range expired {
		for len(tickets) > 0 {
			batch := tickets[:min(len(tickets), maintenanceBatch)]
			tickets = tickets[len(batch):]
			ids, blobs := []string{}, []string{}
			for _, t := range batch {
				ids = append(ids, t.ID)
				blobs = append(blobs, folder+"/"+t.BlobID)
			}
			e = s.Meta.Transaction(maintenanceScope(ctx, folder, []string{}, ids, blobs), func(m *Metadata) error {
				for tid, t := range m.Tickets {
					// A retry may have renewed it since the survey. Completed publications
					// remain charged until this durable retirement succeeds.
					live := s.wakes.live(folder + "/" + t.BlobID)
					if !m.Folders[folder].Deleted && t.AuthEpoch == m.Folders[folder].AuthEpoch && t.Expires.After(s.now()) && (!t.Writing || live) {
						continue
					}
					s.retireTicket(m, t)
					delete(m.Tickets, tid)
					key := folder + "/" + t.BlobID
					if g, ok := m.Garbage[key]; ok && !live {
						g.Writing = false
						m.Garbage[key] = g
						pending[key] = g
					}
				}
				return nil
			})
			if e != nil {
				return e
			}
		}
	}
	// Folder deletion is acknowledged before any row scan. Retirement work is
	// bounded per pass and transfers the complete row charge to cleanup.
	for folder := range deleted {
		rows, e := s.maintenanceRows(ctx, folder, false, 512)
		if e != nil {
			return e
		}
		for len(rows) > 0 {
			batch := rows[:min(len(rows), maintenanceBatch)]
			rows = rows[len(batch):]
			paths, blobs := []string{}, []string{}
			for _, row := range batch {
				paths = append(paths, row.PathID)
				if row.BlobID != "" {
					blobs = append(blobs, folder+"/"+row.BlobID)
				}
			}
			e = s.Meta.Transaction(maintenanceScope(ctx, folder, paths, []string{}, blobs), func(m *Metadata) error {
				if !m.Folders[folder].Deleted {
					return nil
				}
				for pid, row := range m.Files[folder] {
					if row.BlobID != "" {
						key := folder + "/" + row.BlobID
						g := Garbage{FolderID: folder, BlobID: row.BlobID, Owner: m.Folders[folder].Folder.Owner, Size: rowBytes(row)}
						m.Garbage[key] = g
						pending[key] = g
					}
					delete(m.Files[folder], pid)
				}
				return nil
			})
			if e != nil {
				return e
			}
		}
	}
	var errs []error
	if db, ok := s.Meta.(*SQLiteMetaStore); ok {
		if e = s.collectGarbagePages(ctx, db, &errs); e != nil {
			return e
		}
	} else if e = s.collectLoadedGarbage(ctx, pending, &errs); e != nil {
		return e
	}
	// Tombstones retain their existing blob charge until physical deletion.
	rows, e := s.maintenanceRows(ctx, "", true, 0)
	if e != nil {
		return errors.Join(append(errs, e)...)
	}
	groupsRows := map[string][]Row{}
	for _, row := range rows {
		groupsRows[row.FolderID] = append(groupsRows[row.FolderID], row)
	}
	for folder, rows := range groupsRows {
		for len(rows) > 0 {
			batch := rows[:min(len(rows), maintenanceBatch)]
			rows = rows[len(batch):]
			collected := map[string]Row{}
			paths := []string{}
			for _, row := range batch {
				_, de := bounded(ctx, func(c context.Context) (struct{}, error) { return struct{}{}, s.Blobs.Delete(c, folder, row.BlobID) })
				if de != nil && !errors.Is(de, os.ErrNotExist) {
					errs = append(errs, de)
					continue
				}
				collected[row.PathID] = row
				paths = append(paths, row.PathID)
			}
			if len(paths) == 0 {
				continue
			}
			e = s.Meta.Transaction(maintenanceScope(ctx, folder, paths, []string{}, []string{}), func(m *Metadata) error {
				for pid, row := range collected {
					current, ok := m.Files[folder][pid]
					if ok && current.Deleted && current.BlobID == row.BlobID {
						current.BlobID = ""
						current.SealedSize = 0
						// Keep authenticated tombstone metadata until compaction.
						m.Files[folder][pid] = current
					}
				}
				return nil
			})
			if e != nil {
				return errors.Join(append(errs, e)...)
			}
		}
	}
	for folder := range deleted {
		e = s.Meta.Transaction(maintenanceScope(ctx, folder, []string{}, []string{}, []string{}), func(m *Metadata) error {
			f, ok := m.Folders[folder]
			if !ok || !f.Deleted {
				return nil
			}
			u := usage(m, folder)
			if u.Rows == 0 && u.GarbageRows == 0 && u.ReservedRows == 0 {
				delete(m.Folders, folder)
			}
			return nil
		})
		if e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

// collectGarbagePages walks the garbage primary key. Each page deletes blobs
// outside the writer, then one transaction drops those rows and their usage.
func (s *Server) collectGarbagePages(ctx context.Context, db *SQLiteMetaStore, errs *[]error) error {
	var after string
	for {
		if e := ctx.Err(); e != nil {
			return errors.Join(append(*errs, e)...)
		}
		rows, e := db.reads.QueryContext(ctx, `SELECT id, folder, writing FROM garbage WHERE id>? ORDER BY id LIMIT ?`, after, garbagePage)
		if e != nil {
			return errors.Join(append(*errs, e)...)
		}
		type item struct {
			id, folder string
			writing    int64
		}
		page := make([]item, 0, garbagePage)
		for rows.Next() {
			var it item
			if e = rows.Scan(&it.id, &it.folder, &it.writing); e != nil {
				rows.Close()
				return errors.Join(append(*errs, e)...)
			}
			page = append(page, it)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return errors.Join(append(*errs, e)...)
		}
		if len(page) == 0 {
			return nil
		}
		after = page[len(page)-1].id
		byFolder := map[string][]string{}
		var stopped error
		for _, it := range page {
			if e = ctx.Err(); e != nil {
				*errs = append(*errs, e)
				stopped = e
				break
			}
			if it.writing != 0 && s.wakes.live(it.id) {
				continue
			}
			blob := ""
			if len(it.id) == 65 && it.id[32] == '/' && it.id[:32] == it.folder {
				blob = it.id[33:]
			}
			// A row stays charged until its blob delete succeeds. Missing blobs are success.
			if !validID(it.folder) || !validID(blob) {
				*errs = append(*errs, ErrInvalid)
				continue
			}
			if de := s.Blobs.Delete(ctx, it.folder, blob); de != nil && !errors.Is(de, os.ErrNotExist) {
				*errs = append(*errs, de)
				continue
			}
			byFolder[it.folder] = append(byFolder[it.folder], it.id)
		}
		for folder, ids := range byFolder {
			if e = db.deleteGarbageBatch(ctx, folder, ids, s.wakes.live); e != nil {
				return errors.Join(append(*errs, e)...)
			}
		}
		if stopped != nil {
			return errors.Join(*errs...)
		}
	}
}

func (s *Server) collectLoadedGarbage(ctx context.Context, pending map[string]Garbage, errs *[]error) error {
	groups := map[string][]Garbage{}
	for _, g := range pending {
		groups[g.FolderID] = append(groups[g.FolderID], g)
	}
	for folder, garbage := range groups {
		for len(garbage) > 0 {
			if e := ctx.Err(); e != nil {
				return errors.Join(append(*errs, e)...)
			}
			batch := garbage[:min(len(garbage), garbageBatch)]
			garbage = garbage[len(batch):]
			collected := map[string]bool{}
			ids := []string{}
			for _, g := range batch {
				if e := ctx.Err(); e != nil {
					*errs = append(*errs, e)
					break
				}
				key := folder + "/" + g.BlobID
				if s.wakes.live(key) {
					continue
				}
				if de := s.Blobs.Delete(ctx, folder, g.BlobID); de != nil && !errors.Is(de, os.ErrNotExist) {
					*errs = append(*errs, de)
					continue
				}
				collected[key] = true
				ids = append(ids, key)
			}
			if len(ids) == 0 {
				continue
			}
			e := s.Meta.Transaction(maintenanceScope(ctx, folder, []string{}, []string{}, ids), func(m *Metadata) error {
				for key := range collected {
					if !s.wakes.live(key) {
						delete(m.Garbage, key)
					}
				}
				return nil
			})
			if e != nil {
				return errors.Join(append(*errs, e)...)
			}
		}
	}
	return nil
}

// Selection stays on WAL readers; no unrelated rows enter a write transaction.
func (s *Server) maintenanceRows(ctx context.Context, folder string, tombstones bool, count int) ([]Row, error) {
	out := []Row{}
	if db, ok := s.Meta.(*SQLiteMetaStore); ok {
		query := "SELECT data FROM files WHERE 1=1"
		args := []any{}
		if folder != "" {
			query += " AND folder=?"
			args = append(args, folder)
		}
		if tombstones {
			query += ` AND json_extract(data,'$.Deleted')=1 AND json_extract(data,'$.BlobID')!=''`
		}
		if count > 0 {
			query += " LIMIT ?"
			args = append(args, count)
		}
		rows, e := db.reads.QueryContext(ctx, query, args...)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		for rows.Next() {
			var b []byte
			var row Row
			if e = rows.Scan(&b); e == nil {
				e = json.Unmarshal(b, &row)
			}
			if e != nil {
				return nil, e
			}
			out = append(out, row)
		}
		return out, rows.Err()
	}
	e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{ReadOnly: true, Folder: folder}), func(m *Metadata) error {
		for _, rows := range m.Files {
			for _, r := range rows {
				if (!tombstones || r.Deleted && r.BlobID != "") && (count == 0 || len(out) < count) {
					out = append(out, r)
				}
			}
		}
		return nil
	})
	return out, e
}
func (s *Server) renew(ctx context.Context, p Principal, id, tid string) error {
	ctx = context.WithValue(ctx, noFilesKey{}, true)
	t, e := s.ticket(ctx, p, id, tid)
	if e != nil {
		return e
	}
	ttl := s.ReservationTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if t.Expires.Sub(s.now()) >= ttl/2 {
		return nil
	}
	ctx = context.WithValue(ctx, scopeKey{}, Scope{Principal: p, Folder: id, NoFiles: true, Write: true, Tickets: []string{tid}, Garbage: []string{}})
	return s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		t, ok := m.Tickets[tid]
		if !ok || t.Principal != p || t.FolderID != id || t.AuthEpoch != m.Folders[id].AuthEpoch {
			return ErrDenied
		}
		if !t.Expires.After(s.now()) {
			return ErrExpired
		}
		ttl := s.ReservationTTL
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		if t.Expires.Sub(s.now()) >= ttl/2 {
			return nil
		}
		t.Expires = s.now().Add(ttl)
		m.Tickets[tid] = t
		return nil
	})
}

type renewingReader struct {
	r     io.Reader
	renew func() error
}

func (r *renewingReader) Read(p []byte) (int, error) {
	if e := r.renew(); e != nil {
		return 0, e
	}
	n, e := r.r.Read(p)
	if n > 0 {
		if re := r.renew(); re != nil {
			return 0, re
		}
	}
	return n, e
}

// Sync each new component's parent before returning. All operations stay rooted.
func durableMkdirAll(root *os.Root, p string) error {
	if p == "." {
		return nil
	}
	parts := strings.Split(p, "/")
	for i := range parts {
		component := strings.Join(parts[:i+1], "/")
		e := root.Mkdir(component, 0700)
		if errors.Is(e, os.ErrExist) {
			info, se := root.Lstat(component)
			if se != nil {
				return se
			}
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return ErrInvalid
			}
			e = nil
		}
		if e != nil {
			return e
		}
		d, e := root.Open(path.Dir(component))
		if e != nil {
			return e
		}
		e = syncDirectoryFile(d)
		d.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// Attachment and blob roots are trusted embedder paths; make their creation
// durable too, before os.Root confines all peer-derived operations beneath them.
func durableDirectory(dir string) error {
	if info, e := os.Stat(dir); e == nil {
		if !info.IsDir() {
			return ErrInvalid
		}
		parent := path.Dir(dir)
		d, e := os.Open(parent)
		if e != nil {
			return e
		}
		defer d.Close()
		return syncDirectoryFile(d)
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	parent := path.Dir(dir)
	if parent == dir {
		return ErrInvalid
	}
	if e := durableDirectory(parent); e != nil {
		return e
	}
	if e := os.Mkdir(dir, 0700); e != nil && !errors.Is(e, os.ErrExist) {
		return e
	}
	d, e := os.Open(parent)
	if e != nil {
		return e
	}
	defer d.Close()
	return syncDirectoryFile(d)
}

var syncDirectoryFile = func(f *os.File) error { return f.Sync() }

// RecoverUploads retires interrupted publications after a process restart.
// Call only after all writers using these stores have stopped. A generic remote
// BlobStore cannot prove that another process's Put has stopped publishing.
func (s *Server) RecoverUploads(ctx context.Context) error {
	// The ordinary collector also retires publications with no live Put. Use
	// the same bounded transitions during recovery instead of a global write.
	return s.collect(ctx)
}

// Run recovers interrupted publications, then performs background collection
// and compaction. Only one authority process may publish to these stores.
func (s *Server) Run(ctx context.Context, interval time.Duration) error {
	s.wakes.mu.Lock()
	if s.wakes.running {
		s.wakes.mu.Unlock()
		return ErrBusy
	}
	s.wakes.running = true
	s.wakes.mu.Unlock()
	defer func() { s.wakes.mu.Lock(); s.wakes.running = false; s.wakes.mu.Unlock() }()
	// Recovery must not race an active publication in this process. The wait
	// honors cancellation and never queries SQLite while a stream is flowing.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !s.wakes.publication.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	e := s.RecoverUploads(ctx)
	s.wakes.publication.Unlock()
	if e != nil {
		return e
	}
	return s.RunGC(ctx, interval)
}
