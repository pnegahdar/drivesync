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

func (s *Server) read(ctx context.Context, p Principal, id string, fn func(*Metadata) error) error {
	return s.transaction(context.WithValue(ctx, readKey{}, true), p, id, false, fn)
}

func (s *Server) transaction(ctx context.Context, p Principal, id string, create bool, fn func(*Metadata) error) error {
	if !p.valid() {
		return ErrDenied
	}
	txctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	scoped := context.WithValue(txctx, scopeKey{}, Scope{Principal: p, Folder: id, Create: create && id == "", NoFiles: (id == "" && !create) || ctx.Value(noFilesKey{}) == true, ReadOnly: ctx.Value(readKey{}) == true, Paths: func() []string { v, _ := ctx.Value(pathsKey{}).([]string); return v }()})
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
func (s *Server) collect(gcctx context.Context) error {
	gcctx, cancel := context.WithTimeout(gcctx, 5*time.Second)
	defer cancel()
	pending := map[string]Garbage{}
	expired := map[string]bool{}
	emptyDeleted := map[string]bool{}
	ctx := gcctx
	// Survey maintenance work on the read pool. Only folders with actual changes
	// take the writer, so thousands of idle owners cannot stall unrelated writes.
	e := s.Meta.Transaction(context.WithValue(gcctx, scopeKey{}, Scope{GC: true, ReadOnly: true}), func(m *Metadata) error {
		totals := allUsage(m)
		for id, f := range m.Folders {
			if f.Deleted && totals[id].GarbageRows == 0 {
				emptyDeleted[id] = true
			}
		}
		for _, t := range m.Tickets {
			if !t.Expires.After(s.now()) {
				expired[t.FolderID] = true
			}
		}
		for id, g := range m.Garbage {
			if !g.Writing {
				pending[id] = g
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	for folder := range expired {
		e = s.Meta.Transaction(context.WithValue(gcctx, scopeKey{}, Scope{Folder: folder, NoFiles: true}), func(m *Metadata) error {
			s.expire(m)
			for id, g := range m.Garbage {
				if !g.Writing {
					pending[id] = g
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	for folder := range emptyDeleted {
		e = s.Meta.Transaction(context.WithValue(gcctx, scopeKey{}, Scope{Folder: folder, NoFiles: true}), func(m *Metadata) error {
			if f, ok := m.Folders[folder]; ok && f.Deleted && usage(m, folder).GarbageRows == 0 {
				delete(m.Folders, folder)
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	var errs []error
	for id, g := range pending {
		_, e = bounded(ctx, func(c context.Context) (struct{}, error) { return struct{}{}, s.Blobs.Delete(c, g.FolderID, g.BlobID) })
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			errs = append(errs, e)
			continue
		}
		c := context.WithValue(gcctx, scopeKey{}, Scope{Folder: g.FolderID, NoFiles: true})
		e = s.Meta.Transaction(c, func(m *Metadata) error {
			if current, ok := m.Garbage[id]; ok && !current.Writing {
				delete(m.Garbage, id)
			}
			f := m.Folders[g.FolderID]
			if f.Deleted && usage(m, g.FolderID).GarbageRows == 0 {
				delete(m.Folders, g.FolderID)
			}
			return nil
		})
		if e != nil {
			errs = append(errs, e)
		}
	}
	// Deleted rows keep their existing charge until physical deletion succeeds.
	var tombstones []Row
	if db, ok := s.Meta.(*SQLiteMetaStore); ok {
		rows, qe := db.reads.QueryContext(ctx, `SELECT data FROM files WHERE json_extract(data,'$.Deleted')=1 AND json_extract(data,'$.BlobID')!=''`)
		if qe != nil {
			errs = append(errs, qe)
		} else {
			for rows.Next() {
				var data []byte
				var row Row
				if qe = rows.Scan(&data); qe == nil {
					qe = json.Unmarshal(data, &row)
				}
				if qe != nil {
					errs = append(errs, qe)
					break
				}
				tombstones = append(tombstones, row)
			}
			errs = append(errs, rows.Err())
			rows.Close()
		}
	} else {
		qe := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{ReadOnly: true}), func(m *Metadata) error {
			for _, rows := range m.Files {
				for _, row := range rows {
					if row.Deleted && row.BlobID != "" {
						tombstones = append(tombstones, row)
					}
				}
			}
			return nil
		})
		errs = append(errs, qe)
	}
	for _, row := range tombstones {
		_, de := bounded(ctx, func(c context.Context) (struct{}, error) {
			return struct{}{}, s.Blobs.Delete(c, row.FolderID, row.BlobID)
		})
		if de != nil && !errors.Is(de, os.ErrNotExist) {
			errs = append(errs, de)
			continue
		}
		qe := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{Folder: row.FolderID, Paths: []string{row.PathID}}), func(m *Metadata) error {
			current, ok := m.Files[row.FolderID][row.PathID]
			if ok && current.Deleted && current.BlobID == row.BlobID {
				current.BlobID = ""
				current.SealedSize = 0
				current.Metadata = nil
				m.Files[row.FolderID][row.PathID] = current
			}
			return nil
		})
		errs = append(errs, qe)
	}
	return errors.Join(errs...)
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
	ctx = context.WithValue(ctx, scopeKey{}, Scope{Principal: p, Folder: id, NoFiles: true})
	return s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		t, ok := m.Tickets[tid]
		if !ok || t.Principal != p || t.FolderID != id {
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
	c := context.WithValue(ctx, scopeKey{}, Scope{GC: true})
	return s.Meta.Transaction(c, func(m *Metadata) error {
		for tid, t := range m.Tickets {
			if t.Writing {
				s.retireTicket(m, t)
				delete(m.Tickets, tid)
			}
		}
		for key, g := range m.Garbage {
			g.Writing = false
			m.Garbage[key] = g
		}
		return nil
	})
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
