package drivesync

import (
	"context"
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
func (s *Server) transaction(ctx context.Context, p Principal, id string, owner bool, fn func(*Metadata) error) error {
	txctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	scoped := context.WithValue(txctx, scopeKey{}, transactionScope{Principal: p, Folder: id, Owner: owner})
	account := p
	e := s.Meta.Transaction(scoped, func(m *Metadata) error {
		if f, ok := m.Folders[id]; ok {
			account = f.Folder.Owner
		}
		return fn(m)
	})
	if e == nil {
		_ = s.collectScoped(ctx, account, "")
	}
	return e
}
func (s *Server) collectScoped(ctx context.Context, p Principal, id string) error {
	c := context.WithValue(ctx, scopeKey{}, transactionScope{Principal: p, Folder: id, Owner: true, NoFiles: true})
	return s.collect(c)
}
func (s *Server) retireTicket(m *Metadata, t Ticket) {
	f := m.Folders[t.FolderID]
	size := sat(t.SealedSize, RowCost)
	if t.Writing && !t.Uploaded {
		size = sat(size, 1)
	}
	m.Garbage[t.FolderID+"/"+t.BlobID] = Garbage{t.FolderID, t.BlobID, f.Folder.Owner, size}
}
func (s *Server) retireRow(m *Metadata, r Row) {
	if r.BlobID != "" {
		m.Garbage[r.FolderID+"/"+r.BlobID] = Garbage{r.FolderID, r.BlobID, m.Folders[r.FolderID].Folder.Owner, sat(r.SealedSize, RowCost)}
	}
}

// CollectGarbage expires tickets and durably queues and collects obsolete blobs.
// Run periodically even when no clients are active. Failed deletions remain charged.
func (s *Server) CollectGarbage(ctx context.Context) error {
	gcctx := context.WithValue(ctx, scopeKey{}, transactionScope{GC: true})
	return s.collect(gcctx)
}
func (s *Server) collect(gcctx context.Context) error {
	gcctx, cancel := context.WithTimeout(gcctx, 5*time.Second)
	defer cancel()
	var pending map[string]Garbage
	ctx := gcctx
	e := s.Meta.Transaction(gcctx, func(m *Metadata) error {
		s.expire(m)
		pending = map[string]Garbage{}
		for id, g := range m.Garbage {
			pending[id] = g
		}
		return nil
	})
	if e != nil {
		return e
	}
	var errs []error
	for id, g := range pending {
		_, e = bounded(ctx, func(c context.Context) (struct{}, error) { return struct{}{}, s.Blobs.Delete(c, g.FolderID, g.BlobID) })
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			errs = append(errs, e)
			continue
		}
		e = s.Meta.Transaction(gcctx, func(m *Metadata) error { delete(m.Garbage, id); return nil })
		if e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}
func (s *Server) renew(ctx context.Context, p Principal, id, tid string) error {
	ctx = context.WithValue(ctx, scopeKey{}, transactionScope{Principal: p, Folder: id, NoFiles: true})
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
		return nil
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

func (s *Server) cleanupAuthorized(ctx context.Context, p Principal, id string, mut []Mutation) error {
	c := context.WithValue(ctx, scopeKey{}, transactionScope{Principal: p, Folder: id, NoFiles: true})
	e := s.Meta.Transaction(c, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		for _, v := range mut {
			if t, ok := m.Tickets[v.TicketID]; ok && t.Principal == p && t.FolderID == id && !t.Expires.After(s.now()) {
				return ErrExpired
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	_ = s.collectScoped(ctx, p, id)
	return nil
}
