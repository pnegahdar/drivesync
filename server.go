package drivesync

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

type Server struct {
	Meta           MetaStore
	Blobs          BlobStore
	Quotas         QuotaPolicy
	ReservationTTL time.Duration
	Now            func() time.Time
	mu             sync.Mutex
	signal         chan struct{}
}

func NewServer(meta MetaStore, blobs BlobStore) *Server {
	return &Server{Meta: meta, Blobs: blobs, ReservationTTL: 5 * time.Minute, Now: time.Now, signal: make(chan struct{})}
}
func principalKey(p Principal) string { b, _ := json.Marshal(p); return string(b) }
func access(m *Metadata, p Principal, id string, write, owner bool) (FolderRecord, error) {
	f, ok := m.Folders[id]
	if !p.valid() || !ok {
		return f, ErrDenied
	}
	role := f.Grants[principalKey(p)]
	if p == f.Folder.Owner {
		role = Owner
	}
	if role != Owner && role != Writer && role != Reader {
		return f, ErrDenied
	}
	if owner && role != Owner || write && role == Reader {
		return f, ErrDenied
	}
	return f, nil
}
func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}
func (s *Server) notify() {
	s.mu.Lock()
	close(s.signal)
	s.signal = make(chan struct{})
	s.mu.Unlock()
}
func (s *Server) expire(m *Metadata) {
	for id, t := range m.Tickets {
		if !t.Expires.After(s.now()) {
			delete(m.Tickets, id)
		}
	}
}
func (s *Server) quota(ctx context.Context, p Principal) (Quota, error) {
	if s.Quotas == nil {
		return Quota{}, nil
	}
	q, e := s.Quotas.Quota(ctx, p)
	if e == nil && (q.MaxTotalBytes < 0 || q.MaxFolders < 0 || q.MaxFileBytes < 0) {
		e = ErrInvalid
	}
	return q, e
}
func usage(m *Metadata, id string) Usage {
	var u Usage
	for _, r := range m.Files[id] {
		if !r.Deleted {
			u.Bytes += r.SealedSize
			u.Files++
		}
	}
	for _, t := range m.Tickets {
		if t.FolderID == id {
			u.Reserved += t.ReservedBytes
			u.ReservedFiles += t.ReservedFiles
		}
	}
	return u
}
func limit(name string, max, requested int64) error {
	if max > 0 && requested > max {
		return &LimitError{name, max, requested}
	}
	return nil
}
func (s *Server) folder(ctx context.Context, m *Metadata, p Principal, f FolderRecord) (Folder, error) {
	v := f.Folder
	q, e := s.quota(ctx, v.Owner)
	if e != nil {
		return v, e
	}
	v.Limits.MaxFileBytes = minimum(v.Limits.MaxFileBytes, q.MaxFileBytes)
	v.Limits.MaxTotalBytes = minimum(v.Limits.MaxTotalBytes, q.MaxTotalBytes)
	v.Usage = usage(m, v.ID)
	v.Role = f.Grants[principalKey(p)]
	if v.Owner == p {
		v.Role = Owner
	}
	return v, nil
}
func (s *Server) CreateFolder(ctx context.Context, p Principal, spec FolderSpec) (Folder, error) {
	var f Folder
	if !p.valid() {
		return f, ErrDenied
	}
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		if spec.Name == "" || !utf8.ValidString(spec.Name) || !utf8.ValidString(spec.Description) || len(spec.Name) > 255 || len(spec.Description) > 4096 || len(spec.KeyCheck) != 32 || !validLimits(spec.Limits) {
			return ErrInvalid
		}
		q, e := s.quota(ctx, p)
		if e != nil {
			return e
		}
		var count int64
		for _, v := range m.Folders {
			if v.Folder.Owner == p {
				count++
				if v.Folder.Name == spec.Name {
					return ErrConflict
				}
			}
		}
		if e = limit("owner folders", q.MaxFolders, count+1); e != nil {
			return e
		}
		f = Folder{ID: randomID(), Owner: p, Name: spec.Name, Description: spec.Description, Limits: spec.Limits, KeyCheck: append([]byte(nil), spec.KeyCheck...)}
		m.Folders[f.ID] = FolderRecord{f, map[string]Role{}}
		m.Files[f.ID] = map[string]Row{}
		return nil
	})
	return f, e
}
func (s *Server) Grant(ctx context.Context, p Principal, id string, grantee Principal, role Role) error {
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, false, true)
		if e != nil {
			return e
		}
		if !grantee.valid() || (role != Reader && role != Writer && role != Owner) || grantee == f.Folder.Owner {
			return ErrInvalid
		}
		f.Grants[principalKey(grantee)] = role
		if role == Reader {
			for tid, t := range m.Tickets {
				if t.FolderID == id && t.Principal == grantee {
					delete(m.Tickets, tid)
				}
			}
		}
		m.Folders[id] = f
		return nil
	})
	if e == nil {
		s.notify()
	}
	return e
}
func (s *Server) Revoke(ctx context.Context, p Principal, id string, grantee Principal) error {
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, false, true)
		if e != nil {
			return e
		}
		if !grantee.valid() || grantee == f.Folder.Owner {
			return ErrInvalid
		}
		delete(f.Grants, principalKey(grantee))
		m.Folders[id] = f
		for tid, t := range m.Tickets {
			if t.FolderID == id && t.Principal == grantee {
				delete(m.Tickets, tid)
			}
		}
		return nil
	})
	if e == nil {
		s.notify()
	}
	return e
}
func (s *Server) DeleteFolder(ctx context.Context, p Principal, id string) error {
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, false, true); e != nil {
			return e
		}
		delete(m.Folders, id)
		delete(m.Files, id)
		for tid, t := range m.Tickets {
			if t.FolderID == id {
				delete(m.Tickets, tid)
			}
		}
		return nil
	})
	if e == nil {
		s.notify()
	}
	return e
}
func (s *Server) SetLimits(ctx context.Context, p Principal, id string, l Limits) error {
	return s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, false, true)
		if e != nil {
			return e
		}
		if !validLimits(l) {
			return ErrInvalid
		}
		f.Folder.Limits = l
		m.Folders[id] = f
		return nil
	})
}
func (s *Server) ListFolders(ctx context.Context, p Principal) ([]Folder, error) {
	out := []Folder{}
	if !p.valid() {
		return nil, ErrDenied
	}
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		s.expire(m)
		for id, f := range m.Folders {
			if _, e := access(m, p, id, false, false); e == nil {
				v, e := s.folder(ctx, m, p, f)
				if e != nil {
					return e
				}
				out = append(out, v)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, e
}
func (s *Server) GetFolder(ctx context.Context, p Principal, id string) (Folder, error) {
	var out Folder
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, false, false)
		if e != nil {
			return e
		}
		s.expire(m)
		out, e = s.folder(ctx, m, p, f)
		return e
	})
	return out, e
}
func (s *Server) Reserve(ctx context.Context, p Principal, id string, r UploadRequest) (Ticket, error) {
	var t Ticket
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, true, false)
		if e != nil {
			return e
		}
		s.expire(m)
		if !validPathID(r.PathID) || r.SealedSize < 0 || r.SealedSize > math.MaxInt64/4 {
			return ErrInvalid
		}
		old := m.Files[id][r.PathID]
		if old.Version != r.BaseVersion {
			return ErrConflict
		}
		for _, v := range m.Tickets {
			if v.FolderID == id && v.PathID == r.PathID {
				return ErrConflict
			}
		}
		q, e := s.quota(ctx, f.Folder.Owner)
		if e != nil {
			return e
		}
		maxFile := minimum(f.Folder.Limits.MaxFileBytes, q.MaxFileBytes)
		if e = limit("file bytes", maxFile, r.SealedSize); e != nil {
			return e
		}
		delta := r.SealedSize
		files := int64(1)
		if old.Version > 0 && !old.Deleted {
			delta -= old.SealedSize
			files = 0
		}
		if delta < 0 {
			delta = 0
		}
		u := usage(m, id)
		if u.Bytes > math.MaxInt64-u.Reserved-delta {
			return ErrInvalid
		}
		if e = limit("folder bytes", f.Folder.Limits.MaxTotalBytes, u.Bytes+u.Reserved+delta); e != nil {
			return e
		}
		if e = limit("folder files", f.Folder.Limits.MaxFiles, u.Files+u.ReservedFiles+files); e != nil {
			return e
		}
		var ownerBytes int64
		for fid, v := range m.Folders {
			if v.Folder.Owner == f.Folder.Owner {
				a := usage(m, fid)
				if ownerBytes > math.MaxInt64-a.Bytes-a.Reserved {
					return ErrInvalid
				}
				ownerBytes += a.Bytes + a.Reserved
			}
		}
		if ownerBytes > math.MaxInt64-delta {
			return ErrInvalid
		}
		if e = limit("owner bytes", q.MaxTotalBytes, ownerBytes+delta); e != nil {
			return e
		}
		ttl := s.ReservationTTL
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		t = Ticket{ID: randomID(), FolderID: id, BlobID: randomID(), PathID: r.PathID, Principal: p, BaseVersion: r.BaseVersion, SealedSize: r.SealedSize, ReservedBytes: delta, ReservedFiles: files, Expires: s.now().Add(ttl)}
		m.Tickets[t.ID] = t
		return nil
	})
	return t, e
}
func (s *Server) ticket(ctx context.Context, p Principal, id, tid string) (Ticket, error) {
	if reader, ok := s.Meta.(authorizationReader); ok {
		return reader.authorize(ctx, p, id, true, tid, "", s.now())
	}
	var t Ticket
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		v, ok := m.Tickets[tid]
		if !ok || v.FolderID != id || v.Principal != p || !v.Expires.After(s.now()) {
			return ErrDenied
		}
		t = v
		return nil
	})
	return t, e
}
func (s *Server) Upload(ctx context.Context, p Principal, id string, provided Ticket, r io.Reader) error {
	t, e := s.ticket(ctx, p, id, provided.ID)
	if e != nil {
		return e
	}
	if t.Uploaded {
		return ErrInvalid
	}
	guard := &checkedReader{r: r, check: func() error { _, e := s.ticket(ctx, p, id, t.ID); return e }}
	n, e := s.Blobs.Put(ctx, id, t.BlobID, io.LimitReader(guard, t.SealedSize+1))
	if e != nil {
		_ = s.CancelUpload(ctx, p, id, t.ID)
		return e
	}
	if n != t.SealedSize {
		_ = s.Blobs.Delete(ctx, id, t.BlobID)
		_ = s.CancelUpload(ctx, p, id, t.ID)
		return ErrInvalid
	}
	e = s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		v, ok := m.Tickets[t.ID]
		if !ok || v.FolderID != id || v.Principal != p || !v.Expires.After(s.now()) {
			return ErrDenied
		}
		v.Uploaded = true
		m.Tickets[t.ID] = v
		return nil
	})
	if e != nil {
		_ = s.Blobs.Delete(context.Background(), id, t.BlobID)
	}
	return e
}
func (s *Server) CancelUpload(ctx context.Context, p Principal, id, tid string) error {
	return s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		t, ok := m.Tickets[tid]
		if !ok || t.Principal != p || t.FolderID != id {
			return ErrDenied
		}
		delete(m.Tickets, tid)
		return nil
	})
}
func (s *Server) Commit(ctx context.Context, p Principal, id string, mut []Mutation) (Delta, error) {
	var out Delta
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, true, false)
		if e != nil {
			return e
		}
		s.expire(m)
		if len(mut) == 0 || len(mut) > 256 {
			return ErrInvalid
		}
		seen := map[string]bool{}
		q, e := s.quota(ctx, f.Folder.Owner)
		if e != nil {
			return e
		}
		// Revalidate limits as plans may have been lowered since the reservation. Deletes remain legal.
		writes := false
		for _, v := range mut {
			if !v.Deleted {
				writes = true
			}
		}
		if writes {
			u := usage(m, id)
			if e = limit("folder bytes", f.Folder.Limits.MaxTotalBytes, u.Bytes+u.Reserved); e != nil {
				return e
			}
			if e = limit("folder files", f.Folder.Limits.MaxFiles, u.Files+u.ReservedFiles); e != nil {
				return e
			}
			var n int64
			for fid, v := range m.Folders {
				if v.Folder.Owner == f.Folder.Owner {
					u := usage(m, fid)
					n += u.Bytes + u.Reserved
				}
			}
			if e = limit("owner bytes", q.MaxTotalBytes, n); e != nil {
				return e
			}
		}
		for _, v := range mut {
			if !validPathID(v.PathID) || seen[v.PathID] || len(v.Metadata) > 16384 {
				return ErrInvalid
			}
			seen[v.PathID] = true
			old := m.Files[id][v.PathID]
			if old.Version != v.BaseVersion {
				return ErrConflict
			}
			if v.Deleted {
				if v.TicketID != "" || len(v.Metadata) != 0 {
					return ErrInvalid
				}
				continue
			}
			t, ok := m.Tickets[v.TicketID]
			if !ok || t.FolderID != id || t.PathID != v.PathID || t.Principal != p || t.BaseVersion != v.BaseVersion || !t.Uploaded || !t.Expires.After(s.now()) {
				return ErrDenied
			}
			if len(v.Metadata) < 28 {
				return ErrInvalid
			}
			n, e := s.Blobs.Size(ctx, id, t.BlobID)
			if e != nil || n != t.SealedSize {
				return ErrInvalid
			}
			if e = limit("file bytes", minimum(f.Folder.Limits.MaxFileBytes, q.MaxFileBytes), n); e != nil {
				return e
			}
		}
		if f.Folder.Version == math.MaxUint64 {
			return ErrInvalid
		}
		f.Folder.Version++
		for _, v := range mut {
			row := Row{FolderID: id, PathID: v.PathID, Version: f.Folder.Version, Deleted: v.Deleted}
			if !v.Deleted {
				t := m.Tickets[v.TicketID]
				row.BlobID = t.BlobID
				row.SealedSize = t.SealedSize
				row.Metadata = append([]byte(nil), v.Metadata...)
				delete(m.Tickets, t.ID)
			}
			m.Files[id][v.PathID] = row
			out.Rows = append(out.Rows, row)
		}
		m.Folders[id] = f
		out.Version = f.Folder.Version
		return nil
	})
	if e == nil {
		s.notify()
	}
	return out, e
}
func (s *Server) Changes(ctx context.Context, p Principal, id string, after uint64) (Delta, error) {
	var out Delta
	e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		f, e := access(m, p, id, false, false)
		if e != nil {
			return e
		}
		if after > f.Folder.Version {
			return ErrInvalid
		}
		out.Version = f.Folder.Version
		for _, r := range m.Files[id] {
			if r.Version > after {
				r.Metadata = append([]byte(nil), r.Metadata...)
				out.Rows = append(out.Rows, r)
			}
		}
		sort.Slice(out.Rows, func(i, j int) bool {
			if out.Rows[i].Version == out.Rows[j].Version {
				return out.Rows[i].PathID < out.Rows[j].PathID
			}
			return out.Rows[i].Version < out.Rows[j].Version
		})
		return nil
	})
	return out, e
}
func (s *Server) blobAccess(ctx context.Context, p Principal, id, blob string) error {
	if reader, ok := s.Meta.(authorizationReader); ok {
		_, e := reader.authorize(ctx, p, id, false, "", blob, s.now())
		return e
	}
	return s.Meta.Transaction(ctx, func(m *Metadata) error {
		if _, e := access(m, p, id, false, false); e != nil {
			return e
		}
		for _, r := range m.Files[id] {
			if !r.Deleted && r.BlobID == blob {
				return nil
			}
		}
		return ErrDenied
	})
}
func (s *Server) Download(ctx context.Context, p Principal, id, blob string) (io.ReadCloser, error) {
	if e := s.blobAccess(ctx, p, id, blob); e != nil {
		return nil, e
	}
	r, e := s.Blobs.Open(ctx, id, blob)
	if e != nil {
		return nil, e
	}
	return &checkedReadCloser{checkedReader{r, func() error { return s.blobAccess(ctx, p, id, blob) }}, r}, nil
}

type checkedReader struct {
	r     io.Reader
	check func() error
}

func (r *checkedReader) Read(p []byte) (int, error) {
	if e := r.check(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

type checkedReadCloser struct {
	checkedReader
	closer io.Closer
}

func (r *checkedReadCloser) Close() error { return r.closer.Close() }
func (s *Server) Wait(ctx context.Context, p Principal, id string, after uint64) (uint64, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		ch := s.signal
		s.mu.Unlock()
		var version uint64
		var e error
		if reader, ok := s.Meta.(versionReader); ok {
			version, e = reader.folderVersion(ctx, p, id)
		} else {
			var f Folder
			f, e = s.GetFolder(ctx, p, id)
			version = f.Version
		}
		if e != nil {
			return 0, e
		}
		if after > version {
			return 0, ErrInvalid
		}
		if version > after {
			return version, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ch:
		case <-ticker.C:
		}
	}
}

// Subscribe closes after cancellation or revocation, with a terminal error event.
func (s *Server) Subscribe(ctx context.Context, p Principal, id string, after uint64) (<-chan Event, error) {
	if _, e := s.GetFolder(ctx, p, id); e != nil {
		return nil, e
	}
	ch := make(chan Event, 1)
	go func() {
		defer close(ch)
		for {
			v, e := s.Wait(ctx, p, id, after)
			select {
			case ch <- Event{v, e}:
			case <-ctx.Done():
				return
			}
			if e != nil {
				return
			}
			after = v
		}
	}()
	return ch, nil
}

type InProcessClient struct {
	Server    *Server
	Principal Principal
}

func (s *Server) Client(p Principal) *InProcessClient { return &InProcessClient{s, p} }
func (c *InProcessClient) CreateFolder(x context.Context, v FolderSpec) (Folder, error) {
	return c.Server.CreateFolder(x, c.Principal, v)
}
func (c *InProcessClient) Grant(x context.Context, id string, p Principal, r Role) error {
	return c.Server.Grant(x, c.Principal, id, p, r)
}
func (c *InProcessClient) Revoke(x context.Context, id string, p Principal) error {
	return c.Server.Revoke(x, c.Principal, id, p)
}
func (c *InProcessClient) DeleteFolder(x context.Context, id string) error {
	return c.Server.DeleteFolder(x, c.Principal, id)
}
func (c *InProcessClient) SetLimits(x context.Context, id string, l Limits) error {
	return c.Server.SetLimits(x, c.Principal, id, l)
}
func (c *InProcessClient) ListFolders(x context.Context) ([]Folder, error) {
	return c.Server.ListFolders(x, c.Principal)
}
func (c *InProcessClient) GetFolder(x context.Context, id string) (Folder, error) {
	return c.Server.GetFolder(x, c.Principal, id)
}
func (c *InProcessClient) Reserve(x context.Context, id string, r UploadRequest) (Ticket, error) {
	return c.Server.Reserve(x, c.Principal, id, r)
}
func (c *InProcessClient) Upload(x context.Context, id string, t Ticket, r io.Reader) error {
	return c.Server.Upload(x, c.Principal, id, t, r)
}
func (c *InProcessClient) CancelUpload(x context.Context, id, tid string) error {
	return c.Server.CancelUpload(x, c.Principal, id, tid)
}
func (c *InProcessClient) Commit(x context.Context, id string, m []Mutation) (Delta, error) {
	return c.Server.Commit(x, c.Principal, id, m)
}
func (c *InProcessClient) Changes(x context.Context, id string, v uint64) (Delta, error) {
	return c.Server.Changes(x, c.Principal, id, v)
}
func (c *InProcessClient) Download(x context.Context, id, blob string) (io.ReadCloser, error) {
	return c.Server.Download(x, c.Principal, id, blob)
}
func (c *InProcessClient) Wait(x context.Context, id string, v uint64) (uint64, error) {
	return c.Server.Wait(x, c.Principal, id, v)
}
