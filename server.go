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
	if !p.valid() || !ok || f.Deleted {
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
			s.retireTicket(m, t)

			delete(m.Tickets, id)
		}
	}
}
func (s *Server) quota(ctx context.Context, p Principal) (Quota, error) {
	if s.Quotas == nil {
		return Quota{}, nil
	}
	q, e := bounded(ctx, func(c context.Context) (Quota, error) { return s.Quotas.Quota(c, p) })
	if e == nil && (q.MaxTotalBytes < 0 || q.MaxFolders < 0 || q.MaxFileBytes < 0 || q.MaxFiles < 0) {
		e = ErrInvalid
	}
	return q, e
}
func sat(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
func rowBytes(r Row) int64 { return sat(RowCost, sat(r.SealedSize, int64(len(r.Metadata)))) }
func usage(m *Metadata, id string) Usage {
	u := m.Folders[id].FileUsage
	if m.filesLoaded {
		u = fileUsage(m.Files[id])
	}
	for _, t := range m.Tickets {
		if t.FolderID == id {
			u.Reserved = sat(u.Reserved, t.ReservedBytes)
			u.ReservedFiles = sat(u.ReservedFiles, t.ReservedFiles)
			u.ReservedRows = sat(u.ReservedRows, t.ReservedRows)
		}
	}
	for _, g := range m.Garbage {
		if g.FolderID == id {
			u.GarbageRows++
			u.Bytes = sat(u.Bytes, g.Size)
		}
	}
	return u
}
func allocatedRows(l Limits) int64 { return min(l.MaxTotalBytes/RowCost, l.MaxRows) }
func ownerLimit(p Principal, f FolderRecord, name string, max, n int64) error {
	if e := limit(name, max, n); e != nil {
		if p != f.Folder.Owner {
			return ErrQuota
		}
		return e
	}
	return nil
}

// Allocations are sticky until folder deletion. Revocations and downgrades
// change authorization only, so they never query the owner's pricing policy.
func (s *Server) allocation(ctx context.Context, m *Metadata, p Principal, f, old FolderRecord) error {
	if !f.Allocated && len(f.Grants) == 0 {
		m.Folders[f.Folder.ID] = f
		return nil
	}
	if f.Folder.Limits.MaxTotalBytes <= 0 || f.Folder.Limits.MaxRows <= 0 {
		return ErrInvalid
	}
	grows := !old.Allocated || f.Folder.Limits.MaxTotalBytes > old.Folder.Limits.MaxTotalBytes || allocatedRows(f.Folder.Limits) > allocatedRows(old.Folder.Limits) || (old.Folder.Limits.MaxFileBytes > 0 && (f.Folder.Limits.MaxFileBytes == 0 || f.Folder.Limits.MaxFileBytes > old.Folder.Limits.MaxFileBytes))
	f.Allocated = true
	m.Folders[f.Folder.ID] = f
	if !grows {
		return nil
	}
	if p != f.Folder.Owner {
		return ErrDenied
	}
	q, e := s.quota(m.ctx, f.Folder.Owner)
	if e != nil {
		return e
	}
	f.Folder.Limits.MaxFileBytes = minimum(f.Folder.Limits.MaxFileBytes, q.MaxFileBytes)
	m.Folders[f.Folder.ID] = f
	bytes, rows := ownerUsage(m, f.Folder.Owner)
	if e = ownerLimit(p, f, "owner bytes", q.MaxTotalBytes, bytes); e != nil {
		return e
	}
	return ownerLimit(p, f, "owner rows", q.MaxFiles, rows)
}
func limit(name string, max, requested int64) error {
	if max > 0 && requested > max {
		return &LimitError{name, max, requested}
	}
	return nil
}
func (s *Server) folder(ctx context.Context, m *Metadata, p Principal, f FolderRecord) (Folder, error) {
	v := f.Folder
	if p == v.Owner && !f.Allocated {
		q, e := s.quota(m.ctx, v.Owner)
		if e != nil {
			return v, e
		}
		v.Limits.MaxFileBytes = minimum(v.Limits.MaxFileBytes, q.MaxFileBytes)
		v.Limits.MaxTotalBytes = minimum(v.Limits.MaxTotalBytes, q.MaxTotalBytes)
		v.Limits.MaxFiles = minimum(v.Limits.MaxFiles, q.MaxFiles)
	}
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
	e := s.transaction(ctx, p, "", true, func(m *Metadata) error {
		if spec.Name == "" || !utf8.ValidString(spec.Name) || !utf8.ValidString(spec.Description) || len(spec.Name) > 255 || len(spec.Description) > 4096 || len(spec.KeyCheck) != 32 || !validLimits(spec.Limits) {
			return ErrInvalid
		}
		q, e := s.quota(m.ctx, p)
		if e != nil {
			return e
		}
		count := accountUsage(m, principalKey(p)).Folders
		if e = limit("owner folders", q.MaxFolders, count+1); e != nil {
			return e
		}
		f = Folder{ID: randomID(), Owner: p, Name: spec.Name, Description: spec.Description, Limits: spec.Limits, KeyCheck: append([]byte(nil), spec.KeyCheck...)}
		m.Folders[f.ID] = FolderRecord{Folder: f, Grants: map[string]Role{}, OwnerKey: principalKey(p)}
		m.Files[f.ID] = map[string]Row{}
		return nil
	})
	return f, e
}
func (s *Server) Grant(ctx context.Context, p Principal, id string, grantee Principal, role Role) error {
	e := s.transaction(ctx, p, id, true, func(m *Metadata) error {
		f, e := access(m, p, id, false, true)
		if e != nil {
			return e
		}
		if !grantee.valid() || (role != Reader && role != Writer && role != Owner) || grantee == f.Folder.Owner {
			return ErrInvalid
		}
		if _, exists := f.Grants[principalKey(grantee)]; !exists && len(f.Grants) >= MaxGrants {
			return ErrInvalid
		}
		old := f
		f.Grants[principalKey(grantee)] = role
		if role == Reader {
			for tid, t := range m.Tickets {
				if t.FolderID == id && t.Principal == grantee {
					s.retireTicket(m, t)
					delete(m.Tickets, tid)
				}
			}
		}
		m.Folders[id] = f
		return s.allocation(m.ctx, m, p, f, old)
	})
	if e == nil {
		s.notify()
	}
	return e
}
func (s *Server) Revoke(ctx context.Context, p Principal, id string, grantee Principal) error {
	e := s.transaction(ctx, p, id, true, func(m *Metadata) error {
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
				s.retireTicket(m, t)
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
	e := s.transaction(ctx, p, id, true, func(m *Metadata) error {
		if _, e := access(m, p, id, false, true); e != nil {
			return e
		}
		for _, row := range m.Files[id] {
			s.retireRow(m, row)
		}
		delete(m.Files, id)
		for tid, t := range m.Tickets {
			if t.FolderID == id {
				s.retireTicket(m, t)
				delete(m.Tickets, tid)
			}
		}
		f := m.Folders[id]
		f.Deleted = true
		f.Grants = map[string]Role{}
		m.Folders[id] = f
		return nil
	})
	if e == nil {
		s.notify()
	}
	return e
}
func (s *Server) SetLimits(ctx context.Context, p Principal, id string, l Limits) error {
	return s.transaction(ctx, p, id, true, func(m *Metadata) error {
		f, e := access(m, p, id, false, true)
		if e != nil {
			return e
		}
		if !validLimits(l) {
			return ErrInvalid
		}
		if f.Allocated && p != f.Folder.Owner && l != f.Folder.Limits {
			return ErrDenied
		}
		old := f
		f.Folder.Limits = l
		m.Folders[id] = f
		return s.allocation(m.ctx, m, p, f, old)
	})
}
func (s *Server) ListFolders(ctx context.Context, p Principal) ([]Folder, error) {
	out := []Folder{}
	if !p.valid() {
		return nil, ErrDenied
	}
	e := s.transaction(ctx, p, "", false, func(m *Metadata) error {
		s.expire(m)
		for id, f := range m.Folders {
			if _, e := access(m, p, id, false, false); e == nil {
				v, e := s.folder(m.ctx, m, p, f)
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
	ctx = context.WithValue(ctx, noFilesKey{}, true)
	var out Folder
	e := s.transaction(ctx, p, id, false, func(m *Metadata) error {
		f, e := access(m, p, id, false, false)
		if e != nil {
			return e
		}
		s.expire(m)
		out, e = s.folder(m.ctx, m, p, f)
		return e
	})
	return out, e
}
func (s *Server) Reserve(ctx context.Context, p Principal, id string, r UploadRequest) (Ticket, error) {
	var t Ticket
	// Retire our own stale ticket in a separate durable transaction, then collect
	// its object before reserving replacement capacity. Busy peers are left alone.
	pre := s.transaction(ctx, p, id, true, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		s.expire(m)
		if !validPathID(r.PathID) || r.SealedSize < 0 || r.SealedSize > MaxRequestBytes {
			return ErrInvalid
		}
		if m.Files[id][r.PathID].Version != r.BaseVersion {
			return &ConflictError{[]string{r.PathID}}
		}
		for tid, v := range m.Tickets {
			if v.FolderID == id && v.PathID == r.PathID && v.Principal == p {
				s.retireTicket(m, v)
				delete(m.Tickets, tid)
			}
		}
		return nil
	})
	if pre != nil {
		return t, pre
	}
	e := s.transaction(ctx, p, id, true, func(m *Metadata) error {
		f, e := access(m, p, id, true, false)
		if e != nil {
			return e
		}
		s.expire(m)
		if !validPathID(r.PathID) || r.SealedSize < 0 || r.SealedSize > MaxRequestBytes || r.MetadataBytes < 0 || r.MetadataBytes > 16384 {
			return ErrInvalid
		}
		old := m.Files[id][r.PathID]
		if old.Version != r.BaseVersion {
			return &ConflictError{[]string{r.PathID}}
		}
		for tid, v := range m.Tickets {
			if v.FolderID == id && v.PathID == r.PathID {
				if v.Principal != p {
					return ErrBusy
				}
				s.retireTicket(m, v)
				delete(m.Tickets, tid)
			}
		}
		q := Quota{}
		if !f.Allocated {
			q, e = s.quota(m.ctx, f.Folder.Owner)
			if e != nil {
				return e
			}
		}
		if e = limit("file bytes", f.Folder.Limits.MaxFileBytes, r.SealedSize); e != nil {
			return e
		}
		if !f.Allocated {
			if e = ownerLimit(p, f, "owner file bytes", q.MaxFileBytes, r.SealedSize); e != nil {
				return e
			}
		}
		delta := sat(r.SealedSize, sat(RowCost, r.MetadataBytes))
		files := int64Bool(old.Version == 0 || old.Deleted)
		rows := int64(1)

		u := usage(m, id)
		if e = limit("folder files", f.Folder.Limits.MaxFiles, sat(sat(u.Files, u.ReservedFiles), max(files, 0))); e != nil {
			return e
		}
		if e = limit("folder bytes", f.Folder.Limits.MaxTotalBytes, sat(sat(u.Bytes, u.Reserved), delta)); e != nil {
			return e
		}
		if e = limit("folder rows", rowBudget(f.Folder.Limits), sat(sat(sat(u.Rows, u.GarbageRows), u.ReservedRows), rows)); e != nil {
			return e
		}
		if !f.Allocated {
			bytes, nrows := ownerUsage(m, f.Folder.Owner)
			if e = ownerLimit(p, f, "owner bytes", q.MaxTotalBytes, sat(bytes, delta)); e != nil {
				return e
			}
			if e = ownerLimit(p, f, "owner rows", q.MaxFiles, sat(nrows, rows)); e != nil {
				return e
			}
		}
		ttl := s.ReservationTTL
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		t = Ticket{ID: randomID(), FolderID: id, BlobID: randomID(), PathID: r.PathID, Principal: p, BaseVersion: r.BaseVersion, SealedSize: r.SealedSize, ReservedBytes: delta, ReservedFiles: max(files, 0), ReservedRows: rows, Expires: s.now().Add(ttl)}
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
	e := s.transaction(ctx, p, id, false, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		v, ok := m.Tickets[tid]
		if !ok || v.FolderID != id || v.Principal != p {
			return ErrDenied
		}
		if !v.Expires.After(s.now()) {
			return ErrExpired
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
	e = s.transaction(ctx, p, id, false, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		v, ok := m.Tickets[t.ID]
		if !ok || v.Principal != p || v.FolderID != id {
			return ErrDenied
		}
		if !v.Expires.After(s.now()) {
			return ErrExpired
		}
		if v.Writing || v.Uploaded {
			return ErrBusy
		}
		v.Writing = true
		m.Tickets[t.ID] = v
		return nil
	})
	if e != nil {
		return e
	}
	guard := &renewingReader{r: r, renew: func() error { return s.renew(ctx, p, id, t.ID) }}
	n, putErr := s.Blobs.Put(ctx, id, t.BlobID, io.LimitReader(guard, t.SealedSize+1))
	if putErr == nil && n != t.SealedSize {
		putErr = ErrInvalid
	}
	// Publication has finished. This internal transition needs no current grant:
	// revocation/cancellation must still be able to finish durable cleanup.
	finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finishCtx = context.WithValue(finishCtx, scopeKey{}, Scope{Folder: id, NoFiles: true})
	ack := s.Meta.Transaction(finishCtx, func(m *Metadata) error {
		v, ok := m.Tickets[t.ID]
		_, authorized := access(m, p, id, true, false)
		if ok && authorized == nil && v.Principal == p && v.Expires.After(s.now()) && putErr == nil {
			v.Uploaded = true
			v.Writing = false
			m.Tickets[t.ID] = v
			return nil
		}
		if ok {
			s.retireTicket(m, v)
			delete(m.Tickets, t.ID)
		}
		key := id + "/" + t.BlobID
		if g, ok := m.Garbage[key]; ok {
			g.Writing = false
			m.Garbage[key] = g
		}
		return nil
	})
	if ack != nil {
		return ack
	}
	if putErr != nil {
		return putErr
	}
	_, e = s.ticket(ctx, p, id, t.ID)
	return e
}

func (s *Server) CancelUpload(ctx context.Context, p Principal, id, tid string) error {
	return s.transaction(ctx, p, id, true, func(m *Metadata) error {
		if _, e := access(m, p, id, true, false); e != nil {
			return e
		}
		t, ok := m.Tickets[tid]
		if !ok || t.Principal != p || t.FolderID != id {
			return ErrDenied
		}
		s.retireTicket(m, t)
		delete(m.Tickets, tid)
		return nil
	})
}
func (s *Server) Commit(ctx context.Context, p Principal, id string, mut []Mutation) (Delta, error) {
	var out Delta
	e := s.transaction(ctx, p, id, true, func(m *Metadata) error {
		f, e := access(m, p, id, true, false)
		if e != nil {
			return e
		}
		for _, v := range mut {
			if t, ok := m.Tickets[v.TicketID]; ok && t.Principal == p && t.FolderID == id && !t.Expires.After(s.now()) {
				return ErrExpired
			}
		}
		s.expire(m)
		if len(mut) == 0 || len(mut) > 256 {
			return ErrInvalid
		}
		seen := map[string]Mutation{}
		var conflicts []string
		for _, v := range mut {
			if !validPathID(v.PathID) || len(v.Metadata) > 16384 {
				return ErrInvalid
			}
			if _, ok := seen[v.PathID]; ok {
				return ErrInvalid
			}
			seen[v.PathID] = v
			if m.Files[id][v.PathID].Version != v.BaseVersion {
				conflicts = append(conflicts, v.PathID)
			}
		}
		if len(conflicts) > 0 {
			return &ConflictError{conflicts}
		}
		needsQuota := false
		for _, v := range mut {
			if !v.Deleted || m.Files[id][v.PathID].Version == 0 {
				needsQuota = true
			}
		}
		q := Quota{}
		if !f.Allocated && needsQuota {
			q, e = s.quota(m.ctx, f.Folder.Owner)
			if e != nil {
				return e
			}
		}
		var retired []Row
		writes := false
		growth := false
		if f.Folder.Version == math.MaxUint64 {
			return ErrInvalid
		}
		f.Folder.Version++
		for _, v := range mut {
			old := m.Files[id][v.PathID]
			row := Row{FolderID: id, PathID: v.PathID, Version: f.Folder.Version, Deleted: v.Deleted}
			if v.Deleted {
				if v.TicketID != "" || len(v.Metadata) != 0 {
					return ErrInvalid
				}
				if old.Version == 0 {
					growth = true
				}
			} else {
				writes = true
				t, ok := m.Tickets[v.TicketID]
				if !ok || t.FolderID != id || t.PathID != v.PathID || t.Principal != p || t.BaseVersion != v.BaseVersion || !t.Uploaded {
					return ErrDenied
				}
				if !t.Expires.After(s.now()) {
					return ErrExpired
				}
				if len(v.Metadata) < 28 {
					return ErrInvalid
				}

				n, se := bounded(m.ctx, func(c context.Context) (int64, error) { return s.Blobs.Size(c, id, t.BlobID) })
				if se != nil || n != t.SealedSize {
					return ErrInvalid
				}
				if e = limit("file bytes", f.Folder.Limits.MaxFileBytes, n); e != nil {
					return e
				}
				if !f.Allocated {
					if e = ownerLimit(p, f, "owner file bytes", q.MaxFileBytes, n); e != nil {
						return e
					}
				}
				row.BlobID = t.BlobID
				row.SealedSize = n
				row.Metadata = append([]byte(nil), v.Metadata...)
				delete(m.Tickets, t.ID)
			}
			if old.BlobID != "" {
				retired = append(retired, old)
			}
			m.Files[id][v.PathID] = row
			out.Rows = append(out.Rows, row)
		}
		for _, row := range retired {
			s.retireRow(m, row)
		}
		if writes || growth {
			u := usage(m, id)
			if e = limit("folder files", f.Folder.Limits.MaxFiles, sat(u.Files, u.ReservedFiles)); e != nil {
				return e
			}
			if e = limit("folder bytes", f.Folder.Limits.MaxTotalBytes, sat(u.Bytes, u.Reserved)); e != nil {
				return e
			}
			if e = limit("folder rows", rowBudget(f.Folder.Limits), sat(sat(u.Rows, u.GarbageRows), u.ReservedRows)); e != nil {
				return e
			}
			if !f.Allocated {
				bytes, rows := ownerUsage(m, f.Folder.Owner)
				if e = ownerLimit(p, f, "owner bytes", q.MaxTotalBytes, bytes); e != nil {
					return e
				}
				if e = ownerLimit(p, f, "owner rows", q.MaxFiles, rows); e != nil {
					return e
				}
			}
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
	e := s.transaction(ctx, p, id, false, func(m *Metadata) error {
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
	return s.transaction(ctx, p, id, false, func(m *Metadata) error {
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

func rowBudget(l Limits) int64 {
	if l.MaxRows > 0 {
		return l.MaxRows
	}
	if l.MaxFiles > 0 {
		if l.MaxFiles >= 62_500 {
			return 1_000_000
		}
		return l.MaxFiles * 16
	}
	return 1_000_000
}
func int64Bool(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
