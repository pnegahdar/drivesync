package engine

import (
	"bufio"
	"compress/zlib"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// A packed snapshot is derived authority data: every live row of one folder at
// one version, so a new replica can fetch a large tree in one direct GET and
// then catch up with Changes after Version. It holds only what Changes already
// returns (opaque ids, versions, sizes and sealed metadata). It is never
// charged to quota and is deleted with the folder.
//
// Format, big endian:
//
//	"DSSNAP1\n" | folder id (16 bytes) | version (8) | horizon (8)
//	zlib stream of records, then end of input:
//	  1 | path id (32) | blob id (16) | uvarint version | uvarint sealed size |
//	      uvarint metadata length | metadata
//	  0 | uvarint record count
//
// Records are in strictly increasing path id order.
const snapshotMagic = "DSSNAP1\n"
const snapshotHeaderLen = len(snapshotMagic) + 16 + 8 + 8

// Decoder bounds. A snapshot is untrusted input from the authority or a URL.
const (
	MaxSnapshotRows  = 1 << 23
	MaxSnapshotBytes = 2 << 30
)

const defaultSnapshotRows = 1000
const snapshotURLTTL = 5 * time.Minute
const snapshotBuildTimeout = 5 * time.Minute
const snapshotRecheck = time.Minute
const snapshotPage = 1024

// SnapshotBlobID is the fixed blob key of a folder's packed snapshot. It is
// not a hex blob id, so no ticket or row can name it: Download, DownloadURLs
// and Upload stay denied for it. BlobStores must accept it alongside hex ids.
const SnapshotBlobID = "snapshot"

type snapshotInfo struct {
	version, horizon uint64
	checked          time.Time
}
type snapshotBuild struct {
	done chan struct{}
	info snapshotInfo
	err  error
}
type snapshotCache struct {
	mu     sync.Mutex
	info   map[string]snapshotInfo
	builds map[string]*snapshotBuild
}

// folderSnapshotState is an administrative view, used after the caller has
// been authorized. gone covers a deleted or removed folder.
type folderSnapshotState struct {
	version, horizon uint64
	live             int64
	gone             bool
}

type snapshotSource interface {
	snapshotState(ctx context.Context, folder string) (folderSnapshotState, error)
	snapshotChanged(ctx context.Context, folder string, after uint64, limit int64) (int64, error)
	snapshotRows(ctx context.Context, folder string, start func(version, horizon uint64) error, row func(Row) error) error
}

func (s *Server) snapshotThreshold() int64 {
	if s.SnapshotRows < 0 {
		return -1
	}
	if s.SnapshotRows == 0 {
		return defaultSnapshotRows
	}
	return int64(s.SnapshotRows)
}

// snapshotAccess authorizes exactly like Changes.
func (s *Server) snapshotAccess(ctx context.Context, p Principal, id string) error {
	if reader, ok := s.Meta.(versionReader); ok {
		_, e := reader.folderVersion(ctx, p, id)
		return e
	}
	_, e := s.GetFolder(ctx, p, id)
	return e
}

// Snapshot returns a short-lived URL for the folder's packed snapshot and the
// version it describes. Zero and an empty URL mean none is offered: the folder
// is small, the blob store cannot sign URLs, or building or signing failed.
// The snapshot may be older than the folder; read Changes after Version.
func (s *Server) Snapshot(ctx context.Context, p Principal, id string) (uint64, string, error) {
	if e := s.snapshotAccess(ctx, p, id); e != nil {
		return 0, "", e
	}
	signer, ok := s.Blobs.(BlobURLStore)
	threshold := s.snapshotThreshold()
	if !ok || threshold < 0 {
		return 0, "", nil
	}
	st, e := s.folderSnapshotState(ctx, id)
	if e != nil {
		return snapshotFallback(ctx)
	}
	if st.gone {
		return 0, "", ErrDenied
	}
	if st.live < threshold {
		s.dropSmallSnapshot(ctx, id)
		return 0, "", nil
	}
	info, e := s.currentSnapshot(ctx, id, st, threshold)
	if errors.Is(e, ErrDenied) {
		return 0, "", ErrDenied
	}
	if e != nil {
		return snapshotFallback(ctx)
	}
	// Revocation or deletion during the build issues nothing.
	if e = s.snapshotAccess(ctx, p, id); e != nil {
		return 0, "", e
	}
	u, e := signer.DownloadURL(ctx, id, SnapshotBlobID, snapshotURLTTL)
	if e != nil || u == "" {
		return snapshotFallback(ctx)
	}
	return info.version, u, nil
}

func snapshotFallback(ctx context.Context) (uint64, string, error) {
	if e := ctx.Err(); e != nil {
		return 0, "", e
	}
	return 0, "", nil
}

func (s *Server) cachedSnapshot(id string) (snapshotInfo, bool) {
	s.snapshots.mu.Lock()
	defer s.snapshots.mu.Unlock()
	info, ok := s.snapshots.info[id]
	if ok && s.now().Sub(info.checked) > snapshotRecheck {
		return info, false
	}
	return info, ok
}
func (s *Server) cacheSnapshot(id string, info snapshotInfo, ok bool) {
	s.snapshots.mu.Lock()
	defer s.snapshots.mu.Unlock()
	if !ok {
		delete(s.snapshots.info, id)
		return
	}
	if s.snapshots.info == nil {
		s.snapshots.info = map[string]snapshotInfo{}
	}
	s.snapshots.info[id] = info
}

func (s *Server) currentSnapshot(ctx context.Context, id string, st folderSnapshotState, threshold int64) (snapshotInfo, error) {
	info, ok := s.cachedSnapshot(id)
	if !ok {
		var e error
		info, ok, e = s.readSnapshotHeader(ctx, id)
		if e != nil {
			return info, e
		}
		s.cacheSnapshot(id, info, ok)
	}
	if ok {
		fresh, e := s.snapshotFresh(ctx, id, info, st, threshold)
		if e != nil {
			return info, e
		}
		if fresh {
			return info, nil
		}
	}
	return s.buildSnapshot(ctx, id)
}

// A snapshot is stale once more than max(1000, 10% of live rows) rows changed after
// it, counted by row (one commit can change many), or compaction passed it.
func (s *Server) snapshotFresh(ctx context.Context, id string, info snapshotInfo, st folderSnapshotState, threshold int64) (bool, error) {
	if info.version > st.version || st.horizon > info.version {
		return false, nil
	}
	if info.version == st.version {
		return true, nil
	}
	stale := max(int64(1000), st.live/10)
	changed, e := s.snapshotChangedRows(ctx, id, info.version, stale+1)
	return e == nil && changed <= stale, e
}

// readSnapshotHeader recovers a stored snapshot's version across restarts and
// authorities. A missing or malformed blob reports false.
func (s *Server) readSnapshotHeader(ctx context.Context, id string) (snapshotInfo, bool, error) {
	r, e := s.Blobs.Open(ctx, id, SnapshotBlobID)
	if errors.Is(e, os.ErrNotExist) {
		return snapshotInfo{}, false, nil
	}
	if e != nil {
		return snapshotInfo{}, false, e
	}
	defer r.Close()
	var header [snapshotHeaderLen]byte
	if _, e = io.ReadFull(r, header[:]); e != nil {
		return snapshotInfo{}, false, nil
	}
	version, horizon, e := parseSnapshotHeader(header[:], id)
	if e != nil {
		return snapshotInfo{}, false, nil
	}
	return snapshotInfo{version: version, horizon: horizon, checked: s.now()}, true, nil
}

// buildSnapshot coalesces concurrent builds for one folder in this authority.
// The build outlives a cancelled caller so the next request can use it.
func (s *Server) buildSnapshot(ctx context.Context, id string) (snapshotInfo, error) {
	s.snapshots.mu.Lock()
	b := s.snapshots.builds[id]
	if b == nil {
		b = &snapshotBuild{done: make(chan struct{})}
		if s.snapshots.builds == nil {
			s.snapshots.builds = map[string]*snapshotBuild{}
		}
		s.snapshots.builds[id] = b
		go func() {
			bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotBuildTimeout)
			defer cancel()
			info, e := s.writeSnapshot(bctx, id)
			s.snapshots.mu.Lock()
			b.info, b.err = info, e
			delete(s.snapshots.builds, id)
			if e == nil {
				if s.snapshots.info == nil {
					s.snapshots.info = map[string]snapshotInfo{}
				}
				s.snapshots.info[id] = info
			} else {
				delete(s.snapshots.info, id)
			}
			s.snapshots.mu.Unlock()
			close(b.done)
		}()
	}
	s.snapshots.mu.Unlock()
	select {
	case <-b.done:
		return b.info, b.err
	case <-ctx.Done():
		return snapshotInfo{}, ctx.Err()
	}
}

func (s *Server) writeSnapshot(ctx context.Context, id string) (snapshotInfo, error) {
	staging, e := os.CreateTemp("", "drivesync-snapshot-*")
	if e != nil {
		return snapshotInfo{}, e
	}
	// Unlinked at once: the bytes never outlive this process.
	os.Remove(staging.Name())
	defer staging.Close()
	enc := newSnapshotEncoder(staging, id)
	var info snapshotInfo
	e = s.snapshotSourceRows(ctx, id, func(version, horizon uint64) error {
		info = snapshotInfo{version: version, horizon: horizon}
		return enc.header(version, horizon)
	}, enc.row)
	if e == nil {
		e = enc.close()
	}
	if e != nil {
		return snapshotInfo{}, e
	}
	if _, e = staging.Seek(0, io.SeekStart); e != nil {
		return snapshotInfo{}, e
	}
	blob := SnapshotBlobID
	// Blobs are immutable, so a rebuild deletes and publishes again. A reader
	// holding the old URL fails and falls back to Changes.
	if _, e = bounded(ctx, func(c context.Context) (struct{}, error) { return struct{}{}, s.Blobs.Delete(c, id, blob) }); e != nil && !errors.Is(e, os.ErrNotExist) {
		return snapshotInfo{}, e
	}
	// A failed Put publishes nothing, by the BlobStore contract.
	if _, e = s.Blobs.Put(ctx, id, blob, staging); e != nil {
		return snapshotInfo{}, e
	}
	// Deletion cleanup removes the snapshot before the folder record. A
	// publication that finished after that cleanup removes itself here.
	if st, e := s.folderSnapshotState(ctx, id); e != nil || st.gone {
		_, _ = bounded(context.WithoutCancel(ctx), func(c context.Context) (struct{}, error) { return struct{}{}, s.Blobs.Delete(c, id, blob) })
		if e == nil {
			e = ErrDenied
		}
		return snapshotInfo{}, e
	}
	info.checked = s.now()
	return info, nil
}

// dropSmallSnapshot removes a snapshot this authority knows about once the
// folder falls below the threshold, so derived data does not linger.
func (s *Server) dropSmallSnapshot(ctx context.Context, id string) {
	s.snapshots.mu.Lock()
	_, known := s.snapshots.info[id]
	building := s.snapshots.builds[id] != nil
	s.snapshots.mu.Unlock()
	if !known || building {
		return
	}
	if _, e := bounded(ctx, func(c context.Context) (struct{}, error) {
		return struct{}{}, s.Blobs.Delete(c, id, SnapshotBlobID)
	}); e == nil || errors.Is(e, os.ErrNotExist) {
		s.cacheSnapshot(id, snapshotInfo{}, false)
	}
}

// deleteSnapshot runs in folder cleanup before the folder record is removed.
func (s *Server) deleteSnapshot(ctx context.Context, id string) error {
	_, e := bounded(ctx, func(c context.Context) (struct{}, error) {
		return struct{}{}, s.Blobs.Delete(c, id, SnapshotBlobID)
	})
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	s.cacheSnapshot(id, snapshotInfo{}, false)
	return nil
}

func (s *Server) folderSnapshotState(ctx context.Context, id string) (folderSnapshotState, error) {
	if source, ok := s.Meta.(snapshotSource); ok {
		return source.snapshotState(ctx, id)
	}
	var out folderSnapshotState
	e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{Folder: id, ReadOnly: true, NoFiles: true, Paths: []string{}, Tickets: []string{}, Garbage: []string{}}), func(m *Metadata) error {
		f, ok := m.Folders[id]
		out = snapshotStateOf(f, ok)
		return nil
	})
	return out, e
}
func snapshotStateOf(f FolderRecord, ok bool) folderSnapshotState {
	return folderSnapshotState{version: f.Folder.Version, horizon: f.Folder.Horizon, live: f.FileUsage.Files, gone: !ok || f.Deleted}
}

func (s *Server) snapshotChangedRows(ctx context.Context, id string, after uint64, limit int64) (int64, error) {
	if source, ok := s.Meta.(snapshotSource); ok {
		return source.snapshotChanged(ctx, id, after, limit)
	}
	var n int64
	e := s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{Folder: id, ReadOnly: true, Tickets: []string{}, Garbage: []string{}}), func(m *Metadata) error {
		for _, r := range m.Files[id] {
			if r.Version > after && n < limit {
				n++
			}
		}
		return nil
	})
	return n, e
}

func (s *Server) snapshotSourceRows(ctx context.Context, id string, start func(uint64, uint64) error, row func(Row) error) error {
	if source, ok := s.Meta.(snapshotSource); ok {
		return source.snapshotRows(ctx, id, start, row)
	}
	return s.Meta.Transaction(context.WithValue(ctx, scopeKey{}, Scope{Folder: id, ReadOnly: true, Tickets: []string{}, Garbage: []string{}}), func(m *Metadata) error {
		f, ok := m.Folders[id]
		if !ok || f.Deleted {
			return ErrDenied
		}
		if e := start(f.Folder.Version, f.Folder.Horizon); e != nil {
			return e
		}
		rows := make([]Row, 0, len(m.Files[id]))
		for _, r := range m.Files[id] {
			if !r.Deleted {
				rows = append(rows, r)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].PathID < rows[j].PathID })
		for _, r := range rows {
			if e := row(r); e != nil {
				return e
			}
		}
		return nil
	})
}

func (s *SQLiteMetaStore) snapshotState(ctx context.Context, folder string) (folderSnapshotState, error) {
	return queryFolderSnapshotState(s.reads.QueryRowContext(ctx, "SELECT data FROM folders WHERE id=?", folder))
}
func (s *SQLiteMetaStore) snapshotChanged(ctx context.Context, folder string, after uint64, limit int64) (int64, error) {
	var n int64
	e := s.reads.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT 1 FROM files WHERE folder=? AND json_extract(data,'$.Version')>? LIMIT ?)", folder, after, limit).Scan(&n)
	return n, e
}
func (s *SQLiteMetaStore) snapshotRows(ctx context.Context, folder string, start func(uint64, uint64) error, row func(Row) error) error {
	// One read transaction is one WAL snapshot across every page.
	tx, e := s.reads.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return e
	}
	defer tx.Rollback()
	return snapshotPages(ctx, tx, folder, "SELECT data FROM folders WHERE id=?", "SELECT data FROM files WHERE folder=? AND path>? AND json_extract(data,'$.Deleted')=0 ORDER BY path LIMIT ?", start, row)
}

func (s *PostgresMetaStore) snapshotState(ctx context.Context, folder string) (folderSnapshotState, error) {
	return queryFolderSnapshotState(s.db.QueryRowContext(ctx, `SELECT data FROM `+s.tbl("folders")+` WHERE id=$1`, folder))
}
func (s *PostgresMetaStore) snapshotChanged(ctx context.Context, folder string, after uint64, limit int64) (int64, error) {
	var n int64
	e := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM `+s.tbl("files")+` WHERE folder=$1 AND `+s.fn("ds_file_version")+`(data)>$2 LIMIT $3) t`, folder, after, limit).Scan(&n)
	return n, e
}
func (s *PostgresMetaStore) snapshotRows(ctx context.Context, folder string, start func(uint64, uint64) error, row func(Row) error) error {
	// Repeatable read keeps every page on the transaction's snapshot. Short
	// pages keep each statement and the idle gap between them small.
	tx, e := s.readTx(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	return snapshotPages(ctx, tx, folder, `SELECT data FROM `+s.tbl("folders")+` WHERE id=$1`, `SELECT data FROM `+s.tbl("files")+` WHERE folder=$1 AND path>$2 AND NOT `+s.fn("ds_file_deleted")+`(data) ORDER BY path LIMIT $3`, start, row)
}

func queryFolderSnapshotState(r *sql.Row) (folderSnapshotState, error) {
	var data []byte
	if e := r.Scan(&data); errors.Is(e, sql.ErrNoRows) {
		return folderSnapshotState{gone: true}, nil
	} else if e != nil {
		return folderSnapshotState{}, e
	}
	var f FolderRecord
	if e := json.Unmarshal(data, &f); e != nil {
		return folderSnapshotState{}, e
	}
	return snapshotStateOf(f, true), nil
}

func snapshotPages(ctx context.Context, tx *sql.Tx, folder, folderQuery, filesQuery string, start func(uint64, uint64) error, row func(Row) error) error {
	st, e := queryFolderSnapshotState(tx.QueryRowContext(ctx, folderQuery, folder))
	if e != nil {
		return e
	}
	if st.gone {
		return ErrDenied
	}
	if e = start(st.version, st.horizon); e != nil {
		return e
	}
	after := ""
	for {
		rows, e := tx.QueryContext(ctx, filesQuery, folder, after, snapshotPage)
		if e != nil {
			return e
		}
		var page []Row
		for rows.Next() {
			var data []byte
			var r Row
			if e = rows.Scan(&data); e == nil {
				e = json.Unmarshal(data, &r)
			}
			if e != nil {
				rows.Close()
				return e
			}
			page = append(page, r)
		}
		e = errors.Join(rows.Err(), rows.Close())
		if e != nil {
			return e
		}
		for _, r := range page {
			if e = row(r); e != nil {
				return e
			}
		}
		if len(page) < snapshotPage {
			return nil
		}
		after = page[len(page)-1].PathID
	}
}

type snapshotEncoder struct {
	out    *bufio.Writer
	z      *zlib.Writer
	folder []byte
	last   string
	count  uint64
	buf    []byte
}

func newSnapshotEncoder(w io.Writer, folder string) *snapshotEncoder {
	f, _ := hex.DecodeString(folder)
	return &snapshotEncoder{out: bufio.NewWriterSize(w, 256<<10), folder: f}
}
func (e *snapshotEncoder) header(version, horizon uint64) error {
	if len(e.folder) != 16 || horizon > version {
		return ErrInvalid
	}
	h := make([]byte, 0, snapshotHeaderLen)
	h = append(h, snapshotMagic...)
	h = append(h, e.folder...)
	h = binary.BigEndian.AppendUint64(h, version)
	h = binary.BigEndian.AppendUint64(h, horizon)
	if _, err := e.out.Write(h); err != nil {
		return err
	}
	e.z = zlib.NewWriter(e.out)
	return nil
}
func (e *snapshotEncoder) row(r Row) error {
	path, pe := hex.DecodeString(r.PathID)
	blob, be := hex.DecodeString(r.BlobID)
	if e.z == nil || pe != nil || be != nil || len(path) != 32 || len(blob) != 16 || r.Deleted || r.PathID <= e.last || r.SealedSize < 0 || len(r.Metadata) > 16384 {
		return ErrInvalid
	}
	b := append(e.buf[:0], 1)
	b = append(b, path...)
	b = append(b, blob...)
	b = binary.AppendUvarint(b, r.Version)
	b = binary.AppendUvarint(b, uint64(r.SealedSize))
	b = binary.AppendUvarint(b, uint64(len(r.Metadata)))
	b = append(b, r.Metadata...)
	e.buf = b
	e.last = r.PathID
	e.count++
	_, err := e.z.Write(b)
	return err
}
func (e *snapshotEncoder) close() error {
	if e.z == nil {
		return ErrInvalid
	}
	b := binary.AppendUvarint([]byte{0}, e.count)
	if _, err := e.z.Write(b); err != nil {
		return err
	}
	if err := e.z.Close(); err != nil {
		return err
	}
	return e.out.Flush()
}

func parseSnapshotHeader(h []byte, folder string) (version, horizon uint64, err error) {
	if len(h) != snapshotHeaderLen || string(h[:len(snapshotMagic)]) != snapshotMagic || hex.EncodeToString(h[len(snapshotMagic):len(snapshotMagic)+16]) != folder {
		return 0, 0, ErrIntegrity
	}
	version = binary.BigEndian.Uint64(h[len(snapshotMagic)+16:])
	horizon = binary.BigEndian.Uint64(h[len(snapshotMagic)+24:])
	if horizon > version {
		return 0, 0, ErrIntegrity
	}
	return version, horizon, nil
}

// ReadSnapshot streams a packed snapshot's rows to fn in path order. The
// header must name folder and version exactly; every row must be live, at or
// below version, and the stream must end exactly after its counted trailer.
// Rows are not authenticated here: their sealed metadata still needs
// OpenMetadata like rows from Changes. Errors from r pass through; framing
// and bound violations are ErrIntegrity.
func ReadSnapshot(r io.Reader, folder string, version uint64, fn func(Row) error) (horizon uint64, err error) {
	if !validID(folder) {
		return 0, ErrInvalid
	}
	raw := &snapshotSourceReader{r: r}
	in := bufio.NewReaderSize(raw, 64<<10)
	var h [snapshotHeaderLen]byte
	if _, e := io.ReadFull(in, h[:]); e != nil {
		return 0, raw.cause(e)
	}
	got, horizon, e := parseSnapshotHeader(h[:], folder)
	if e != nil || got != version || version == 0 {
		return 0, ErrIntegrity
	}
	z, e := zlib.NewReader(in)
	if e != nil {
		return 0, raw.cause(e)
	}
	body := bufio.NewReaderSize(&snapshotLimit{z, MaxSnapshotBytes}, 64<<10)
	var count uint64
	last := ""
	var path [32]byte
	var blob [16]byte
	for {
		tag, e := body.ReadByte()
		if e != nil {
			return 0, raw.cause(e)
		}
		if tag == 0 {
			n, e := binary.ReadUvarint(body)
			if e != nil {
				return 0, raw.cause(e)
			}
			if n != count {
				return 0, ErrIntegrity
			}
			break
		}
		if tag != 1 || count == MaxSnapshotRows {
			return 0, ErrIntegrity
		}
		if _, e = io.ReadFull(body, path[:]); e == nil {
			_, e = io.ReadFull(body, blob[:])
		}
		var v, size, n uint64
		if e == nil {
			v, e = binary.ReadUvarint(body)
		}
		if e == nil {
			size, e = binary.ReadUvarint(body)
		}
		if e == nil {
			n, e = binary.ReadUvarint(body)
		}
		if e != nil {
			return 0, raw.cause(e)
		}
		if v == 0 || v > version || size > uint64(MaxRequestBytes) || n > 16384 {
			return 0, ErrIntegrity
		}
		row := Row{FolderID: folder, PathID: hex.EncodeToString(path[:]), BlobID: hex.EncodeToString(blob[:]), Version: v, SealedSize: int64(size), Metadata: make([]byte, n)}
		if _, e = io.ReadFull(body, row.Metadata); e != nil {
			return 0, raw.cause(e)
		}
		if row.PathID <= last {
			return 0, ErrIntegrity
		}
		last = row.PathID
		count++
		if e = fn(row); e != nil {
			return 0, e
		}
	}
	// Nothing may follow the trailer, inside the zlib stream (its checksum is
	// verified at EOF) or after it.
	if _, e = body.ReadByte(); e != io.EOF {
		return 0, raw.cause(e)
	}
	if e = z.Close(); e != nil {
		return 0, raw.cause(e)
	}
	if _, e = in.ReadByte(); e != io.EOF {
		return 0, raw.cause(e)
	}
	return horizon, nil
}

// DecodeSnapshot reads a whole packed snapshot as a full Delta at version.
func DecodeSnapshot(r io.Reader, folder string, version uint64) (Delta, error) {
	out := Delta{Full: true, Version: version}
	horizon, e := ReadSnapshot(r, folder, version, func(row Row) error {
		out.Rows = append(out.Rows, row)
		return nil
	})
	if e != nil {
		return Delta{}, e
	}
	out.Horizon = horizon
	return out, nil
}

// snapshotSourceReader remembers a transport error so it is reported as
// itself, not as a framing failure.
type snapshotSourceReader struct {
	r   io.Reader
	err error
}

func (s *snapshotSourceReader) Read(p []byte) (int, error) {
	n, e := s.r.Read(p)
	if e != nil && e != io.EOF {
		s.err = e
	}
	return n, e
}
func (s *snapshotSourceReader) cause(e error) error {
	if s.err != nil {
		return s.err
	}
	if e == nil {
		return ErrIntegrity
	}
	return fmt.Errorf("%w: %w", ErrIntegrity, e)
}

type snapshotLimit struct {
	r         io.Reader
	remaining int64
}

func (l *snapshotLimit) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, ErrIntegrity
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, e := l.r.Read(p)
	l.remaining -= int64(n)
	return n, e
}
