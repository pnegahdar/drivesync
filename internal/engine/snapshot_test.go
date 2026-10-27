package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// snapshotURLStore signs fake URLs and can hold or fail snapshot publication.
type snapshotURLStore struct {
	BlobStore
	signErr atomic.Bool
	putErr  atomic.Bool
	puts    atomic.Int64
	entered chan struct{}
	gate    chan struct{}
}

func (b *snapshotURLStore) Put(ctx context.Context, f, id string, r io.Reader) (int64, error) {
	if id == SnapshotBlobID {
		b.puts.Add(1)
		if b.entered != nil {
			b.entered <- struct{}{}
		}
		if b.gate != nil {
			<-b.gate
		}
		if b.putErr.Load() {
			return 0, errors.New("put failed")
		}
	}
	return b.BlobStore.Put(ctx, f, id, r)
}
func (b *snapshotURLStore) DownloadURL(ctx context.Context, f, id string, ttl time.Duration) (string, error) {
	if ttl != 5*time.Minute || b.signErr.Load() {
		return "", errors.New("sign failed")
	}
	return "https://blobs.invalid/" + f + "/" + id + "?sig=secret", nil
}

// fetch follows a URL like a replica would, never trusting it for identity.
func (b *snapshotURLStore) fetch(t *testing.T, folder string, version uint64, raw string) Delta {
	t.Helper()
	u, e := url.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != folder || parts[1] != SnapshotBlobID {
		t.Fatalf("url %q", raw)
	}
	r, e := b.BlobStore.Open(context.Background(), parts[0], parts[1])
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	d, e := DecodeSnapshot(r, folder, version)
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func (b *snapshotURLStore) stored(folder string) bool {
	_, e := b.BlobStore.Size(context.Background(), folder, SnapshotBlobID)
	return e == nil
}

func snapshotServer(t *testing.T, rows int) (*Server, *snapshotURLStore) {
	s, _ := testServer(t)
	store := &snapshotURLStore{BlobStore: s.Blobs}
	s.Blobs = store
	s.SnapshotRows = rows
	return s, store
}
func putN(t *testing.T, c Client, f Folder, k FolderKey, prefix string, n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = put(t, c, f, k, fmt.Sprintf("%s%d", prefix, i), 0, []byte(fmt.Sprint(prefix, i)))
	}
	return rows
}
func deleteRows(t *testing.T, c Client, f Folder, rows []Row) {
	t.Helper()
	m := make([]Mutation, len(rows))
	for i, r := range rows {
		m[i] = Mutation{PathID: r.PathID, BaseVersion: r.Version, Deleted: true}
	}
	if _, e := c.Commit(context.Background(), f.ID, m); e != nil {
		t.Fatal(e)
	}
}
func liveRows(t *testing.T, c Client, f Folder) map[string]Row {
	t.Helper()
	d, e := c.Changes(context.Background(), f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	out := map[string]Row{}
	for _, r := range d.Rows {
		if !r.Deleted {
			out[r.PathID] = r
		}
	}
	return out
}

func TestSnapshotServesLiveRowsUncharged(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, store := snapshotServer(t, 3)
			c := clients(t, s, httpMode)(owner)
			f, k := sharedFor(t, c)
			ctx := context.Background()
			rows := putN(t, c, f, k, "a", 2)
			if v, u, e := c.Snapshot(ctx, f.ID); v != 0 || u != "" || e != nil {
				t.Fatal("below threshold", v, u, e)
			}
			rows = append(rows, putN(t, c, f, k, "b", 2)...)
			deleteRows(t, c, f, rows[:1])
			before, e := c.GetFolder(ctx, f.ID)
			if e != nil {
				t.Fatal(e)
			}
			v, u, e := c.Snapshot(ctx, f.ID)
			if e != nil || v != before.Version || u == "" {
				t.Fatal(v, u, e)
			}
			d := store.fetch(t, f.ID, v, u)
			want := liveRows(t, c, f)
			if !d.Full || d.Version != v || len(d.Rows) != len(want) || len(want) != 3 {
				t.Fatal(d, want)
			}
			for _, r := range d.Rows {
				w := want[r.PathID]
				if r.FolderID != f.ID || r.BlobID != w.BlobID || r.Version != w.Version || r.SealedSize != w.SealedSize || !bytes.Equal(r.Metadata, w.Metadata) {
					t.Fatal(r, w)
				}
				if _, e := OpenMetadata(k, f.ID, r); e != nil {
					t.Fatal(e)
				}
			}
			after, e := c.GetFolder(ctx, f.ID)
			if e != nil || after.Usage != before.Usage {
				t.Fatal("snapshot charged", before.Usage, after.Usage, e)
			}
			// The derived blob is reachable only through Snapshot.
			blob := SnapshotBlobID
			if _, e := c.Download(ctx, f.ID, blob); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			if _, e := c.DownloadURLs(ctx, f.ID, []string{blob}); !errors.Is(e, ErrDenied) {
				t.Fatal(e)
			}
			if _, e := c.Changes(ctx, f.ID, v); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestSnapshotAuthorizationMatrix(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, store := snapshotServer(t, 2)
			cc := clients(t, s, httpMode)
			admin := cc(owner)
			f, k := sharedFor(t, admin)
			ctx := context.Background()
			putN(t, admin, f, k, "f", 2)
			grants := map[string]Role{"writer": Writer, "reader": Reader, "revoked": Reader, "delegated": Owner}
			for name, role := range grants {
				if e := admin.Grant(ctx, f.ID, Principal{"tenant", name}, role); e != nil {
					t.Fatal(e)
				}
			}
			if e := admin.Revoke(ctx, f.ID, Principal{"tenant", "revoked"}); e != nil {
				t.Fatal(e)
			}
			for _, tc := range []struct {
				p       Principal
				allowed bool
			}{{owner, true}, {Principal{"tenant", "delegated"}, true}, {Principal{"tenant", "writer"}, true}, {Principal{"tenant", "reader"}, true}, {Principal{"other", "owner"}, false}, {Principal{"tenant", "stranger"}, false}, {Principal{"tenant", "revoked"}, false}, {Principal{}, false}} {
				v, u, e := cc(tc.p).Snapshot(ctx, f.ID)
				if tc.allowed && (e != nil || v == 0 || u == "") {
					t.Fatal(tc.p, v, u, e)
				}
				if !tc.allowed && (e != ErrDenied || v != 0 || u != "") {
					t.Fatal(tc.p, v, u, e)
				}
			}
			// Guessed and nonexistent ids look the same as denied.
			for _, id := range []string{randomID(), SnapshotBlobID, f.ID[:31]} {
				if v, u, e := admin.Snapshot(ctx, id); e != ErrDenied || v != 0 || u != "" {
					t.Fatal(id, v, u, e)
				}
			}
			if store.puts.Load() != 1 {
				t.Fatal("builds", store.puts.Load())
			}
			if e := admin.DeleteFolder(ctx, f.ID); e != nil {
				t.Fatal(e)
			}
			if _, _, e := admin.Snapshot(ctx, f.ID); e != ErrDenied {
				t.Fatal(e)
			}
		})
	}
}

func TestSnapshotRebuildCountsRowsNotVersions(t *testing.T) {
	s, store := snapshotServer(t, 3)
	c := s.Client(owner)
	f, k := sharedFor(t, c)
	ctx := context.Background()
	rows := putN(t, c, f, k, "r", 8)
	// Seed enough distinct retained rows to exercise the fixed 1,000-row
	// refresh floor without 1,000 network uploads.
	if e := s.Meta.Transaction(ctx, func(m *Metadata) error {
		for n := 8; n < 1007; n++ {
			row := rows[0]
			row.PathID = fmt.Sprintf("%064x", n)
			m.Files[f.ID][row.PathID] = row
			rows = append(rows, row)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	v1, _, e := c.Snapshot(ctx, f.ID)
	if e != nil || v1 == 0 {
		t.Fatal(v1, e)
	}
	// Two changed rows across two commits stay below the fixed refresh floor.
	put(t, c, f, k, "r0", rows[0].Version, []byte("changed"))
	put(t, c, f, k, "r1", rows[1].Version, []byte("changed"))
	v, u, e := c.Snapshot(ctx, f.ID)
	if e != nil || v != v1 || u == "" || store.puts.Load() != 1 {
		t.Fatal("rebuilt early", v, v1, store.puts.Load(), e)
	}
	// Each commit advances the version once even though many rows change.
	for start := 5; start < 1003; start += 256 {
		deleteRows(t, c, f, rows[start:min(start+256, 1003)])
	}
	if v, _, e := c.Snapshot(ctx, f.ID); e != nil || v != v1 || store.puts.Load() != 1 {
		t.Fatal("rebuilt at exactly 1000 changes", v, e)
	}
	deleteRows(t, c, f, rows[1003:])
	v2, u, e := c.Snapshot(ctx, f.ID)
	if e != nil || v2 <= v1 || store.puts.Load() != 2 {
		t.Fatal("not rebuilt", v2, v1, store.puts.Load(), e)
	}
	if d := store.fetch(t, f.ID, v2, u); len(d.Rows) != 5 {
		t.Fatal(len(d.Rows))
	}
	// A new authority recovers the stored version from the header.
	fresh := NewServer(s.Meta, store)
	fresh.SnapshotRows = 3
	if v, _, e := fresh.Client(owner).Snapshot(ctx, f.ID); e != nil || v != v2 || store.puts.Load() != 2 {
		t.Fatal("restart rebuilt", v, v2, store.puts.Load(), e)
	}
	if v, _, e := NewServer(s.Meta, store).Client(owner).Snapshot(ctx, f.ID); e != nil || v != 0 || !store.stored(f.ID) {
		t.Fatal("default threshold", v, e)
	}
	s.SnapshotRows = 6
	if v, _, e := c.Snapshot(ctx, f.ID); e != nil || v != 0 || store.stored(f.ID) {
		t.Fatal("below threshold kept snapshot", v, e)
	}
}

func TestSnapshotRecoversMissingBlob(t *testing.T) {
	s, store := snapshotServer(t, 2)
	c := s.Client(owner)
	f, k := sharedFor(t, c)
	ctx := context.Background()
	putN(t, c, f, k, "m", 2)
	v, _, e := c.Snapshot(ctx, f.ID)
	if e != nil || v == 0 {
		t.Fatal(v, e)
	}
	if e := store.BlobStore.Delete(ctx, f.ID, SnapshotBlobID); e != nil {
		t.Fatal(e)
	}
	s.cacheSnapshot(f.ID, snapshotInfo{}, false)
	if v2, u, e := c.Snapshot(ctx, f.ID); e != nil || v2 != v || u == "" || !store.stored(f.ID) || store.puts.Load() != 2 {
		t.Fatal(v2, e, store.puts.Load())
	}
}

func TestSnapshotCoalescesBuilds(t *testing.T) {
	s, store := snapshotServer(t, 2)
	c := s.Client(owner)
	f, k := sharedFor(t, c)
	putN(t, c, f, k, "c", 3)
	store.entered = make(chan struct{}, 16)
	store.gate = make(chan struct{})
	ctx := context.Background()
	var wg sync.WaitGroup
	versions := make([]uint64, 8)
	errs := make([]error, 8)
	for i := range versions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			versions[i], _, errs[i] = c.Snapshot(ctx, f.ID)
		}()
	}
	<-store.entered
	// A cancelled waiter returns without stopping the shared build.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, e := c.Snapshot(cancelled, f.ID); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	time.Sleep(20 * time.Millisecond)
	close(store.gate)
	wg.Wait()
	for i := range versions {
		if errs[i] != nil || versions[i] == 0 || versions[i] != versions[0] {
			t.Fatal(i, versions[i], errs[i])
		}
	}
	if store.puts.Load() != 1 {
		t.Fatal("builds", store.puts.Load())
	}
}

func TestSnapshotFallbacks(t *testing.T) {
	ctx := context.Background()
	t.Run("no signer", func(t *testing.T) {
		s, _ := testServer(t)
		s.SnapshotRows = 1
		c := s.Client(owner)
		f, k := sharedFor(t, c)
		putN(t, c, f, k, "n", 2)
		if v, u, e := c.Snapshot(ctx, f.ID); v != 0 || u != "" || e != nil {
			t.Fatal(v, u, e)
		}
		if _, e := s.Blobs.Size(ctx, f.ID, SnapshotBlobID); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("built without a signer", e)
		}
		if _, _, e := s.Client(Principal{"tenant", "stranger"}).Snapshot(ctx, f.ID); e != ErrDenied {
			t.Fatal(e)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		s, store := snapshotServer(t, -1)
		c := s.Client(owner)
		f, k := sharedFor(t, c)
		putN(t, c, f, k, "d", 2)
		if v, u, e := c.Snapshot(ctx, f.ID); v != 0 || u != "" || e != nil || store.puts.Load() != 0 {
			t.Fatal(v, u, e)
		}
	})
	for _, mode := range []string{"sign", "put"} {
		for _, httpMode := range []bool{false, true} {
			t.Run(fmt.Sprint(mode, httpMode), func(t *testing.T) {
				s, store := snapshotServer(t, 1)
				c := clients(t, s, httpMode)(owner)
				f, k := sharedFor(t, c)
				putN(t, c, f, k, "e", 2)
				store.signErr.Store(mode == "sign")
				store.putErr.Store(mode == "put")
				if v, u, e := c.Snapshot(ctx, f.ID); v != 0 || u != "" || e != nil {
					t.Fatal(v, u, e)
				}
				store.signErr.Store(false)
				store.putErr.Store(false)
				if v, u, e := c.Snapshot(ctx, f.ID); v == 0 || u == "" || e != nil {
					t.Fatal(v, u, e)
				}
			})
		}
	}
}

func TestSnapshotRevokedDuringBuild(t *testing.T) {
	s, store := snapshotServer(t, 2)
	admin := s.Client(owner)
	f, k := sharedFor(t, admin)
	putN(t, admin, f, k, "v", 2)
	ctx := context.Background()
	reader := Principal{"tenant", "reader"}
	if e := admin.Grant(ctx, f.ID, reader, Reader); e != nil {
		t.Fatal(e)
	}
	store.entered = make(chan struct{}, 1)
	store.gate = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		v, u, e := s.Client(reader).Snapshot(ctx, f.ID)
		if e == nil && (v != 0 || u != "") {
			e = errors.New("issued after revocation")
		}
		done <- e
	}()
	<-store.entered
	if e := admin.Revoke(ctx, f.ID, reader); e != nil {
		t.Fatal(e)
	}
	close(store.gate)
	if e := <-done; e != ErrDenied {
		t.Fatal(e)
	}
	// The build itself is still good for principals that keep access.
	if v, u, e := admin.Snapshot(ctx, f.ID); e != nil || v == 0 || u == "" || store.puts.Load() != 1 {
		t.Fatal(v, u, e, store.puts.Load())
	}
}

func TestSnapshotDeletedWithFolder(t *testing.T) {
	collect := func(t *testing.T, s *Server, id string) {
		t.Helper()
		for i := 0; i < 20; i++ {
			if e := s.CollectGarbage(context.Background()); e != nil {
				t.Fatal(e)
			}
			if st, e := s.folderSnapshotState(context.Background(), id); e != nil {
				t.Fatal(e)
			} else if st.gone && st.version == 0 {
				return
			}
		}
		t.Fatal("folder record kept")
	}
	ctx := context.Background()
	t.Run("stored", func(t *testing.T) {
		s, store := snapshotServer(t, 2)
		c := s.Client(owner)
		f, k := sharedFor(t, c)
		putN(t, c, f, k, "x", 2)
		if v, _, e := c.Snapshot(ctx, f.ID); e != nil || v == 0 || !store.stored(f.ID) {
			t.Fatal(v, e)
		}
		if e := c.DeleteFolder(ctx, f.ID); e != nil {
			t.Fatal(e)
		}
		collect(t, s, f.ID)
		if store.stored(f.ID) {
			t.Fatal("snapshot outlived its folder")
		}
	})
	t.Run("publishing", func(t *testing.T) {
		s, store := snapshotServer(t, 2)
		c := s.Client(owner)
		f, k := sharedFor(t, c)
		putN(t, c, f, k, "y", 2)
		store.entered = make(chan struct{}, 1)
		store.gate = make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, u, e := c.Snapshot(ctx, f.ID)
			if u != "" {
				e = errors.New("issued after deletion")
			}
			done <- e
		}()
		<-store.entered
		if e := c.DeleteFolder(ctx, f.ID); e != nil {
			t.Fatal(e)
		}
		collect(t, s, f.ID)
		close(store.gate)
		if e := <-done; e != ErrDenied {
			t.Fatal(e)
		}
		if store.stored(f.ID) {
			t.Fatal("publication after deletion left an orphan")
		}
	})
}

func TestSnapshotDecoderRejectsTampering(t *testing.T) {
	folder := randomID()
	row := func(i int, v uint64) Row {
		return Row{PathID: fmt.Sprintf("%064x", i), BlobID: randomID(), Version: v, SealedSize: int64(i), Metadata: bytes.Repeat([]byte{byte(i)}, 40)}
	}
	encode := func(t *testing.T, f string, version, horizon uint64, rows ...Row) []byte {
		t.Helper()
		var b bytes.Buffer
		enc := newSnapshotEncoder(&b, f)
		if e := enc.header(version, horizon); e != nil {
			t.Fatal(e)
		}
		for _, r := range rows {
			if e := enc.row(r); e != nil {
				t.Fatal(e)
			}
		}
		if e := enc.close(); e != nil {
			t.Fatal(e)
		}
		return b.Bytes()
	}
	good := encode(t, folder, 9, 2, row(1, 3), row(2, 9), row(5, 1))
	d, e := DecodeSnapshot(bytes.NewReader(good), folder, 9)
	if e != nil || !d.Full || d.Version != 9 || d.Horizon != 2 || len(d.Rows) != 3 || d.Rows[1].Version != 9 || d.Rows[2].FolderID != folder {
		t.Fatal(d, e)
	}
	empty := encode(t, folder, 4, 0)
	if d, e := DecodeSnapshot(bytes.NewReader(empty), folder, 4); e != nil || len(d.Rows) != 0 {
		t.Fatal(d, e)
	}
	integrity := func(name string, b []byte, f string, v uint64) {
		t.Helper()
		if _, e := DecodeSnapshot(bytes.NewReader(b), f, v); !errors.Is(e, ErrIntegrity) {
			t.Fatalf("%s: %v", name, e)
		}
	}
	integrity("other folder", good, randomID(), 9)
	integrity("other version", good, folder, 8)
	integrity("zero version", encode(t, folder, 0, 0), folder, 0)
	integrity("trailing", append(append([]byte(nil), good...), 0), folder, 9)
	for n := 0; n < len(good); n++ {
		integrity(fmt.Sprint("truncated ", n), good[:n], folder, 9)
	}
	for i := range good {
		flipped := append([]byte(nil), good...)
		flipped[i] ^= 0x40
		// A flip in deflate padding may decode to the same rows; anything
		// else fails the framing, bounds or zlib checksum.
		if got, e := DecodeSnapshot(bytes.NewReader(flipped), folder, 9); e == nil && (i < snapshotHeaderLen || !reflect.DeepEqual(got, d)) {
			t.Fatalf("flip %d accepted: %v", i, got)
		}
	}
	// Hand-built bodies the encoder refuses: future rows, order, count.
	raw := func(version uint64, body []byte) []byte {
		var b bytes.Buffer
		enc := newSnapshotEncoder(&b, folder)
		if e := enc.header(version, 0); e != nil {
			t.Fatal(e)
		}
		enc.z.Write(body)
		enc.z.Close()
		enc.out.Flush()
		return b.Bytes()
	}
	record := func(r Row) []byte {
		path, _ := hex.DecodeString(r.PathID)
		blob, _ := hex.DecodeString(r.BlobID)
		out := append([]byte{1}, path...)
		out = append(out, blob...)
		out = appendUvarints(out, r.Version, uint64(r.SealedSize), uint64(len(r.Metadata)))
		return append(out, r.Metadata...)
	}
	integrity("future row", raw(5, append(record(row(1, 6)), 0, 1)), folder, 5)
	integrity("zero row version", raw(5, append(record(row(1, 0)), 0, 1)), folder, 5)
	integrity("unordered", raw(5, append(append(record(row(2, 1)), record(row(1, 1))...), 0, 2)), folder, 5)
	integrity("duplicate", raw(5, append(append(record(row(1, 1)), record(row(1, 1))...), 0, 2)), folder, 5)
	integrity("count", raw(5, append(record(row(1, 1)), 0, 2)), folder, 5)
	integrity("tag", raw(5, []byte{7}), folder, 5)
	integrity("metadata bound", raw(5, append(append([]byte{1}, make([]byte, 48)...), 1, 1, 0x81, 0x80, 0x01)), folder, 5)
	// Reader errors are reported as themselves.
	cause := errors.New("network")
	if _, e := DecodeSnapshot(io.MultiReader(bytes.NewReader(good[:50]), errReader{cause}), folder, 9); !errors.Is(e, cause) {
		t.Fatal(e)
	}
	if _, e := DecodeSnapshot(bytes.NewReader(good), "snapshot", 9); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}

type errReader struct{ e error }

func (r errReader) Read([]byte) (int, error) { return 0, r.e }

func appendUvarints(b []byte, values ...uint64) []byte {
	for _, v := range values {
		for v >= 0x80 {
			b = append(b, byte(v)|0x80)
			v >>= 7
		}
		b = append(b, byte(v))
	}
	return b
}

func TestSnapshotDirectoryBlobStore(t *testing.T) {
	ctx := context.Background()
	blobs, e := OpenDirectoryBlobStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { blobs.Close() })
	store := &snapshotURLStore{BlobStore: blobs}
	s := NewServer(openMeta(t), store)
	s.SnapshotRows = 2
	c := clients(t, s, true)(owner)
	f, k := sharedFor(t, c)
	putN(t, c, f, k, "d", 3)
	v, u, e := c.Snapshot(ctx, f.ID)
	if e != nil || v == 0 {
		t.Fatal(v, e)
	}
	if d := store.fetch(t, f.ID, v, u); len(d.Rows) != 3 {
		t.Fatal(len(d.Rows))
	}
	if _, e := c.Download(ctx, f.ID, SnapshotBlobID); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	if e := c.DeleteFolder(ctx, f.ID); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20 && store.stored(f.ID); i++ {
		if e := s.CollectGarbage(ctx); e != nil {
			t.Fatal(e)
		}
	}
	if store.stored(f.ID) {
		t.Fatal("snapshot outlived its folder")
	}
}
