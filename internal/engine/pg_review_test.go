package engine

// Pre-release review tests for the multi-process Postgres store. Every test
// here skips unless DRIVESYNC_STORE=postgres. A failing test is a finding.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pnegahdar/drivesync/internal/embedpg"
)

type reviewClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *reviewClock) now() time.Time         { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *reviewClock) set(t time.Time)        { c.mu.Lock(); c.t = t; c.mu.Unlock() }
func newReviewClock(t time.Time) *reviewClock { return &reviewClock{t: t} }

// gatedDeletes pauses Delete of one blob until release is closed.
type gatedDeletes struct {
	*MemoryBlobStore
	blob    string
	hit     chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedDeletes) Delete(ctx context.Context, f, id string) error {
	if id == g.blob {
		g.once.Do(func() { close(g.hit) })
		<-g.release
	}
	return g.MemoryBlobStore.Delete(ctx, f, id)
}

func reviewSkip(t *testing.T) {
	t.Helper()
	if !postgresMode() {
		t.Skip("Postgres review test")
	}
}

// holdFolderLock takes the same transaction-scoped advisory lock that
// transactionOnce takes for folder, so the test can order competing writers.
func holdFolderLock(t *testing.T, db *sql.DB, schema string, class int32, key string) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("SELECT pg_advisory_xact_lock($1, hashtext($2))", class, advisoryIdentity(schema, key)); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	return tx
}

func awaitAdvisoryWaiters(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got int
		if err := db.QueryRow("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d advisory waiters, want %d", got, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func openReviewStores(t *testing.T, n int) ([]*PostgresMetaStore, *sql.DB, string) {
	t.Helper()
	db := embedpg.SharedDB(t)
	schema := embedpg.NewSchema()
	var out []*PostgresMetaStore
	for i := 0; i < n; i++ {
		m, err := OpenPostgresMetaStore(context.Background(), db, schema)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	t.Cleanup(func() {
		for _, m := range out {
			m.Close()
		}
		embedpg.DropSchema(db, schema)
	})
	return out, db, schema
}

func uploadedTicket(t *testing.T, s *Server, p Principal, f Folder, k FolderKey, name string, data []byte) (Ticket, []byte) {
	t.Helper()
	pid, e := PathID(k, f.ID, name)
	if e != nil {
		t.Fatal(e)
	}
	ticket, e := s.Reserve(context.Background(), p, f.ID, UploadRequest{PathID: pid, SealedSize: SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Upload(context.Background(), p, f.ID, ticket, bytes.NewReader(sealedBody(t, k, f, pid, ticket.BlobID, data))); e != nil {
		t.Fatal(e)
	}
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: name, BlobID: ticket.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hashBytes(data)})
	if e != nil {
		t.Fatal(e)
	}
	return ticket, meta
}

// A collect pass retires a ticket whose lease looks expired. The retire
// transaction loses a serialization race to the uploader's own renewal, so
// the retry sees a live lease and keeps the ticket. The first attempt already
// added the blob to the in-memory pending set, and collectLoadedGarbage then
// deletes the blob. If the uploader commits before that delete, a committed
// file loses its content.
func TestCollectKeepsBlobRenewedBeforeDelete(t *testing.T) {
	reviewSkip(t)
	stores, db, schema := openReviewStores(t, 2)
	mem := NewMemoryBlobStore()
	gate := &gatedDeletes{MemoryBlobStore: mem, hit: make(chan struct{}), release: make(chan struct{})}
	base := time.Now()
	ttl := 5 * time.Minute
	bClock, gClock := newReviewClock(base), newReviewClock(base.Add(2*ttl))
	uploader := NewServer(stores[0], mem)
	uploader.Now = bClock.now
	collector := NewServer(stores[1], gate)
	collector.Now = gClock.now

	c := uploader.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	data := []byte("committed content")
	ticket, goOn, uploaded := holdUpload(t, uploader, owner, f, k, "victim", data)
	gate.blob = ticket.BlobID

	// The uploader stalled past its lease (from the collector's view), and
	// its stream resumes: the next read renews the lease.
	bClock.set(base.Add(ttl * 16 / 10))
	x := holdFolderLock(t, db, schema, lockFolder, f.ID)
	close(goOn)
	awaitAdvisoryWaiters(t, db, 1) // the renewal queues first
	gcDone := make(chan error, 1)
	go func() { gcDone <- collector.CollectGarbage(context.Background()) }()
	awaitAdvisoryWaiters(t, db, 2) // then the retire transaction, snapshot already taken
	x.Rollback()

	if e := <-uploaded; e != nil {
		t.Fatalf("upload: %v", e)
	}
	select {
	case <-gate.hit:
		t.Log("collector is deleting the blob of a ticket it did not retire")
	case e := <-gcDone:
		gcDone <- e
	case <-time.After(5 * time.Second):
		t.Fatal("collector neither finished nor deleted")
	}
	pid, _ := PathID(k, f.ID, "victim")
	meta, e := SealMetadata(k, f.ID, pid, FileMetadata{Path: "victim", BlobID: ticket.BlobID, Size: int64(len(data)), Mode: 0600, Hash: hashBytes(data)})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Commit(context.Background(), f.ID, []Mutation{{PathID: pid, TicketID: ticket.ID, Metadata: meta}}); e != nil {
		t.Fatalf("commit: %v", e)
	}
	close(gate.release)
	if e = <-gcDone; e != nil {
		t.Logf("collect: %v", e)
	}
	r, e := c.Download(context.Background(), f.ID, ticket.BlobID)
	if e != nil {
		t.Fatalf("committed file lost its blob after a collect pass: %v", e)
	}
	raw, e := io.ReadAll(r)
	r.Close()
	if e != nil || int64(len(raw)) != ticket.SealedSize {
		t.Fatalf("committed blob %d bytes: %v", len(raw), e)
	}
}

// Commit appends to the Delta it returns from inside the transaction
// callback. A serialization retry runs the callback again without resetting
// it, so the caller receives the first attempt's uncommitted rows as well.
func TestCommitRetryReturnsOnlyTheCommittedRow(t *testing.T) {
	reviewSkip(t)
	stores, db, schema := openReviewStores(t, 2)
	blobs := NewMemoryBlobStore()
	sa, sb := NewServer(stores[0], blobs), NewServer(stores[1], blobs)
	f, k := folderFor(t, sa.Client(owner), Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	ta, ma := uploadedTicket(t, sa, owner, f, k, "a", []byte("a"))
	tb, mb := uploadedTicket(t, sb, owner, f, k, "b", []byte("b"))

	x := holdFolderLock(t, db, schema, lockFolder, f.ID)
	first := make(chan error, 1)
	go func() {
		_, e := sb.Commit(context.Background(), owner, f.ID, []Mutation{{PathID: tb.PathID, TicketID: tb.ID, Metadata: mb}})
		first <- e
	}()
	awaitAdvisoryWaiters(t, db, 1)
	type result struct {
		d Delta
		e error
	}
	second := make(chan result, 1)
	go func() {
		d, e := sa.Commit(context.Background(), owner, f.ID, []Mutation{{PathID: ta.PathID, TicketID: ta.ID, Metadata: ma}})
		second <- result{d, e}
	}()
	awaitAdvisoryWaiters(t, db, 2)
	x.Rollback()
	if e := <-first; e != nil {
		t.Fatal(e)
	}
	got := <-second
	if got.e != nil {
		t.Fatal(got.e)
	}
	if len(got.d.Rows) != 1 {
		var versions []uint64
		for _, r := range got.d.Rows {
			versions = append(versions, r.Version)
		}
		t.Fatalf("one mutation returned %d rows, versions %v, delta version %d", len(got.d.Rows), versions, got.d.Version)
	}
}

// Advisory locks are taken after the SERIALIZABLE snapshot exists. Every
// transaction that waited for the lock therefore reads stale state and fails
// with 40001 once it writes, so concurrent writers to one folder exhaust the
// eight attempts and the raw Postgres error reaches the caller.
func TestContendedFolderReservationsAllSucceed(t *testing.T) {
	reviewSkip(t)
	for _, procs := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d processes", procs), func(t *testing.T) {
			stores, _, _ := openReviewStores(t, procs)
			blobs := NewMemoryBlobStore()
			var servers []*Server
			for _, m := range stores {
				servers = append(servers, NewServer(m, blobs))
			}
			f, _ := folderFor(t, servers[0].Client(owner), Limits{MaxTotalBytes: 1 << 40, MaxFileBytes: 1 << 20, MaxRows: 1 << 20, MaxFiles: 1 << 20})
			const writers = 64
			var wg sync.WaitGroup
			var failed atomic.Int32
			var mu sync.Mutex
			sample := map[string]int{}
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, e := servers[i%len(servers)].Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: fmt.Sprintf("%064x", i+1), SealedSize: 1})
					if e != nil {
						failed.Add(1)
						mu.Lock()
						sample[e.Error()]++
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			if failed.Load() > 0 {
				t.Fatalf("%d of %d concurrent reservations on one folder failed: %v", failed.Load(), writers, sample)
			}
		})
	}
}

// validSchema accepts 63-byte names, but the NOTIFY channel is
// "drivesync_"+schema and Postgres rejects channel names of 64 bytes or more.
func TestLongSchemaNameStillAcceptsWrites(t *testing.T) {
	reviewSkip(t)
	db := embedpg.SharedDB(t)
	schema := embedpg.NewSchema()
	schema += strings.Repeat("x", 60-len(schema))
	if !validSchema(schema) {
		t.Fatalf("%q rejected", schema)
	}
	m, err := OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		t.Fatalf("open accepted schema: %v", err)
	}
	t.Cleanup(func() { m.Close(); embedpg.DropSchema(db, schema) })
	s := NewServer(m, NewMemoryBlobStore())
	k := NewFolderKey()
	if _, e := CreateFolder(context.Background(), s.Client(owner), FolderSpec{Name: "x"}, k); e != nil {
		t.Fatalf("first write on a %d-byte schema: %v", len(schema), e)
	}
}

// LISTEN keeps one pooled connection for the life of the store. A pool of one
// or two cannot serve that session and still run a maintenance transaction
// beside an ordinary read, so Open rejects it. A pool of three must collect
// without stalling a concurrent read.
func TestPoolOfTwoIsRejected(t *testing.T) {
	reviewSkip(t)
	shared := embedpg.SharedDB(t)
	var port int
	if err := shared.QueryRow("SELECT inet_server_port()").Scan(&port); err != nil {
		t.Fatal(err)
	}
	open := func(n int) (*sql.DB, error) {
		db, err := sql.Open("pgx", fmt.Sprintf("postgres://drivesync:drivesync@localhost:%d/drivesync?sslmode=disable", port))
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(n)
		t.Cleanup(func() { db.Close() })
		return db, nil
	}
	small, err := open(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenPostgresMetaStore(context.Background(), small, embedpg.NewSchema()); err == nil || !strings.Contains(err.Error(), "at least 3") {
		t.Fatalf("pool of 2: %v", err)
	}
	db, err := open(3)
	if err != nil {
		t.Fatal(err)
	}
	schema := embedpg.NewSchema()
	m, err := OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(); embedpg.DropSchema(shared, schema) })
	s := NewServer(m, NewMemoryBlobStore())
	f, _ := folderFor(t, s.Client(owner), Limits{})
	gc := make(chan error, 1)
	go func() { gc <- s.CollectGarbage(context.Background()) }()
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	_, e := s.GetFolder(context.Background(), owner, f.ID)
	took := time.Since(start)
	gcErr := <-gc
	if e != nil || took > time.Second || gcErr != nil {
		t.Fatalf("GetFolder during collect: %v after %s; collect: %v", e, took.Round(time.Millisecond), gcErr)
	}
}

// A dropped LISTEN session reconnects and wakes every waiter once, so a
// commit made while the listener was down is not missed.
func TestDroppedListenerWakesWaiters(t *testing.T) {
	reviewSkip(t)
	stores, db, _ := openReviewStores(t, 2)
	blobs := NewMemoryBlobStore()
	sa, sb := NewServer(stores[0], blobs), NewServer(stores[1], blobs)
	f, k := folderFor(t, sa.Client(owner), Limits{})
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	woke := make(chan error, 1)
	go func() { _, e := sa.Wait(ctx, owner, f.ID, 0); woke <- e }()
	time.Sleep(100 * time.Millisecond)
	if _, err := db.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query = $1`, "LISTEN "+stores[0].channel); err != nil {
		t.Fatal(err)
	}
	put(t, sb.Client(owner), f, k, "n", 0, []byte("x"))
	select {
	case e := <-woke:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter not woken after the LISTEN session dropped")
	}
}

// A frozen or partitioned authority that holds an owner's advisory lock
// blocks writes to every folder of that owner on every other authority. No
// lock_timeout or idle_in_transaction_session_timeout bounds it.
func TestIdleOwnerLockDoesNotBlockOtherFolders(t *testing.T) {
	reviewSkip(t)
	stores, db, schema := openReviewStores(t, 1)
	s := NewServer(stores[0], NewMemoryBlobStore())
	a, _ := folderFor(t, s.Client(owner), Limits{})
	b, _ := folderFor(t, s.Client(owner), Limits{})
	// Another authority began a write on folder a and then stopped running
	// (SIGSTOP, VM pause, network partition): its backend keeps the locks.
	x := holdFolderLock(t, db, schema, lockFolder, a.ID)
	if _, err := x.Exec("SELECT pg_advisory_xact_lock($1, hashtext($2))", lockOwner, advisoryIdentity(schema, principalKey(owner))); err != nil {
		t.Fatal(err)
	}
	defer x.Rollback()
	start := time.Now()
	_, e := s.Reserve(context.Background(), owner, b.ID, UploadRequest{PathID: fmt.Sprintf("%064x", 1), SealedSize: 1})
	if e != nil {
		t.Fatalf("write to another folder of the same owner: %v after %s", e, time.Since(start).Round(time.Millisecond))
	}
}

// Names are a real column. NUL and invalid UTF-8 are rejected on both stores.
// U+0001 and the literal escape strings stay distinct, so they do not collide.
func TestFolderNamesRejectNULAndStayDistinct(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	for _, name := range []string{"n\x00", "n\xff"} {
		if _, e := CreateFolder(context.Background(), c, FolderSpec{Name: name}, NewFolderKey()); e == nil {
			t.Errorf("create %q succeeded", name)
		}
	}
	for _, name := range []string{"n\x01", `n\u0000`, `n\u0001`} {
		if _, e := CreateFolder(context.Background(), c, FolderSpec{Name: name}, NewFolderKey()); e != nil {
			t.Errorf("create %q: %v", name, e)
		}
	}
	list, e := s.ListFolders(context.Background(), owner)
	if e != nil {
		t.Fatal(e)
	}
	if len(list) != 3 {
		t.Fatalf("%d folders, want 3", len(list))
	}
}

var _ = errors.Is

// Private folders of one owner share the owner's advisory lock and its
// Account row, so writes to different folders contend the same way.
func TestOwnerReservationsAcrossFoldersAllSucceed(t *testing.T) {
	reviewSkip(t)
	stores, _, _ := openReviewStores(t, 3)
	blobs := NewMemoryBlobStore()
	var servers []*Server
	for _, m := range stores {
		servers = append(servers, NewServer(m, blobs))
	}
	const writers = 64
	var folders []Folder
	for i := 0; i < writers; i++ {
		f, _ := folderFor(t, servers[0].Client(owner), Limits{})
		folders = append(folders, f)
	}
	var wg sync.WaitGroup
	var failed atomic.Int32
	var mu sync.Mutex
	sample := map[string]int{}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := servers[i%len(servers)].Reserve(context.Background(), owner, folders[i].ID, UploadRequest{PathID: fmt.Sprintf("%064x", i+1), SealedSize: 1})
			if e != nil {
				failed.Add(1)
				mu.Lock()
				sample[e.Error()]++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if failed.Load() > 0 {
		t.Fatalf("%d of %d reservations on distinct folders of one owner failed: %v", failed.Load(), writers, sample)
	}
}

// Owner quota is shared by every private folder. Reservations racing on
// distinct folders through three authorities must accept exactly the budget,
// and the durable counter must equal the folders' usage afterwards.
func TestOwnerQuotaAcrossFoldersStaysExact(t *testing.T) {
	reviewSkip(t)
	stores, _, _ := openReviewStores(t, 3)
	blobs := NewMemoryBlobStore()
	const allow = 9
	policy := QuotaFunc(func(context.Context, Principal) (Quota, error) {
		return Quota{MaxTotalBytes: allow * (RowCost + 1), MaxFiles: 1 << 20}, nil
	})
	var servers []*Server
	for _, m := range stores {
		s := NewServer(m, blobs)
		s.Quotas = policy
		servers = append(servers, s)
	}
	var folders []Folder
	for i := 0; i < 6; i++ {
		f, _ := folderFor(t, servers[0].Client(owner), Limits{})
		folders = append(folders, f)
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	var mu sync.Mutex
	var bad []error
	for round := 0; round < 40; round++ {
		for i := 0; i < 60; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				s := servers[i%len(servers)]
				f := folders[i%len(folders)]
				_, e := s.Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: fmt.Sprintf("%032x%032x", round, i+1), SealedSize: 1})
				var l *LimitError
				switch {
				case e == nil:
					accepted.Add(1)
				case errors.As(e, &l):
				default:
					// Serialization exhaustion is reported separately.
					if !serializationFailure(e) {
						mu.Lock()
						bad = append(bad, e)
						mu.Unlock()
					}
				}
			}(i)
		}
		wg.Wait()
		if accepted.Load() >= allow {
			break
		}
	}
	if len(bad) > 0 {
		t.Fatal(bad)
	}
	if accepted.Load() != allow {
		t.Fatalf("accepted %d reservations, owner budget %d", accepted.Load(), allow)
	}
	var sum int64
	var account Account
	if e := servers[1].Meta.Transaction(context.Background(), func(m *Metadata) error {
		for _, f := range folders {
			u := usage(m, f.ID)
			sum += sat(u.Bytes, u.Reserved)
		}
		account = m.Accounts[principalKey(owner)]
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if sum != allow*(RowCost+1) || account.Bytes != sum {
		t.Fatalf("folder usage %d, owner counter %+v, want %d", sum, account, allow*(RowCost+1))
	}
}

// bulkRows inserts n live file rows at version 1 into folder f.
func bulkRows(t *testing.T, m testStore, f string, n int) {
	t.Helper()
	switch s := m.(type) {
	case *PostgresMetaStore:
		if _, err := s.db.Exec(`INSERT INTO "`+s.schema+`".files(folder,path,data) SELECT $1, lpad(to_hex(g),64,'0'), convert_to('{"FolderID":"'||$1||'","PathID":"'||lpad(to_hex(g),64,'0')||'","BlobID":"'||md5(g::text)||'","Version":1,"SealedSize":100,"Metadata":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","Deleted":false,"DeletedAt":0}','UTF8') FROM generate_series(1,$2::int) g`, f, n); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`ANALYZE "` + s.schema + `".files`); err != nil {
			t.Fatal(err)
		}
	case *SQLiteMetaStore:
		tx, err := s.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		stmt, err := tx.Prepare("INSERT INTO files(folder,path,data) VALUES(?,?,?)")
		if err != nil {
			t.Fatal(err)
		}
		for g := 1; g <= n; g++ {
			pid := fmt.Sprintf("%064x", g)
			data := fmt.Sprintf(`{"FolderID":"%s","PathID":"%s","BlobID":"%032x","Version":1,"SealedSize":100,"Metadata":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","Deleted":false,"DeletedAt":0}`, f, pid, g)
			if _, err = stmt.Exec(f, pid, []byte(data)); err != nil {
				t.Fatal(err)
			}
		}
		stmt.Close()
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// Per-chunk download authorization and change pages read the folder's whole
// file table into Go on Postgres; SQLite answers both from an index. A
// 64 KiB read and a 512-row page therefore cost O(folder rows).
func TestLargeFolderChunkCheckAndPullStayBounded(t *testing.T) {
	s, m := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	data := bytes.Repeat([]byte("z"), 1<<20)
	row := put(t, c, f, k, "big", 0, data)
	const n = 50000
	bulkRows(t, m, f.ID, n)

	start := time.Now()
	const calls = 20
	for i := 0; i < calls; i++ {
		if _, e := m.authorize(context.Background(), owner, f.ID, false, "", row.BlobID, time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	perCheck := time.Since(start) / calls

	start = time.Now()
	d, e := s.Changes(context.Background(), owner, f.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	pull := time.Since(start)
	t.Logf("%T: %d rows; blob check %s per chunk read, full pull of %d rows %s", m, n, perCheck, len(d.Rows), pull.Round(time.Millisecond))
	if perCheck > 5*time.Millisecond || pull > 3*time.Second {
		t.Fatalf("blob check %s per chunk, full pull %s for a %d-row folder", perCheck, pull.Round(time.Millisecond), n)
	}
}

// One maintenance pass on Postgres reads every tenant's file rows twice
// (tombstone selection and compaction) inside a five-second budget. SQLite
// selects tombstones by index. Once the whole table outgrows that budget the
// pass fails before it frees tombstone blobs or purges deleted folders.
func TestGCPassIgnoresOtherTenantsRows(t *testing.T) {
	s, m := testServer(t)
	other := Principal{Tenant: "other", Subject: "owner"}
	big, _ := folderFor(t, s.Client(other), Limits{})
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	doomed := put(t, c, f, k, "doomed", 0, []byte("x"))
	if _, e := c.Commit(context.Background(), f.ID, []Mutation{{PathID: doomed.PathID, BaseVersion: doomed.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	for _, n := range []int{0, 100000} {
		if n > 0 {
			bulkRows(t, m, big.ID, n)
		}
		start := time.Now()
		if e := s.CollectGarbage(context.Background()); e != nil {
			t.Fatal(e)
		}
		t.Logf("%T: pass with %d foreign rows took %s", m, n, time.Since(start).Round(time.Millisecond))
		if n > 0 && time.Since(start) > 100*time.Millisecond {
			t.Fatalf("a pass with %d other-tenant rows took %s", n, time.Since(start).Round(time.Millisecond))
		}
	}
}

// Killing every backend approximates a Postgres restart from the client side.
// Waiters must return, writes must recover, and maintenance must run again.
func TestWritesAndGCRecoverAfterBackendsDie(t *testing.T) {
	reviewSkip(t)
	stores, db, _ := openReviewStores(t, 2)
	blobs := NewMemoryBlobStore()
	sa, sb := NewServer(stores[0], blobs), NewServer(stores[1], blobs)
	f, k := folderFor(t, sa.Client(owner), Limits{})
	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	woke := make(chan error, 1)
	go func() { _, e := sa.Wait(ctx, owner, f.ID, 0); woke <- e }()
	time.Sleep(100 * time.Millisecond)
	if _, err := db.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'`); err != nil {
		t.Fatal(err)
	}
	var e error
	for i := 0; i < 3; i++ {
		pid, _ := PathID(k, f.ID, "n")
		if _, e = sb.Reserve(context.Background(), owner, f.ID, UploadRequest{PathID: pid, SealedSize: 1}); e == nil {
			break
		}
		t.Logf("write %d after kill: %v", i, e)
	}
	if e != nil {
		t.Fatalf("writes did not recover: %v", e)
	}
	put(t, sb.Client(owner), f, k, "m", 0, []byte("x"))
	select {
	case e := <-woke:
		t.Logf("waiter returned: %v", e)
	case <-time.After(3 * time.Second):
		t.Fatal("waiter hung after backends were killed")
	}
	if e = sa.CollectGarbage(context.Background()); e != nil {
		t.Logf("first collect after kill: %v", e)
		if e = sa.CollectGarbage(context.Background()); e != nil {
			t.Fatalf("collect did not recover: %v", e)
		}
	}
}

// Writers in different tenants touch disjoint rows, but every folder update
// is non-HOT (the name index is an expression over data) and inserts into the
// shared folders_pkey leaf page that every other writer has predicate-locked.
// SERIALIZABLE then aborts unrelated tenants' transactions against each other.
func TestCrossTenantReservationsDoNotAbort(t *testing.T) {
	reviewSkip(t)
	stores, _, _ := openReviewStores(t, 3)
	blobs := NewMemoryBlobStore()
	var servers []*Server
	for _, m := range stores {
		servers = append(servers, NewServer(m, blobs))
	}
	const tenants = 64
	var folders []Folder
	var owners []Principal
	for i := 0; i < tenants; i++ {
		p := Principal{Tenant: fmt.Sprintf("tenant-%d", i), Subject: "owner"}
		f, _ := folderFor(t, servers[0].Client(p), Limits{})
		folders = append(folders, f)
		owners = append(owners, p)
	}
	var failed atomic.Int32
	var wg sync.WaitGroup
	var mu sync.Mutex
	sample := map[string]int{}
	for round := 0; round < 3; round++ {
		for i := 0; i < tenants; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, e := servers[i%len(servers)].Reserve(context.Background(), owners[i], folders[i].ID, UploadRequest{PathID: fmt.Sprintf("%032x%032x", round, i+1), SealedSize: 1})
				if e != nil {
					failed.Add(1)
					mu.Lock()
					sample[e.Error()]++
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
	}
	if failed.Load() > 0 {
		t.Fatalf("%d of %d single-writer reservations in separate tenants failed: %v", failed.Load(), 3*tenants, sample)
	}
}

// slowPublish reads the whole stream, then waits before publishing, like an
// object store completing a multipart upload.
type slowPublish struct {
	*MemoryBlobStore
	read    chan struct{}
	publish chan struct{}
	once    sync.Once
}

func (s *slowPublish) Put(ctx context.Context, f, id string, r io.Reader) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	s.once.Do(func() { close(s.read) })
	<-s.publish
	return s.MemoryBlobStore.Put(ctx, f, id, bytes.NewReader(data))
}

// An authorization change retires another authority's in-flight upload and
// queues its blob as Writing garbage. The next pass on a different authority
// deletes that garbage row although the writer's lease is still open, and the
// writer then publishes: the blob is stored with no row, ticket, or garbage
// entry referencing it, so it is never collected and never charged.
func TestEpochChangeHonoursAnotherAuthorityLease(t *testing.T) {
	reviewSkip(t)
	stores, _, _ := openReviewStores(t, 2)
	mem := NewMemoryBlobStore()
	slow := &slowPublish{MemoryBlobStore: mem, read: make(chan struct{}), publish: make(chan struct{})}
	uploader, collector := NewServer(stores[0], slow), NewServer(stores[1], mem)
	c := uploader.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	pid, _ := PathID(k, f.ID, "slow")
	data := []byte("published after cleanup")
	ticket, e := c.Reserve(context.Background(), f.ID, UploadRequest{PathID: pid, SealedSize: SealedSize(int64(len(data)))})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		done <- c.Upload(context.Background(), f.ID, ticket, bytes.NewReader(sealedBody(t, k, f, pid, ticket.BlobID, data)))
	}()
	<-slow.read
	if e = c.Grant(context.Background(), f.ID, Principal{Tenant: "tenant", Subject: "reader"}, Reader); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e = collector.CollectGarbage(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	close(slow.publish)
	t.Logf("upload: %v", <-done)
	if _, e = mem.Size(context.Background(), f.ID, ticket.BlobID); e != nil {
		return // nothing was published
	}
	referenced := false
	if e = collector.Meta.Transaction(context.Background(), func(m *Metadata) error {
		_, g := m.Garbage[f.ID+"/"+ticket.BlobID]
		_, tk := m.Tickets[ticket.ID]
		referenced = g || tk
		for _, r := range m.Files[f.ID] {
			referenced = referenced || r.BlobID == ticket.BlobID
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if !referenced {
		for i := 0; i < 3; i++ {
			_ = collector.CollectGarbage(context.Background())
		}
		if _, e = mem.Size(context.Background(), f.ID, ticket.BlobID); e == nil {
			t.Fatal("published blob has no row, ticket, or garbage entry: it is never collected or charged")
		}
	}
}

// The maintenance lock is a session lock with no lease. A frozen or
// partitioned authority that holds it stops collection on every authority:
// each pass returns nil without collecting, so garbage stays charged.
func TestAdvisoryMaintenanceLockDoesNotStopGC(t *testing.T) {
	reviewSkip(t)
	stores, db, schema := openReviewStores(t, 2)
	blobs := NewMemoryBlobStore()
	sa, sb := NewServer(stores[0], blobs), NewServer(stores[1], blobs)
	c := sa.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	row := put(t, c, f, k, "old", 0, []byte("old"))
	put(t, c, f, k, "old", row.Version, []byte("new")) // retires the first blob as garbage
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1, hashtext($2))", lockMaintenance, schema); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock_all()")
	for i := 0; i < 3; i++ {
		for _, s := range []*Server{sa, sb} {
			if e := s.CollectGarbage(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
	}
	got, e := sa.GetFolder(context.Background(), owner, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	if got.Usage.GarbageRows != 0 {
		t.Fatalf("garbage rows %d still charged; every pass skipped silently behind a lock with no lease", got.Usage.GarbageRows)
	}
}

// Run waits for publication.TryLock before its first recovery pass. Any
// upload in this process holds the read side, so a process that always has
// one upload in flight never starts maintenance. Postgres recovery already
// honors this process's live uploads through wakes.live and leases.
func TestRunCollectsWhileAnUploadIsInFlight(t *testing.T) {
	reviewSkip(t)
	stores, _, _ := openReviewStores(t, 1)
	s := NewServer(stores[0], NewMemoryBlobStore())
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{MaxTotalBytes: 1 << 20, MaxFileBytes: 1 << 20, MaxRows: 100})
	row := put(t, c, f, k, "old", 0, []byte("old"))
	put(t, c, f, k, "old", row.Version, []byte("new"))
	_, goOn, done := holdUpload(t, s, owner, f, k, "streaming", []byte("long upload"))
	defer func() { close(goOn); <-done }()
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- s.Run(ctx, 50*time.Millisecond) }()
	defer func() { cancel(); <-ran }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, e := s.GetFolder(context.Background(), owner, f.ID)
		if e != nil {
			t.Fatal(e)
		}
		if got.Usage.GarbageRows == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("Run collected nothing while one upload stayed in flight")
}
