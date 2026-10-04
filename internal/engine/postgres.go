package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// Advisory-lock classes. Folder is always taken before owner so two writers
// cannot deadlock on the same pair. lockMaintenance is the old session-lock
// class. Maintenance is a lease row now, and this class is not taken.
const (
	lockFolder      int32 = 1
	lockOwner       int32 = 2
	lockMaintenance int32 = 3
	// serializationAttempts bounds a write aborted with SQLSTATE 40001
	// (serialization failure) or 40P01 (deadlock). Each attempt discards the
	// transaction and reruns the callback on a fresh Metadata. The wait before
	// attempt n+1 is n*2ms plus a jitter in [0, n*2ms). Eight attempts therefore
	// wait at most about 2+4+…+14 = 56ms, twice that with jitter. Writes run at
	// READ COMMITTED behind advisory locks, so these retries are a safety net.
	serializationAttempts = 8
	// lockTimeout bounds a wait on another session's advisory lock. A frozen
	// holder cannot stall a writer longer than this.
	lockTimeout = 4 * time.Second
	// idleTxTimeout aborts a transaction that stops issuing statements, which
	// releases its locks. Callbacks may pause on blob IO; this stays above that.
	idleTxTimeout = 8 * time.Second
	// maintenanceLease is how long a garbage-collection pass owns the schema.
	// The holder renews it during the pass. A frozen holder stops blocking
	// once the lease expires. No pooled connection is held between renewals.
	maintenanceLease = 5 * time.Second
	// minPoolConns is one connection for LISTEN plus two for transactions.
	minPoolConns = 3
)

// PostgresMetaStore is a MetaStore backed by one Postgres schema. Several
// processes may open the same schema. The caller owns db and must open it with
// the pgx stdlib driver (driver name "pgx"). Close does not close db. The pool
// must allow at least three connections: one stays on LISTEN and two remain
// for transactions. Unlimited (MaxOpenConnections 0) is accepted. Garbage
// collection claims a lease row and does not hold a pooled connection.
type PostgresMetaStore struct {
	db      *sql.DB
	schema  string
	channel string
	holder  string
	wakes   *notifications
	cancel  context.CancelFunc
	stopped chan struct{}
}

// notifyChannel is drivesync_ plus 16 hex chars of the schema hash. Postgres
// rejects channel names of 64 bytes or more, and a schema may be 63.
func notifyChannel(schema string) string {
	sum := sha256.Sum256([]byte(schema))
	return "drivesync_" + hex.EncodeToString(sum[:8])
}

// advisoryIdentity mixes the schema into an advisory-lock key so two schemas
// in one database do not share folder, owner, or maintenance locks.
func advisoryIdentity(schema, key string) string { return schema + "\x1f" + key }

func (s *PostgresMetaStore) Notifications() *Notifications { return s.wakes }

func validSchema(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if c != '_' && (c < 'a' || c > 'z') {
				return false
			}
			continue
		}
		if c != '_' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// OpenPostgresMetaStore creates schema when it is missing and starts the
// cross-process listener. An empty schema name uses "drivesync". schema must
// be a lowercase identifier. There is no migration path: the tables are the
// prototype shape and are created with IF NOT EXISTS. db must allow at least
// three connections. See PostgresMetaStore.
func OpenPostgresMetaStore(ctx context.Context, db *sql.DB, schema string) (*PostgresMetaStore, error) {
	if schema == "" {
		schema = "drivesync"
	}
	if !validSchema(schema) {
		return nil, fmt.Errorf("drivesync: schema %q must be a lowercase identifier", schema)
	}
	if db == nil {
		return nil, fmt.Errorf("drivesync: nil database")
	}
	// MaxOpenConnections of 0 means unlimited. One or two cannot hold the
	// LISTEN session and still run a transaction beside another request.
	if n := db.Stats().MaxOpenConnections; n > 0 && n < minPoolConns {
		return nil, fmt.Errorf("drivesync: postgres pool allows %d connections; OpenPostgresMetaStore needs at least 3 (one for LISTEN and two for transactions)", n)
	}
	ident := `"` + schema + `"`
	ddl := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + ident,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.folders (id TEXT PRIMARY KEY, owner TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', live INTEGER NOT NULL DEFAULT 1, data BYTEA NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS folder_names ON ` + ident + `.folders (owner, name) WHERE live <> 0`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.grants (folder TEXT NOT NULL, principal TEXT NOT NULL, data BYTEA NOT NULL, PRIMARY KEY(folder, principal))`,
		`CREATE INDEX IF NOT EXISTS grants_principal ON ` + ident + `.grants(principal, folder)`,
		`CREATE INDEX IF NOT EXISTS folders_owner ON ` + ident + `.folders(owner)`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.files (folder TEXT NOT NULL, path TEXT NOT NULL, data BYTEA NOT NULL, PRIMARY KEY(folder, path))`,
		`CREATE OR REPLACE FUNCTION ` + ident + `.ds_file_version(data bytea) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$ SELECT NULLIF(convert_from(data, 'UTF8')::jsonb->>'Version', '')::bigint $$`,
		`CREATE OR REPLACE FUNCTION ` + ident + `.ds_file_blob(data bytea) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT convert_from(data, 'UTF8')::jsonb->>'BlobID' $$`,
		`CREATE OR REPLACE FUNCTION ` + ident + `.ds_file_deleted(data bytea) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE((convert_from(data, 'UTF8')::jsonb->>'Deleted') = 'true', false) $$`,
		`CREATE OR REPLACE FUNCTION ` + ident + `.ds_file_deleted_at(data bytea) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(NULLIF(convert_from(data, 'UTF8')::jsonb->>'DeletedAt', '')::bigint, 0) $$`,
		`CREATE INDEX IF NOT EXISTS files_version ON ` + ident + `.files (folder, ` + ident + `.ds_file_version(data), path)`,
		`CREATE INDEX IF NOT EXISTS files_blob ON ` + ident + `.files (folder, ` + ident + `.ds_file_blob(data))`,
		`CREATE INDEX IF NOT EXISTS files_tombstones ON ` + ident + `.files (` + ident + `.ds_file_deleted_at(data), folder) WHERE ` + ident + `.ds_file_deleted(data)`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.tickets (id TEXT PRIMARY KEY, folder TEXT NOT NULL, data BYTEA NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS tickets_folder ON ` + ident + `.tickets(folder, id)`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.garbage (id TEXT PRIMARY KEY, folder TEXT NOT NULL, size BIGINT NOT NULL DEFAULT 0, writing INTEGER NOT NULL DEFAULT 0, data BYTEA NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS garbage_folder ON ` + ident + `.garbage(folder, id)`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.accounts (id TEXT PRIMARY KEY, data BYTEA NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ` + ident + `.maintenance (id INTEGER PRIMARY KEY, holder TEXT NOT NULL, expires BIGINT NOT NULL)`,
	}
	for _, q := range ddl {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return nil, err
		}
	}
	listenCtx, cancel := context.WithCancel(context.Background())
	s := &PostgresMetaStore{
		db:      db,
		schema:  schema,
		channel: notifyChannel(schema),
		holder:  randomID(),
		wakes:   newNotifications(),
		cancel:  cancel,
		stopped: make(chan struct{}),
	}
	go s.listen(listenCtx)
	return s, nil
}

func (s *PostgresMetaStore) Close() error {
	s.cancel()
	<-s.stopped
	return nil
}

func (s *PostgresMetaStore) listen(ctx context.Context) {
	defer close(s.stopped)
	wake := false
	backoff := 50 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		listened, err := s.listenOnce(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		if listened {
			wake = true
		}
		_ = err
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// listenOnce holds one dedicated connection. wake is set after a previous
// session dropped, so a successful LISTEN then wakes every waiter once.
func (s *PostgresMetaStore) listenOnce(ctx context.Context, wake bool) (bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	listened := false
	err = conn.Raw(func(dc any) error {
		std, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("drivesync: LISTEN requires the pgx stdlib driver, got %T", dc)
		}
		pg := std.Conn()
		if _, err := pg.Exec(ctx, "LISTEN "+s.channel); err != nil {
			return err
		}
		listened = true
		// The first connect wakes waiters too. A commit that landed before
		// LISTEN succeeded would otherwise sit until the wait deadline.
		// Reconnects pass wake=true and do the same.
		_ = wake
		s.wakes.wakeAll()
		for {
			n, err := pg.WaitForNotification(ctx)
			if err != nil {
				return err
			}
			if n != nil && n.Payload != "" {
				s.wakes.notify(n.Payload)
			}
		}
	})
	return listened, err
}

func (s *PostgresMetaStore) Transaction(ctx context.Context, fn func(*Metadata) error) error {
	var err error
	for attempt := 1; attempt <= serializationAttempts; attempt++ {
		err = s.transactionOnce(ctx, fn)
		if err == nil || !serializationFailure(err) {
			return err
		}
		if attempt == serializationAttempts || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		// attempt is the try that just failed, so the pause before the next
		// try is attempt*2ms plus jitter in [0, attempt*2ms).
		base := time.Duration(attempt) * 2 * time.Millisecond
		wait := base + time.Duration(rand.Int64N(int64(base)))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func serializationFailure(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "40001" || pg.Code == "40P01"
	}
	return false
}

func setLocalLimits(ctx context.Context, tx sqlTx) error {
	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '4s'"); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "SET LOCAL idle_in_transaction_session_timeout = '8s'")
	return err
}

func (s *PostgresMetaStore) transactionOnce(ctx context.Context, fn func(*Metadata) error) error {
	scope, _ := ScopeFromContext(ctx)
	// Writes take the folder lock and then the owner lock before they read,
	// which is what keeps READ COMMITTED from losing a concurrent update.
	// Read-only snapshots stay repeatable. 40001 and 40P01 still retry.
	opts := &sql.TxOptions{Isolation: sql.LevelReadCommitted}
	if scope.ReadOnly {
		opts = &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
	}
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	btx := boundTx{Tx: tx, dollar: true}
	if _, err = btx.ExecContext(ctx, "SELECT set_config('search_path', $1, true)", s.schema); err != nil {
		return err
	}
	// Indexes answer folder-scoped lookups. A seq scan would read other tenants.
	if _, err = btx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		return err
	}
	if err = setLocalLimits(ctx, btx); err != nil {
		return err
	}
	if !scope.ReadOnly {
		if err = s.lockScope(ctx, btx, scope); err != nil {
			return err
		}
	}
	if !scope.ReadOnly && scope.Folder != "" && scope.Principal.valid() {
		role, err := sqlRole(ctx, btx, scope.Principal, scope.Folder, scope.Write)
		if err != nil {
			return err
		}
		if scope.Owner && role != Owner {
			return ErrDenied
		}
	} else if scope.Folder != "" && scope.Principal.valid() {
		if _, err = sqlRole(ctx, btx, scope.Principal, scope.Folder, false); err != nil {
			return err
		}
	}
	changed, err := persistScoped(ctx, btx, fn, true)
	if err != nil {
		return err
	}
	if scope.ReadOnly {
		return nil
	}
	for id := range changed {
		if _, err = tx.ExecContext(ctx, "SELECT pg_notify($1, $2)", s.channel, id); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for id := range changed {
		s.wakes.notify(id)
	}
	return nil
}

func (s *PostgresMetaStore) advisory(ctx context.Context, tx sqlTx, class int32, key string) error {
	_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1, hashtext($2))", class, advisoryIdentity(s.schema, key))
	return err
}

// lockScope takes each folder lock, then each owner lock. Folder ids and owner
// keys are sorted so two transactions cannot deadlock. The owner id is the one
// read between those locks: every other read happens after both are held.
// Create, which has no folder yet, locks only the caller's counter.
func (s *PostgresMetaStore) lockScope(ctx context.Context, tx sqlTx, scope Scope) error {
	folders := []string{}
	if scope.Folder != "" {
		folders = append(folders, scope.Folder)
	}
	sort.Strings(folders)
	for _, id := range folders {
		if err := s.advisory(ctx, tx, lockFolder, id); err != nil {
			return err
		}
	}
	owners := map[string]struct{}{}
	for _, id := range folders {
		var owner string
		err := tx.QueryRowContext(ctx, "SELECT owner FROM "+s.tbl("folders")+" WHERE id=$1", id).Scan(&owner)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if owner != "" {
			owners[owner] = struct{}{}
		}
	}
	if len(owners) == 0 && scope.Principal.valid() {
		owners[principalKey(scope.Principal)] = struct{}{}
	}
	keys := make([]string, 0, len(owners))
	for key := range owners {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := s.advisory(ctx, tx, lockOwner, key); err != nil {
			return err
		}
	}
	return nil
}

// tryMaintenance claims a lease row for one GC or compaction pass and renews
// it until release. A frozen holder stops blocking after maintenanceLease.
// The claim is one short transaction; no pooled connection is held between
// renewals.
func (s *PostgresMetaStore) tryMaintenance(ctx context.Context) (func(), bool, error) {
	ok, err := s.claimMaintenance(ctx)
	if err != nil || !ok {
		return nil, false, err
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(maintenanceLease / 3)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				c, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = s.claimMaintenance(c)
				cancel()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
			c, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tx, err := s.db.BeginTx(c, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				return
			}
			defer tx.Rollback()
			btx := boundTx{Tx: tx, dollar: true}
			if err = setLocalLimits(c, btx); err != nil {
				return
			}
			if _, err = btx.ExecContext(c, `UPDATE `+s.tbl("maintenance")+` SET expires=0 WHERE holder=$1`, s.holder); err != nil {
				return
			}
			_ = tx.Commit()
		})
	}, true, nil
}

func (s *PostgresMetaStore) claimMaintenance(ctx context.Context) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	btx := boundTx{Tx: tx, dollar: true}
	if err = setLocalLimits(ctx, btx); err != nil {
		return false, err
	}
	now := time.Now().UnixNano()
	exp := now + int64(maintenanceLease)
	res, err := btx.ExecContext(ctx, `INSERT INTO `+s.tbl("maintenance")+` AS m (id, holder, expires) VALUES (1, $1, $2)
ON CONFLICT (id) DO UPDATE SET holder=excluded.holder, expires=excluded.expires
WHERE m.expires < $3 OR m.holder = excluded.holder`, s.holder, exp, now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *PostgresMetaStore) tbl(name string) string { return `"` + s.schema + `".` + name }
func (s *PostgresMetaStore) fn(name string) string  { return `"` + s.schema + `".` + name }

func (s *PostgresMetaStore) authorize(ctx context.Context, p Principal, id string, write bool, ticket, blob string, now time.Time) (Ticket, error) {
	if !p.valid() {
		return Ticket{}, ErrDenied
	}
	query := `SELECT g.data, COALESCE(t.data, 'null'), f.data FROM ` + s.tbl("grants") + ` g JOIN ` + s.tbl("folders") + ` f ON f.id=g.folder LEFT JOIN ` + s.tbl("tickets") + ` t ON t.id=$1 AND t.folder=g.folder WHERE g.folder=$2 AND g.principal=$3`
	args := []any{ticket, id, principalKey(p)}
	if ticket != "" {
		query += ` AND t.id IS NOT NULL`
	}
	var roleJSON, ticketJSON, folderJSON []byte
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&roleJSON, &ticketJSON, &folderJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrDenied
	}
	if err != nil {
		return Ticket{}, err
	}
	var role Role
	if err = json.Unmarshal(roleJSON, &role); err != nil {
		return Ticket{}, err
	}
	if role != Owner && role != Writer && role != Reader || write && role == Reader {
		return Ticket{}, ErrDenied
	}
	var t Ticket
	if ticket != "" {
		if err = json.Unmarshal(ticketJSON, &t); err != nil {
			return t, err
		}
		if t.FolderID != id || t.Principal != p {
			return Ticket{}, ErrDenied
		}
		var folder FolderRecord
		if err = json.Unmarshal(folderJSON, &folder); err != nil || t.AuthEpoch != folder.AuthEpoch {
			return Ticket{}, ErrDenied
		}
		if !t.Expires.After(now) {
			return Ticket{}, ErrExpired
		}
	}
	if blob != "" {
		ok, err := s.liveBlob(ctx, id, blob)
		if err != nil {
			return Ticket{}, err
		}
		if !ok {
			return Ticket{}, ErrDenied
		}
	}
	return t, nil
}

func (s *PostgresMetaStore) liveBlob(ctx context.Context, folder, blob string) (bool, error) {
	tx, err := s.readTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM `+s.tbl("files")+` WHERE folder=$1 AND `+s.fn("ds_file_blob")+`(data)=$2 AND NOT `+s.fn("ds_file_deleted")+`(data) LIMIT 1`, folder, blob).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *PostgresMetaStore) folderVersion(ctx context.Context, p Principal, id string) (uint64, error) {
	if !p.valid() {
		return 0, ErrDenied
	}
	var folderJSON, roleJSON []byte
	err := s.db.QueryRowContext(ctx, `SELECT f.data, g.data FROM `+s.tbl("grants")+` g JOIN `+s.tbl("folders")+` f ON f.id=g.folder WHERE g.folder=$1 AND g.principal=$2`, id, principalKey(p)).Scan(&folderJSON, &roleJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrDenied
	}
	if err != nil {
		return 0, err
	}
	var role Role
	var folder FolderRecord
	if err = json.Unmarshal(roleJSON, &role); err != nil {
		return 0, err
	}
	if err = json.Unmarshal(folderJSON, &folder); err != nil {
		return 0, err
	}
	if role != Owner && role != Writer && role != Reader {
		return 0, ErrDenied
	}
	return folder.Folder.Version, nil
}

func (s *PostgresMetaStore) changesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	version, err := s.folderVersion(ctx, p, id)
	if err != nil {
		return Delta{}, err
	}
	var folderJSON []byte
	if err = s.db.QueryRowContext(ctx, `SELECT data FROM `+s.tbl("folders")+` WHERE id=$1`, id).Scan(&folderJSON); err != nil {
		return Delta{}, err
	}
	var folder FolderRecord
	if err = json.Unmarshal(folderJSON, &folder); err != nil {
		return Delta{}, err
	}
	if after > version {
		return Delta{}, ErrInvalid
	}
	if until == 0 {
		until = version
	}
	if until < after || until > version {
		return Delta{}, ErrInvalid
	}
	mode, err := pageMode(page, after, until, folder.Folder.Horizon)
	if err != nil {
		return Delta{}, err
	}
	// Caught up incrementally: no row can have a version in (after, until].
	// Reading the folder's files here made an idle pull cost the whole tree.
	if !mode.full && mode.path == "" && after >= until {
		if _, err = s.folderVersion(ctx, p, id); err != nil {
			return Delta{}, err
		}
		return Delta{Version: until, Horizon: mode.horizon}, nil
	}
	ver := s.fn("ds_file_version")
	query := `SELECT data FROM ` + s.tbl("files") + ` WHERE folder=$1 AND ` + ver + `(data)>$2 AND ` + ver + `(data)<=$3 AND (` + ver + `(data)>$4 OR (` + ver + `(data)=$5 AND path>$6)) ORDER BY ` + ver + `(data), path LIMIT 513`
	args := []any{id, after, until, mode.version, mode.version, mode.path}
	if mode.full {
		query = `SELECT data FROM ` + s.tbl("files") + ` WHERE folder=$1 AND path>$2 ORDER BY path LIMIT 513`
		args = []any{id, mode.path}
	}
	tx, err := s.readTx(ctx)
	if err != nil {
		return Delta{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return Delta{}, err
	}
	defer rows.Close()
	out := Delta{Version: until, Horizon: mode.horizon, Full: mode.full}
	for rows.Next() {
		var data []byte
		var row Row
		if err = rows.Scan(&data); err != nil {
			return Delta{}, err
		}
		if err = json.Unmarshal(data, &row); err != nil {
			return Delta{}, err
		}
		if len(out.Rows) == 512 {
			out.Next = mode.next(out.Rows[len(out.Rows)-1])
			break
		}
		out.Rows = append(out.Rows, row)
	}
	if err = rows.Err(); err != nil {
		return Delta{}, err
	}
	rows.Close()
	if _, err = s.folderVersion(ctx, p, id); err != nil {
		return Delta{}, err
	}
	return out, nil
}

func (s *PostgresMetaStore) listFolders(ctx context.Context, p Principal) ([]Folder, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Stale statistics would seq-scan grants and grow with other tenants'
	// folders. The principal index answers this lookup.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		return nil, err
	}
	if err = setLocalLimits(ctx, boundTx{Tx: tx, dollar: true}); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
SELECT f.data, g.data
FROM `+s.tbl("grants")+` g
JOIN `+s.tbl("folders")+` f ON f.id = g.folder
WHERE g.principal = $1
ORDER BY f.id`, principalKey(p))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Folder{}
	for rows.Next() {
		var folderData, roleData []byte
		if err = rows.Scan(&folderData, &roleData); err != nil {
			return nil, err
		}
		var f FolderRecord
		var role Role
		if err = json.Unmarshal(folderData, &f); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(roleData, &role); err != nil {
			return nil, err
		}
		if f.Deleted {
			continue
		}
		if p == f.Folder.Owner {
			role = Owner
		}
		if role != Owner && role != Writer && role != Reader {
			continue
		}
		v := f.Folder
		u := f.FileUsage
		extra := f.TransferUsage
		u.Bytes = sat(u.Bytes, extra.Bytes)
		u.Reserved, u.ReservedFiles, u.ReservedRows, u.GarbageRows = extra.Reserved, extra.ReservedFiles, extra.ReservedRows, extra.GarbageRows
		v.Usage = u
		v.Role = role
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *PostgresMetaStore) readTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	btx := boundTx{Tx: tx, dollar: true}
	if _, err = btx.ExecContext(ctx, "SELECT set_config('search_path', $1, true)", s.schema); err != nil {
		tx.Rollback()
		return nil, err
	}
	if _, err = btx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = setLocalLimits(ctx, btx); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// selectFiles reads only the folder or tombstone rows a maintenance pass acts on.
func (s *PostgresMetaStore) selectFiles(ctx context.Context, folder string, tombstones bool, count int) ([]Row, error) {
	tx, err := s.readTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	q := `SELECT data FROM ` + s.tbl("files") + ` WHERE TRUE`
	args := []any{}
	n := 1
	if folder != "" {
		q += fmt.Sprintf(" AND folder=$%d", n)
		args = append(args, folder)
		n++
	}
	if tombstones {
		q += ` AND ` + s.fn("ds_file_deleted") + `(data) AND COALESCE(` + s.fn("ds_file_blob") + `(data), '') <> ''`
	}
	if count > 0 {
		q += fmt.Sprintf(" LIMIT $%d", n)
		args = append(args, count)
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Row{}
	for rows.Next() {
		var data []byte
		var row Row
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// emptyTombstones returns paths whose sealed blob is already gone and whose
// tombstone is older than cutoff. The partial index skips live rows.
func (s *PostgresMetaStore) emptyTombstones(ctx context.Context, cutoff int64) (map[string][]string, error) {
	tx, err := s.readTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT folder, path FROM `+s.tbl("files")+` WHERE `+s.fn("ds_file_deleted")+`(data) AND COALESCE(`+s.fn("ds_file_blob")+`(data), '') = '' AND `+s.fn("ds_file_deleted_at")+`(data) > 0 AND `+s.fn("ds_file_deleted_at")+`(data) < $1`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var folder, path string
		if err = rows.Scan(&folder, &path); err != nil {
			return nil, err
		}
		out[folder] = append(out[folder], path)
	}
	return out, rows.Err()
}
