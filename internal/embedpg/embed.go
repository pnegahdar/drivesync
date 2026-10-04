// Package embedpg starts one embedded Postgres per process for tests.
// Production packages do not import it. DRIVESYNC_STORE=postgres selects it;
// the default is sqlite, decided by the caller.
package embedpg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// postgresVersion is the embedded server. The darwin-arm64 18.0.0 archive is
// an x86_64 binary and fails with a bad CPU type on Apple Silicon. 18.6.0
// ships a universal binary. Binaries unpack once under this version.
const postgresVersion = "18.6.0"

// Postgres reports whether this process should open the Postgres metadata store.
func Postgres() bool { return os.Getenv("DRIVESYNC_STORE") == "postgres" }

// NewSchema returns a lowercase schema name unique to one test.
func NewSchema() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "ds_" + hex.EncodeToString(b[:])
}

// DropSchema removes a test schema. It ignores errors so cleanup stays quiet.
func DropSchema(db *sql.DB, schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
}

var (
	pgOnce   sync.Once
	pgDB     *sql.DB
	pgErr    error
	pgServer *embeddedpostgres.EmbeddedPostgres
	pgDir    string
)

// Shutdown stops the process-wide embedded server and deletes its files.
// TestMain calls it so a finished package leaves neither a postmaster nor its
// few hundred megabytes behind. macOS allows only a few System V
// shared-memory IDs, and a leaked server holds one.
func Shutdown() {
	if pgServer != nil {
		_ = pgServer.Stop()
		pgServer = nil
	}
	if pgDB != nil {
		_ = pgDB.Close()
		pgDB = nil
	}
	if pgDir != "" {
		_ = os.RemoveAll(pgDir)
		pgDir = ""
	}
}

// SharedDB returns the process-wide embedded database. The caller owns schemas
// created in it and must not close the pool.
func SharedDB(t testing.TB) *sql.DB {
	t.Helper()
	pgOnce.Do(func() { pgDB, pgErr = start() })
	if pgErr != nil {
		t.Fatal(pgErr)
	}
	return pgDB
}

func start() (*sql.DB, error) {
	// A crashed package can leave its data directory and postmaster behind.
	// Reap only a drivesync-pg-* folder whose postmaster.pid names a dead pid.
	// A folder with no pid file may belong to a start that has not written it yet.
	reapStaleDataDirs()
	binaries, cache, unlock, err := sharedBinaries()
	if err != nil {
		return nil, err
	}
	defer unlock()
	dir, err := os.MkdirTemp("", "drivesync-pg-")
	if err != nil {
		return nil, err
	}
	pgDir = dir
	defer func() {
		// A server that never started leaves nothing for Shutdown to delete.
		if pgServer == nil {
			_ = os.RemoveAll(dir)
			pgDir = ""
		}
	}()
	var last error
	for try := 0; try < 5; try++ {
		port, err := freePort()
		if err != nil {
			return nil, err
		}
		// RuntimePath is removed on every Start, so it stays in this run's
		// temp dir. BinariesPath and CachePath are the shared unpack and are
		// not removed. Each run gets its own data directory.
		cfg := embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.PostgresVersion(postgresVersion)).
			Port(uint32(port)).
			Database("drivesync").
			Username("drivesync").
			Password("drivesync").
			BinariesPath(binaries).
			CachePath(cache).
			RuntimePath(filepath.Join(dir, "run")).
			DataPath(filepath.Join(dir, "data")).
			Logger(io.Discard).
			StartTimeout(90 * time.Second).
			// mmap keeps the running server off the System V shared-memory
			// table. initdb still needs one ID for a moment.
			// The server idle limit is short so a raw transaction that stops
			// issuing statements releases its advisory locks. Library
			// transactions SET LOCAL a longer limit.
			StartParameters(map[string]string{
				"shared_memory_type":                  "mmap",
				"dynamic_shared_memory_type":          "mmap",
				"idle_in_transaction_session_timeout": "2000",
			})
		ep := embeddedpostgres.NewDatabase(cfg)
		if err = ep.Start(); err != nil {
			last = err
			continue
		}
		pgServer = ep
		db, err := sql.Open("pgx", cfg.GetConnectionURL())
		if err != nil {
			_ = ep.Stop()
			pgServer = nil
			return nil, err
		}
		db.SetMaxOpenConns(32)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err != nil {
			db.Close()
			_ = ep.Stop()
			pgServer = nil
			last = err
			continue
		}
		return db, nil
	}
	return nil, fmt.Errorf("start embedded postgres: %w", last)
}

// sharedBinaries returns the versioned unpack directory and holds its file
// lock until unlock. The lock covers download and extract. It is not held
// while tests run.
func sharedBinaries() (binaries, cache string, unlock func(), err error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", "", nil, err
	}
	root := filepath.Join(base, "drivesync-embedpg", postgresVersion)
	if err = os.MkdirAll(root, 0o755); err != nil {
		return "", "", nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return "", "", nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return "", "", nil, err
	}
	unlock = func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}
	return filepath.Join(root, "binaries"), filepath.Join(root, "cache"), unlock, nil
}

// reapStaleDataDirs removes leftover embedded data directories. A missing
// postmaster.pid is not proof the owner has exited: a concurrent start may
// have just created the directory.
func reapStaleDataDirs() {
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "drivesync-pg-") {
			continue
		}
		dir := filepath.Join(os.TempDir(), entry.Name())
		if !postmasterDead(filepath.Join(dir, "data", "postmaster.pid")) {
			continue
		}
		_ = os.RemoveAll(dir)
	}
}

// postmasterDead reports whether pidFile names a process that is not running.
// A missing or unreadable file returns false.
func postmasterDead(pidFile string) bool {
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	line, _, _ := strings.Cut(string(b), "\n")
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || pid <= 0 {
		return false
	}
	err = syscall.Kill(pid, 0)
	return errors.Is(err, syscall.ESRCH)
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	return port, ln.Close()
}
