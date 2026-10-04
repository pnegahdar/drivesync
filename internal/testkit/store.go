// Package testkit opens the metadata store selected by DRIVESYNC_STORE.
// The default is sqlite. postgres starts one embedded Postgres per process
// and gives each test its own schema.
package testkit

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
	"github.com/pnegahdar/drivesync/internal/embedpg"
	"github.com/pnegahdar/drivesync/internal/engine"
)

// EngineStore is the private metadata store tests share across backends.
type EngineStore interface {
	Transaction(context.Context, func(*engine.Metadata) error) error
	Close() error
}

// Postgres reports whether DRIVESYNC_STORE selects Postgres.
func Postgres() bool { return embedpg.Postgres() }

// OpenEngine opens the engine store for this test and closes it on cleanup.
func OpenEngine(t testing.TB) EngineStore {
	t.Helper()
	if !Postgres() {
		m, err := engine.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		return m
	}
	db := embedpg.SharedDB(t)
	schema := embedpg.NewSchema()
	m, err := engine.OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Close()
		embedpg.DropSchema(db, schema)
	})
	return m
}

// OpenEnginePair returns two stores on one database. SQLite shares a file.
// Postgres shares a schema, so each store is its own authority process view.
func OpenEnginePair(t testing.TB) (EngineStore, EngineStore) {
	t.Helper()
	if !Postgres() {
		name := filepath.Join(t.TempDir(), "meta.sqlite")
		a, err := engine.OpenSQLiteMetaStore(name)
		if err != nil {
			t.Fatal(err)
		}
		b, err := engine.OpenSQLiteMetaStore(name)
		if err != nil {
			a.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { a.Close(); b.Close() })
		return a, b
	}
	db := embedpg.SharedDB(t)
	schema := embedpg.NewSchema()
	a, err := engine.OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		t.Fatal(err)
	}
	b, err := engine.OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		a.Close()
		b.Close()
		embedpg.DropSchema(db, schema)
	})
	return a, b
}

// OpenPublic opens the public metadata store for this test.
func OpenPublic(t testing.TB) *drivesync.MetaStore {
	t.Helper()
	if !Postgres() {
		m, err := drivesync.OpenSQLiteMetaStore(filepath.Join(t.TempDir(), "meta.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		return m
	}
	db := embedpg.SharedDB(t)
	schema := embedpg.NewSchema()
	m, err := drivesync.OpenPostgresMetaStore(context.Background(), db, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Close()
		embedpg.DropSchema(db, schema)
	})
	return m
}
