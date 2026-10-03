package drivesync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCapFloorIncludesPendingAndGarbage(t *testing.T) {
	for _, kind := range []string{"reservation", "garbage", "rows"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testServer(t)
			c := s.Client(owner)
			ctx := context.Background()
			f, k := folderFor(t, c, Limits{})
			if kind == "reservation" {
				pid, _ := PathID(k, f.ID, "pending")
				if _, e := c.Reserve(ctx, f.ID, UploadRequest{PathID: pid, SealedSize: 100}); e != nil {
					t.Fatal(e)
				}
			} else {
				row := put(t, c, f, k, "file", 0, []byte("bytes"))
				if _, e := c.Commit(ctx, f.ID, []Mutation{{PathID: row.PathID, BaseVersion: row.Version, Deleted: true}}); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "rows" {
				put(t, c, f, k, "another", 0, []byte("bytes"))
			}
			got, _ := c.GetFolder(ctx, f.ID)
			bytes, rows := sat(got.Usage.Bytes, got.Usage.Reserved), sat(sat(got.Usage.Rows, got.Usage.GarbageRows), got.Usage.ReservedRows)
			l := Limits{MaxTotalBytes: bytes, MaxRows: rows, MaxFileBytes: bytes}
			if kind == "rows" {
				l.MaxRows--
			} else {
				l.MaxTotalBytes--
				l.MaxFileBytes = l.MaxTotalBytes
			}
			s.Quotas = QuotaFunc(func(context.Context, Principal) (Quota, error) {
				t.Error("cap floor called pricing")
				return Quota{}, errors.New("outage")
			})
			var le *LimitError
			if e := c.SetLimits(ctx, f.ID, l); !errors.As(e, &le) {
				t.Fatal("accepted undercharged cap", got.Usage, l, e)
			}
		})
	}
}

type versionCounter struct {
	*SQLiteMetaStore
	calls atomic.Int32
}

func (m *versionCounter) folderVersion(ctx context.Context, p Principal, id string) (uint64, error) {
	m.calls.Add(1)
	return m.SQLiteMetaStore.folderVersion(ctx, p, id)
}
func TestWaitsAreIdleAndFolderScoped(t *testing.T) {
	_, m := testServer(t)
	cm := &versionCounter{SQLiteMetaStore: m}
	s := NewServer(cm, NewMemoryBlobStore())
	c := s.Client(owner)
	f, _ := folderFor(t, c, Limits{})
	other, k := folderFor(t, c, Limits{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Wait(ctx, f.ID, 0); done <- e }()
	deadline := time.Now().Add(time.Second)
	for cm.calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("wait never queried")
		}
		time.Sleep(time.Millisecond)
	}
	put(t, c, other, k, "unrelated", 0, []byte("one"))
	time.Sleep(600 * time.Millisecond)
	if calls := cm.calls.Load(); calls != 1 {
		t.Fatal("idle/foreign mutation queried wait folder", calls)
	}
	if e := c.DeleteFolder(ctx, f.ID); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if !errors.Is(e, ErrDenied) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("deletion didn't wake wait")
	}
}
func TestSQLiteIndexedAccessPlans(t *testing.T) {
	_, m := testServer(t)
	for _, q := range []string{
		"SELECT data FROM tickets WHERE folder='f'", "SELECT data FROM garbage WHERE folder='f'",
		"SELECT data FROM grants WHERE folder='f' AND principal='p'",
		"SELECT data FROM folders WHERE id IN (SELECT folder FROM grants WHERE principal='p')",
	} {
		rows, e := m.db.Query("EXPLAIN QUERY PLAN " + q)
		if e != nil {
			t.Fatal(e)
		}
		for rows.Next() {
			var a, b, c int
			var detail string
			if e = rows.Scan(&a, &b, &c, &detail); e != nil {
				t.Fatal(e)
			}
			if strings.Contains(detail, "SCAN ") {
				t.Fatal("unindexed access", q, detail)
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
}
func TestWaitAcrossSQLiteAuthorities(t *testing.T) {
	s, m := testServer(t)
	second, e := OpenSQLiteMetaStore(mName(t, m))
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	other := NewServer(second, s.Blobs)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := other.Client(owner).Wait(ctx, f.ID, 0); done <- e }()
	put(t, c, f, k, "new", 0, []byte("x"))
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
func mName(t *testing.T, m *SQLiteMetaStore) string {
	t.Helper()
	var seq int
	var name, file string
	if e := m.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &file); e != nil {
		t.Fatal(fmt.Sprint(e))
	}
	return file
}
