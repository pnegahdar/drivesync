package confirmreview

import (
	"context"
	"errors"
	"fmt"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

type failingDeletes struct{ *ds.MemoryBlobStore }

func (failingDeletes) Delete(context.Context, string, string) error {
	return errors.New("blob store unavailable")
}

// Existing-row deletes skip every limit check, but each one adds a tombstone
// row plus a queued-garbage row. In an allocated folder the owner is charged
// exactly the cap, so stored bytes/rows exceed the charge until GC succeeds; a
// failing blob store makes the excess persistent (documented as "brief").
func TestExistingRowDeletesExceedAllocatedCharge(t *testing.T) {
	m, e := ds.OpenSQLiteMetaStore(t.TempDir() + "/m.sqlite")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { m.Close() })
	s := ds.NewServer(m, failingDeletes{ds.NewMemoryBlobStore()})
	owner := s.Client(ds.Principal{Tenant: "t", Subject: "owner"})
	const files = 40
	limits := ds.Limits{MaxTotalBytes: 40 * 400, MaxRows: files, MaxFileBytes: 1 << 20}
	f, k := mkFolder(t, owner, limits)
	if e := owner.Grant(bg, f.ID, ds.Principal{Tenant: "t", Subject: "writer"}, ds.Writer); e != nil {
		t.Fatal(e)
	}
	var rows []ds.Row
	for i := 0; ; i++ {
		r, e := tryPut(owner, f, k, fmt.Sprintf("f%02d", i), 0, []byte("x"))
		if e != nil {
			break
		}
		rows = append(rows, r)
	}
	full, _ := owner.GetFolder(bg, f.ID)
	for _, r := range rows {
		if _, e := owner.Commit(bg, f.ID, []ds.Mutation{{PathID: r.PathID, BaseVersion: r.Version, Deleted: true}}); e != nil {
			t.Fatal(e)
		}
	}
	_ = s.CollectGarbage(bg)
	after, _ := owner.GetFolder(bg, f.ID)
	storedRows := after.Usage.Rows + after.Usage.GarbageRows
	t.Logf("cap bytes=%d rows=%d (charged rows=%d); full: bytes=%d rows=%d; after deleting all + failed GC: bytes=%d rows=%d", limits.MaxTotalBytes, limits.MaxRows, min(limits.MaxTotalBytes/ds.RowCost, limits.MaxRows), full.Usage.Bytes, full.Usage.Rows, after.Usage.Bytes, storedRows)
	if after.Usage.Bytes > limits.MaxTotalBytes || storedRows > min(limits.MaxTotalBytes/ds.RowCost, limits.MaxRows) {
		t.Fatalf("stored bytes/rows exceed the owner's allocated charge: bytes %d > %d or rows %d > %d", after.Usage.Bytes, limits.MaxTotalBytes, storedRows, min(limits.MaxTotalBytes/ds.RowCost, limits.MaxRows))
	}
}
