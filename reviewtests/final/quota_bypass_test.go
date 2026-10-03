package finalreview

import (
	"context"
	"errors"
	"fmt"
	"testing"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

const plan = 1 << 20

func planPolicy() ds.QuotaPolicy {
	return ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: plan}, nil
	})
}

// fill writes 60 KiB files until the folder or the plan rejects the next one.
func fill(t *testing.T, c ds.Client, f ds.Folder, k ds.FolderKey, tag string) int {
	data := make([]byte, 60<<10)
	n := 0
	for ; ; n++ {
		if _, e := tryPut(c, f, k, fmt.Sprintf("%s-%d.bin", tag, n), 0, data); e != nil {
			t.Logf("%s: stopped after %d files: %v", tag, n, e)
			return n
		}
	}
}

func storedBytes(t *testing.T, c ds.Client) (total int64) {
	fs, e := c.ListFolders(bg)
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range fs {
		total += f.Usage.Bytes
	}
	return
}

// Finding 1a: allocate a filled private folder with a tiny exact cap. The owner is
// charged the 1-byte cap, not the ~1 MiB already stored, and refills the plan.
func TestAllocateFilledPrivateFolderWithTinyCap(t *testing.T) {
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			s, _ := newServer(t)
			s.Quotas = planPolicy()
			alice := ds.Principal{Tenant: "acme", Subject: "alice"}
			puppet := ds.Principal{Tenant: "acme", Subject: "nobody"}
			var c ds.Client = s.Client(alice)
			if transport == "http" {
				c = httpClients(t, s)(alice)
			}
			for round := 0; round < 4; round++ {
				f, k := mkFolder(t, c, ds.Limits{})
				if fill(t, c, f, k, fmt.Sprint("r", round)) == 0 {
					t.Fatalf("round %d: no headroom", round)
				}
				if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: 1, MaxRows: 1, MaxFileBytes: 1}); e != nil {
					var l *ds.LimitError
					if !errors.As(e, &l) {
						t.Fatal(e)
					}
					if total := storedBytes(t, c); total > plan {
						t.Fatal(total)
					}
					return
				}
				if e := c.Grant(bg, f.ID, puppet, ds.Reader); e != nil {
					t.Fatalf("round %d grant: %v", round, e)
				}
				_ = c.Revoke(bg, f.ID, puppet) // allocation is sticky; revocation is free
			}
			total := storedBytes(t, c)
			t.Logf("plan %d bytes, stored %d sealed bytes (%.2fx)", plan, total, float64(total)/plan)
			if total > plan {
				t.Errorf("stored %d bytes on a %d-byte plan", total, plan)
			}
		})
	}
}

// Finding 1b: same bypass through a properly shared folder, filled under its cap
// and then lowered. Only the primary owner can do this, but needs no help.
func TestLowerFilledSharedCapRepeatedly(t *testing.T) {
	s, _ := newServer(t)
	s.Quotas = planPolicy()
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "partner", Subject: "bob"}
	c := s.Client(alice)
	for round := 0; round < 4; round++ {
		f, k := mkFolder(t, c, ds.Limits{MaxTotalBytes: plan - 64<<10, MaxRows: 4096})
		if e := c.Grant(bg, f.ID, bob, ds.Writer); e != nil {
			// reduce cap until it fits in remaining headroom
			t.Fatalf("round %d grant: %v", round, e)
		}
		fill(t, c, f, k, fmt.Sprint("s", round))
		if e := c.SetLimits(bg, f.ID, ds.Limits{MaxTotalBytes: 1, MaxRows: 1, MaxFileBytes: 1}); e != nil {
			var l *ds.LimitError
			if !errors.As(e, &l) {
				t.Fatal(e)
			}
			if total := storedBytes(t, c); total > plan {
				t.Fatal(total)
			}
			return
		}
	}
	total := storedBytes(t, c)
	t.Logf("plan %d bytes, stored %d sealed bytes (%.2fx)", plan, total, float64(total)/plan)
	if total > plan {
		t.Errorf("stored %d bytes on a %d-byte plan", total, plan)
	}
}
