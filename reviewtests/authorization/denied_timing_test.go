package authorization

import (
	"fmt"
	"strings"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// Revoked principal timing: existing-with-grants vs deleted/nonexistent folder.
func TestDeniedVersusNonexistentTiming(t *testing.T) {
	s, _ := newServer(t)
	owner := s.Client(ds.Principal{Tenant: "acme", Subject: "owner"})
	ex := ds.Principal{Tenant: "acme", Subject: "ex-employee"}
	exc := s.Client(ex)
	f, _ := mkFolder(t, owner, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 100})
	long := strings.Repeat("s", 250)
	for i := 0; i < 255; i++ {
		if e := owner.Grant(bg, f.ID, ds.Principal{Tenant: fmt.Sprintf("t%03d", i) + strings.Repeat("x", 240), Subject: long}, ds.Reader); e != nil {
			t.Fatal(e)
		}
	}
	if e := owner.Grant(bg, f.ID, ex, ds.Reader); e != nil {
		t.Fatal(e)
	}
	if e := owner.Revoke(bg, f.ID, ex); e != nil {
		t.Fatal(e)
	}
	g, _ := mkFolder(t, owner, ds.Limits{})
	if e := owner.DeleteFolder(bg, g.ID); e != nil {
		t.Fatal(e)
	}
	s.CollectGarbage(bg)
	probe := func(id string) time.Duration {
		return median(t, 401, func() {
			if _, e := exc.Wait(bg, id, 0); e != ds.ErrDenied {
				t.Fatal(e)
			}
		})
	}
	for round := 0; round < 2; round++ {
		existing, missing := probe(f.ID), probe("0123456789abcdef0123456789abcdef")
		t.Logf("revoked principal Wait: existing(256 grants)=%v purged=%v random=%v", existing, probe(g.ID), missing)
		if existing > 5*missing {
			t.Fatal("denial decodes existing folder grants", existing, missing)
		}
	}
}

func TestDeniedVersusNonexistentTimingNoGrants(t *testing.T) {
	s, _ := newServer(t)
	owner := s.Client(ds.Principal{Tenant: "acme", Subject: "owner"})
	ex := ds.Principal{Tenant: "acme", Subject: "ex-employee"}
	exc := s.Client(ex)
	f, _ := mkFolder(t, owner, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 100})
	owner.Grant(bg, f.ID, ex, ds.Reader)
	owner.Revoke(bg, f.ID, ex)
	probe := func(id string) time.Duration {
		return median(t, 2001, func() { exc.Wait(bg, id, 0) })
	}
	for round := 0; round < 3; round++ {
		t.Logf("existing(0 grants)=%v random=%v", probe(f.ID), probe("0123456789abcdef0123456789abcdef"))
	}
}
