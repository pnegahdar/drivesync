package dsreview2

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// After a plan downgrade, the primary owner can no longer revoke a cross-tenant
// grantee (as long as another cross-tenant grantee remains), nor downgrade them.
func TestRevokeBlockedWhenOwnerOverQuota(t *testing.T) {
	s := newServer(t)
	var plan atomic.Int64
	plan.Store(10 << 20)
	var fileCap atomic.Int64
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: plan.Load(), MaxFileBytes: fileCap.Load()}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "evil", Subject: "bob"}
	carol := ds.Principal{Tenant: "partner", Subject: "carol"}
	ac := s.Client(alice)
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 4 << 20, MaxRows: 1000})
	for _, p := range []ds.Principal{bob, carol} {
		if e := ac.Grant(bg, shared.ID, p, ds.Writer); e != nil {
			t.Fatal(e)
		}
	}
	plan.Store(1 << 20) // plan downgrade (grandfathered allocation)
	e1 := ac.Revoke(bg, shared.ID, bob)
	e2 := ac.Grant(bg, shared.ID, bob, ds.Reader)
	t.Logf("after plan downgrade: revoke bob -> %v; downgrade bob to reader -> %v", e1, e2)
	if e1 != nil {
		t.Errorf("BUG: primary owner cannot revoke a cross-tenant writer: %v", e1)
	}
	plan.Store(10 << 20)
	fileCap.Store(1 << 20) // plan lowers only the per-file cap
	e3 := ac.Revoke(bg, shared.ID, bob)
	t.Logf("after per-file cap downgrade: revoke bob -> %v", e3)
	if e3 != nil {
		t.Errorf("BUG: revoke blocked by per-file quota: %v", e3)
	}
	// Still a writer?
	_, e := s.Client(bob).Reserve(bg, shared.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", 1), SealedSize: 100})
	t.Logf("bob reserve after attempted revocations: %v", e)
}

// A delegated (non-primary) owner cannot revoke the last cross-tenant grantee.
func TestDelegatedOwnerCannotRevokeLastCrossTenant(t *testing.T) {
	s := newServer(t)
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	dave := ds.Principal{Tenant: "acme", Subject: "dave-admin"}
	bob := ds.Principal{Tenant: "evil", Subject: "bob"}
	ac := s.Client(alice)
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 1000})
	if e := ac.Grant(bg, shared.ID, dave, ds.Owner); e != nil {
		t.Fatal(e)
	}
	if e := ac.Grant(bg, shared.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	e := s.Client(dave).Revoke(bg, shared.ID, bob)
	e2 := s.Client(dave).Grant(bg, shared.ID, bob, ds.Reader)
	t.Logf("delegated owner revoke -> %v; downgrade -> %v", e, e2)
	if errors.Is(e, ds.ErrDenied) {
		t.Errorf("BUG: delegated owner cannot revoke compromised cross-tenant writer")
	}
}

// A same-tenant writer (different user) binary-searches ErrQuota to learn the
// owner's exact usage across private folders, then watches it change.
func TestSameTenantWriterLearnsExactPrivateUsage(t *testing.T) {
	s := newServer(t)
	const plan = 1 << 30
	var currentPlan atomic.Int64
	currentPlan.Store(2 * plan)
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: currentPlan.Load()}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	eve := ds.Principal{Tenant: "acme", Subject: "eve"}
	ac, ec := s.Client(alice), s.Client(eve)
	private, pk := mkFolder(t, ac, ds.Limits{})
	salary := putFile(t, ac, private, pk, "salary-2027.xlsx", 0, make([]byte, 123456))
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: plan, MaxRows: 1000})
	if e := ac.Grant(bg, shared.ID, eve, ds.Writer); e != nil {
		t.Fatal(e)
	}
	currentPlan.Store(plan) // lower the plan below existing allocated usage
	probe := func() int64 {
		lo, hi := int64(0), int64(plan)
		n := 0
		for lo < hi {
			mid := lo + (hi-lo+1)/2
			n++
			tk, e := ec.Reserve(bg, shared.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", 7), SealedSize: mid})
			if e == nil {
				ec.CancelUpload(bg, shared.ID, tk.ID)
				lo = mid
			} else if isQuota(e) {
				hi = mid - 1
			} else {
				t.Fatal(e)
			}
		}
		return plan - lo // = owner usage + 256 row cost of the probe
	}
	u0 := probe()
	if _, e := ac.Commit(bg, private.ID, []ds.Mutation{{PathID: salary.PathID, BaseVersion: salary.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	if e := s.CollectGarbage(bg); e != nil {
		t.Fatal(e)
	}
	u1 := probe()
	t.Logf("eve learned alice's account usage: before=%d after private delete=%d (delta %d; sealed file %d + row/metadata)", u0, u1, u1-u0, ds.SealedSize(123456))
	if u1 != u0 {
		t.Errorf("BUG: same-tenant writer reads exact private usage via ErrQuota probes")
	}
}

func isQuota(e error) bool {
	var le *ds.LimitError
	return errors.Is(e, ds.ErrQuota) || errors.As(e, &le)
}
