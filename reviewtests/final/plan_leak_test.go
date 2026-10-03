package finalreview

import (
	"context"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// Grantees read the owner's plan per-file cap from the folder limits when the
// owner left MaxFileBytes unset (captured silently at allocation).
func TestGranteeSeesOwnerPlanFileCap(t *testing.T) {
	s, _ := newServer(t)
	const planFileCap = 7_340_033 // distinctive plan tier value
	s.Quotas = ds.QuotaFunc(func(context.Context, ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 1 << 30, MaxFileBytes: planFileCap}, nil
	})
	owner := s.Client(ds.Principal{Tenant: "acme", Subject: "owner"})
	guest := ds.Principal{Tenant: "partner", Subject: "guest"}
	f, e := owner.CreateFolder(bg, ds.FolderSpec{Name: "missing-file-cap", Limits: ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 100}, KeyCheck: ds.KeyCheck(ds.NewFolderKey())})
	if e != nil {
		t.Fatal(e)
	}
	if e := owner.Grant(bg, f.ID, guest, ds.Reader); e == ds.ErrInvalid {
		return
	} else if e != nil {
		t.Fatal(e)
	}
	g, e := s.Client(guest).GetFolder(bg, f.ID)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("guest sees Limits=%+v", g.Limits)
	if g.Limits.MaxFileBytes == planFileCap {
		t.Error("cross-tenant guest learned the owner's plan per-file cap")
	}
}
