package dsreview

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

// A cross-tenant writer on ONE shared folder learns the owner's total usage across
// all of the owner's private folders (and the owner's plan quota), live.
func TestOwnerUsageLeakThroughLimitError(t *testing.T) {
	s := newServer(t)
	s.Quotas = ds.QuotaFunc(func(_ context.Context, p ds.Principal) (ds.Quota, error) {
		return ds.Quota{MaxTotalBytes: 10 << 20}, nil
	})
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	bob := ds.Principal{Tenant: "evilcorp", Subject: "bob"}
	srv := httptest.NewServer(s.Handler(func(r *http.Request) (ds.Principal, error) {
		return ds.Principal{Tenant: r.Header.Get("T"), Subject: r.Header.Get("S")}, nil
	}))
	defer srv.Close()
	ac := s.Client(alice)
	bc := ds.NewHTTPClient(srv.URL, http.Header{"T": {bob.Tenant}, "S": {bob.Subject}})

	uncapped, _ := mkFolder(t, ac, ds.Limits{})
	if e := ac.Grant(bg, uncapped.ID, bob, ds.Writer); e == nil {
		t.Error("uncapped shared folder exposes private-usage probing")
	}
	private, pk := mkFolder(t, ac, ds.Limits{}) // bob has no access
	shared, _ := mkFolder(t, ac, ds.Limits{MaxTotalBytes: 2 << 20, MaxRows: 1000})
	if e := ac.Grant(bg, shared.ID, bob, ds.Writer); e != nil {
		t.Fatal(e)
	}
	if _, e := bc.GetFolder(bg, private.ID); !errors.Is(e, ds.ErrDenied) {
		t.Fatal("bob should not see private", e)
	}
	probe := func() (int64, int64) {
		const ask = 1 << 40
		_, e := bc.Reserve(bg, shared.ID, ds.UploadRequest{PathID: "00000000000000000000000000000000000000000000000000000000000000aa", SealedSize: ask})
		var l *ds.LimitError
		if !errors.As(e, &l) {
			t.Fatal("expected limit error", e)
		}
		t.Logf("bob sees: %v", l)
		return l.Requested - ask, l.Maximum
	}
	used0, quota := probe()
	putFile(t, ac, private, pk, "secret-plan.pdf", 0, make([]byte, 123456))
	used1, _ := probe()
	// Reservations in the private folder are visible too (live activity oracle).
	pid, _ := ds.PathID(pk, private.ID, "upload-in-progress")
	if _, e := ac.Reserve(bg, private.ID, ds.UploadRequest{PathID: pid, SealedSize: 777777}); e != nil {
		t.Fatal(e)
	}
	used2, _ := probe()
	t.Logf("owner plan quota=%d; owner usage before=%d after private upload=%d during private reservation=%d", quota, used0, used1, used2)
	if used1 != used0 || used2 != used0 || quota != 2<<20 {
		t.Fatal("private owner usage or quota leaked")
	}
}
