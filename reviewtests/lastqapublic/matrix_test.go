package qapublic

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// Control (expected to PASS): the public facade enforces grants for every
// principal class over both transports, and denied == nonexistent.
func TestPublicAccessMatrixBothTransports(t *testing.T) {
	writer := drivesync.Principal{Tenant: "acme", Subject: "writer"}
	reader := drivesync.Principal{Tenant: "acme", Subject: "reader"}
	other := drivesync.Principal{Tenant: "globex", Subject: "alice"} // same subject, other tenant
	ungranted := drivesync.Principal{Tenant: "acme", Subject: "mallory"}
	revoked := drivesync.Principal{Tenant: "acme", Subject: "revoked"}
	anon := drivesync.Principal{}
	all := []drivesync.Principal{alice, writer, reader, other, ungranted, revoked, anon}
	for _, transport := range []string{"inprocess", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newServer(t)
			client := s.Client
			if transport == "http" {
				tokens := map[string]drivesync.Principal{}
				for i, p := range all {
					if p != anon {
						tokens[string(rune('a'+i))] = p
					}
				}
				h := httptest.NewServer(s.Handler(func(r *http.Request) (drivesync.Principal, error) {
					if p, ok := tokens[r.Header.Get("Authorization")]; ok {
						return p, nil
					}
					return drivesync.Principal{}, drivesync.ErrDenied
				}))
				t.Cleanup(h.Close)
				client = func(p drivesync.Principal) *drivesync.Client {
					for tok, q := range tokens {
						if q == p {
							return drivesync.NewHTTPClient(h.URL, http.Header{"Authorization": {tok}})
						}
					}
					return drivesync.NewHTTPClient(h.URL, nil)
				}
			}
			key := drivesync.NewFolderKey()
			f, e := client(alice).CreateFolder(bg, drivesync.FolderSpec{Name: "m", Limits: shared}, key)
			if e != nil {
				t.Fatal(e)
			}
			for p, r := range map[drivesync.Principal]drivesync.Role{writer: drivesync.Writer, reader: drivesync.Reader, revoked: drivesync.Writer} {
				if e = client(alice).Grant(bg, f.ID, p, r); e != nil {
					t.Fatal(e)
				}
			}
			if e = client(alice).Revoke(bg, f.ID, revoked); e != nil {
				t.Fatal(e)
			}
			guess := "0123456789abcdef0123456789abcdef"
			target := drivesync.Principal{Tenant: "acme", Subject: "new"}
			for _, p := range all {
				c := client(p)
				granted := p == alice || p == writer || p == reader
				_, ge := c.GetFolder(bg, f.ID)
				_, gg := c.GetFolder(bg, guess)
				if !errors.Is(gg, drivesync.ErrDenied) || granted != (ge == nil) || (!granted && !errors.Is(ge, drivesync.ErrDenied)) {
					t.Errorf("%v Get: %v guess: %v", p, ge, gg)
				}
				list, le := c.ListFolders(bg)
				if granted != (le == nil && len(list) == 1) {
					t.Errorf("%v List: %v %v", p, list, le)
				}
				ownerOnly := []error{
					c.Grant(bg, f.ID, target, drivesync.Reader),
					c.Revoke(bg, f.ID, target),
					c.SetLimits(bg, f.ID, shared),
				}
				for i, oe := range ownerOnly {
					if (p == alice) != (oe == nil) || (p != alice && !errors.Is(oe, drivesync.ErrDenied)) {
						t.Errorf("%v owner op %d: %v", p, i, oe)
					}
				}
				dir := filepath.Join(t.TempDir(), "r")
				r, ae := drivesync.Attach(bg, c, f.ID, key, dir, drivesync.Options{Manual: true, Name: "r"})
				if granted != (ae == nil) || (!granted && !errors.Is(ae, drivesync.ErrDenied)) {
					t.Errorf("%v Attach: %v", p, ae)
				}
				if r != nil {
					_ = os.WriteFile(filepath.Join(dir, string(rune('a'+len(p.Subject)))+p.Subject+".txt"), []byte(p.Subject), 0600)
					_ = r.Sync(bg)
					st := r.Status()
					if (p == reader) != (len(st.Rejected) == 1) {
						t.Errorf("%v write: rejected=%v", p, st.Rejected)
					}
					r.Close()
				}
				if p != alice {
					if de := c.DeleteFolder(bg, f.ID); !errors.Is(de, drivesync.ErrDenied) {
						t.Errorf("%v Delete: %v", p, de)
					}
				}
			}
			got, _ := client(alice).GetFolder(bg, f.ID)
			if got.Usage.Files != 2 { // owner + writer files only
				t.Errorf("files written: %d", got.Usage.Files)
			}
			if e = client(alice).DeleteFolder(bg, f.ID); e != nil {
				t.Fatal(e)
			}
			for _, p := range all {
				if _, e := client(p).GetFolder(bg, f.ID); !errors.Is(e, drivesync.ErrDenied) {
					t.Errorf("%v after delete: %v", p, e)
				}
			}
		})
	}
}
