package disk

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pnegahdar/drivesync"
)

// The RPC endpoint accepts any Content-Type. With cookie-based authentication
// a cross-site <form enctype="text/plain"> can submit a valid JSON body (the
// "name=value" separator lands inside a string field) without a CORS preflight.
func TestRPCAcceptsCrossSiteSimpleRequestBodies(t *testing.T) {
	s := newServer(t)
	h := s.Handler(func(r *http.Request) (drivesync.Principal, error) {
		if c, e := r.Cookie("session"); e == nil && c.Value == "alice" {
			return alice, nil
		}
		return drivesync.Principal{}, drivesync.ErrDenied
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	// What a browser sends for <form method=post enctype=text/plain> with
	// name=`{"Op":"create","Spec":{"Name":"csrf","KeyCheck":"<44 b64 chars>"},"Page":"` value=`"}`.
	body := `{"Op":"create","Spec":{"Name":"csrf","KeyCheck":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},"Page":"="}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	req.AddCookie(&http.Cookie{Name: "session", Value: "alice"})
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("text/plain cross-site body executed an RPC: %s", strings.TrimSpace(string(b))[:80])
	}
}
