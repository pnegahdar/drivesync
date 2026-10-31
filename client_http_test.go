package drivesync

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Authority requests go through the injected client, so a transport that adds
// credentials authenticates them; a nil client behaves as NewHTTPClient.
func TestAnInjectedClientCarriesAuthorityRequests(t *testing.T) {
	s := apiServer(t, ServerOptions{})
	handler := s.Handler(func(r *http.Request) (Principal, error) {
		if r.Header.Get("Authorization") != "Bearer rotated" {
			return Principal{}, errors.New("unauthenticated")
		}
		return Principal{"t", "u"}, nil
	})
	host := httptest.NewServer(handler)
	defer host.Close()
	injected := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer rotated")
		return http.DefaultTransport.RoundTrip(r)
	})}
	if _, e := NewHTTPClientWithClient(host.URL, nil, injected).CreateFolder(context.Background(), FolderSpec{Name: "folder"}, NewFolderKey()); e != nil {
		t.Fatalf("injected client: %v", e)
	}
	if _, e := NewHTTPClientWithClient(host.URL, nil, nil).CreateFolder(context.Background(), FolderSpec{Name: "other"}, NewFolderKey()); e == nil {
		t.Fatal("a nil client authenticated without the transport's credentials")
	}
}
