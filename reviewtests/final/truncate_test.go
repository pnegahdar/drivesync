package finalreview

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

type abortAfter struct {
	http.ResponseWriter
	n, limit int
}

func (w *abortAfter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.limit {
		w.ResponseWriter.Write(p[:w.limit-w.n])
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler) // connection reset mid-body
	}
	w.n += len(p)
	return w.ResponseWriter.Write(p)
}
func (w *abortAfter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Finding: one transport truncation during an HTTP download is classified as
// ErrIntegrity, so the unchanged row is quarantined and never retried.
func TestTruncatedHTTPDownloadIsQuarantinedForever(t *testing.T) {
	s, _ := newServer(t)
	alice := ds.Principal{Tenant: "acme", Subject: "alice"}
	var aborts atomic.Int32
	aborts.Store(1)
	inner := s.Handler(func(r *http.Request) (ds.Principal, error) { return alice, nil })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/download/") && aborts.Add(-1) >= 0 {
			w = &abortAfter{ResponseWriter: w, limit: 4096}
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	c := ds.NewHTTPClient(srv.URL, http.Header{})
	f, k := mkFolder(t, c, ds.Limits{})
	data := bytes.Repeat([]byte("x"), 200<<10)
	putFile(t, c, f, k, "report.pdf", 0, data)

	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, e := ds.Attach(bg, c, f.ID, k, dir, ds.Options{Name: "b", StateDir: filepath.Join(base, "state"), Manual: true, RetryInterval: time.Nanosecond})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	t.Logf("first sync: %v", r.Sync(bg))
	for i := 0; i < 3; i++ {
		t.Logf("later sync %d: %v", i, r.Sync(bg))
	}
	st := r.Status()
	t.Logf("quarantined=%v errors=%v", st.Quarantined, st.Errors)
	if _, e := os.Stat(filepath.Join(dir, "report.pdf")); e != nil {
		t.Errorf("file never downloaded after a single transient truncation: %v", e)
	}
}
