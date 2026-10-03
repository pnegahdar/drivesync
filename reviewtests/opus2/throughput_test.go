package dsreview2

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync"
)

type chunked struct {
	r io.Reader
	n int
}

func (c chunked) Read(p []byte) (int, error) {
	if len(p) > c.n {
		p = p[:c.n]
	}
	return c.r.Read(p)
}

func TestUploadThroughputWithRenewal(t *testing.T) {
	s := newServer(t)
	c := s.Client(ds.Principal{Tenant: "t", Subject: "u"})
	f, _ := mkFolder(t, c, ds.Limits{})
	const size = 64 << 20
	for _, read := range []int{16 << 10, 64 << 10} {
		tk, e := c.Reserve(bg, f.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", read), SealedSize: size})
		if e != nil {
			t.Fatal(e)
		}
		start := time.Now()
		if e = c.Upload(bg, f.ID, tk, chunked{bytes.NewReader(make([]byte, size)), read}); e != nil {
			t.Fatal(e)
		}
		d := time.Since(start)
		t.Logf("64 MiB upload with %d KiB reads: %v (%.1f MB/s, %d input reads)", read>>10, d, float64(size)/d.Seconds()/1e6, size/read)
	}
}
