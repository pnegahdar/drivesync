package quota

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

type slowClient struct{ ds.Client }
type slowReader struct{ r io.Reader }

func (s slowReader) Read(p []byte) (int, error) {
	if len(p) > 16<<10 {
		p = p[:16<<10]
	}
	time.Sleep(20 * time.Millisecond) // ~800 KiB/s link
	return s.r.Read(p)
}
func (c slowClient) Upload(x context.Context, id string, t ds.Ticket, r io.Reader) error {
	return c.Client.Upload(x, id, t, slowReader{r})
}

// Reservations have a fixed TTL and no renewal; any upload that takes longer than
// the TTL fails every time, so the file never syncs (scaled: 300ms TTL ~ 5 min).
func TestUploadLongerThanTTLNeverSyncs(t *testing.T) {
	s := newServer(t)
	s.ReservationTTL = 300 * time.Millisecond
	oc := s.Client(ds.Principal{Tenant: "t", Subject: "o"})
	f, k := mkFolder(t, oc, ds.Limits{})
	r, dir := attach(t, slowClient{oc}, f, k, "r")
	write(t, dir, "big.bin", strings.Repeat("x", 512<<10))
	putFile(t, oc, f, k, "from-teammate.txt", 0, []byte("hi"))
	var e error
	for i := 0; i < 3; i++ {
		e = r.Sync(bg)
		t.Logf("attempt %d: %v", i, e)
	}
	got, _ := oc.GetFolder(bg, f.ID)
	if _, ok := files(t, dir)["from-teammate.txt"]; !ok {
		t.Errorf("BUG: downloads are blocked too while the big upload keeps failing")
	}
	if got.Usage.Files <= 1 {
		t.Errorf("BUG: file never uploads; last error %v", e)
	}
}
