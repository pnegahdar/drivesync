package sharing

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

type oneByte struct{ r io.Reader }

func (o oneByte) Read(p []byte) (int, error) { return o.r.Read(p[:1]) }

// Every Read of an upload body performs up to two durable SQLite write
// transactions (renewal before and after). A writer trickling bytes saturates
// the single global writer connection that every tenant shares.
func TestRenewalWriteAmplification(t *testing.T) {
	s := newServer(t)
	attacker := s.Client(ds.Principal{Tenant: "evil", Subject: "a"})
	af, _ := mkFolder(t, attacker, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 1000})
	victim := s.Client(ds.Principal{Tenant: "victim", Subject: "v"})
	vf, vk := mkFolder(t, victim, ds.Limits{})
	measure := func() time.Duration {
		var out []time.Duration
		for i := 0; i < 15; i++ {
			start := time.Now()
			putFile(t, victim, vf, vk, fmt.Sprintf("f%d-%d", time.Now().UnixNano(), i), 0, []byte("x"))
			out = append(out, time.Since(start))
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out[len(out)/2]
	}
	base := measure()
	const n = 3000
	var wg sync.WaitGroup
	var elapsed time.Duration
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tk, e := attacker.Reserve(bg, af.ID, ds.UploadRequest{PathID: fmt.Sprintf("%064x", w+1), SealedSize: n})
				if e != nil {
					t.Error(e)
					return
				}
				start := time.Now()
				attacker.Upload(bg, af.ID, tk, oneByte{bytes.NewReader(make([]byte, n))})
				if w == 0 && elapsed == 0 {
					elapsed = time.Since(start)
				}
				attacker.CancelUpload(bg, af.ID, tk.ID)
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	during := measure()
	close(stop)
	wg.Wait()
	t.Logf("a %d-byte upload read 1 byte at a time took %v; victim median put: %v idle -> %v with 4 trickling uploads (x%.0f)",
		n, elapsed, base, during, float64(during)/float64(base))
}
