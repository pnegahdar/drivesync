package confirmreview

import (
	"testing"
	"time"
)

// 6,000 owners with one empty folder each: every CollectGarbage pass exceeds
// the 5 s transaction deadline in Metadata.Finish and is rolled back, so GC
// never completes (garbage stays charged, deleted folders are never purged).
func TestGCNeverCompletesWithSixThousandOwners(t *testing.T) {
	s, m := newServer(t)
	seedOwners(t, s, m, 6000, nil)
	for i := 0; i < 2; i++ {
		start := time.Now()
		if e := s.CollectGarbage(bg); e != nil {
			t.Fatalf("GC pass %d failed after %v: %v", i, time.Since(start), e)
		}
	}
}
