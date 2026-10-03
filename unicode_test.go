package drivesync

import (
	"bufio"
	"compress/gzip"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Verify the complete canonical normalization conformance corpus, including
// blocked compositions, Hangul and characters added after older OS data sets.
func TestNFCConformance(t *testing.T) {
	f, e := os.Open("testdata/NormalizationTest.txt.gz")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	r, e := gzip.NewReader(f)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	scanner := bufio.NewScanner(r)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(strings.Split(scanner.Text(), "#")[0])
		if line == "" || strings.HasPrefix(line, "@") {
			continue
		}
		fields := strings.Split(line, ";")
		if len(fields) < 5 {
			t.Fatal(line)
		}
		var col [5]string
		for i := range col {
			var runes []rune
			for _, hex := range strings.Fields(fields[i]) {
				n, e := strconv.ParseUint(hex, 16, 32)
				if e != nil {
					t.Fatal(e)
				}
				runes = append(runes, rune(n))
			}
			col[i] = string(runes)
		}
		for i, want := range []string{col[1], col[1], col[1], col[3], col[3]} {
			if got := nfc(col[i]); got != want {
				t.Fatalf("case %d col %d: %x -> %x want %x", count, i, []rune(col[i]), []rune(got), []rune(want))
			}
		}
		count++
	}
	if e = scanner.Err(); e != nil {
		t.Fatal(e)
	}
	if count < 10000 {
		t.Fatalf("incomplete corpus %d", count)
	}
	t.Logf("%d Unicode NFC conformance cases", count)
}
