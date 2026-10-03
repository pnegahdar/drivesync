package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStatusHidesRecoveryState(t *testing.T) {
	s, e := decodeStatus([]byte(`{"PendingUpBytes":12,"Version":99,"Watcher":"polling","Rejected":[{"Path":"big","Reason":"total bytes","Hash":"private","RetryAt":"2026-01-01T00:00:00Z"}],"Quarantined":[{"PathID":"bad-peer","Version":7}],"Skipped":["symlink"],"Errors":["offline"]}`))
	if e != nil {
		t.Fatal(e)
	}
	if s.PendingUpBytes != 12 || len(s.Rejected) != 1 || s.Rejected[0].Reason != "total bytes" || len(s.Quarantined) != 1 || s.Quarantined[0] != "bad-peer" || len(s.Errors) != 2 || s.Errors[1] != "symlink" {
		t.Fatalf("status: %+v", s)
	}
	b, e := json.Marshal(s)
	if e != nil {
		t.Fatal(e)
	}
	for _, private := range []string{"Version", "Watcher", "Hash", "RetryAt", "Skipped"} {
		if strings.Contains(string(b), private) {
			t.Fatalf("private field %s in %s", private, b)
		}
	}
	if _, e := decodeStatus([]byte(`{`)); e == nil {
		t.Fatal("malformed status accepted")
	}
}
