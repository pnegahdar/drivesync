package engine

import (
	"errors"
	"testing"
)

func TestSecondWaitHitsTheLimit(t *testing.T) {
	n := newNotifications()
	n.setMaxWaits(1)
	release, err := n.acquire(Principal{Tenant: "t", Subject: "owner"}, "folder")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err = n.acquire(Principal{Tenant: "t", Subject: "owner"}, "folder"); !errors.Is(err, ErrWaitLimit) {
		t.Fatalf("second wait: %v", err)
	}
	n.setMaxWaits(0)
	n.mu.Lock()
	got := n.maxWaits
	n.mu.Unlock()
	if got != defaultMaxWaitsPerPrincipal {
		t.Fatalf("zero restores %d, got %d", defaultMaxWaitsPerPrincipal, got)
	}
}
