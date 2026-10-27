package engine

import (
	"context"
	"errors"
	"net/http"
)

// Snapshot is an optional acceleration: every failure uses the ordinary feed.
// Requests use the blob transport, which has no authority credentials.
func (r *Replica) initialSnapshot(ctx context.Context) (Delta, bool) {
	if r.noSnapshot {
		return Delta{}, false
	}
	version, u, e := r.client.Snapshot(ctx, r.folder)
	if errors.Is(e, ErrInvalid) {
		r.noSnapshot = true
	}
	if e != nil || version == 0 || u == "" {
		return Delta{}, false
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if e != nil || req.URL.User != nil || (req.URL.Scheme != "https" && req.URL.Scheme != "http") {
		return Delta{}, false
	}
	resp, e := blobHTTP.Do(req)
	if e != nil {
		return Delta{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Delta{}, false
	}
	delta, e := DecodeSnapshot(directBody{resp.Body}, r.folder, version)
	if e != nil {
		return Delta{}, false
	}
	for _, row := range delta.Rows {
		if _, e = OpenMetadata(r.key, r.folder, row); e != nil {
			return Delta{}, false
		}
	}
	return delta, true
}
