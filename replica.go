package drivesync

import (
	"context"

	"github.com/pnegahdar/drivesync/internal/engine"
)

// Replica keeps an encrypted folder in an ordinary local directory.
type Replica struct{ replica *engine.Replica }

// Attach uses ctx for setup. The replica remains active until Close.
func Attach(ctx context.Context, c *Client, id string, key FolderKey, dir string, opts Options) (*Replica, error) {
	if key == (FolderKey{}) {
		return nil, ErrKey
	}
	r, e := engine.Attach(ctx, c.client, id, engine.FolderKey(key), dir, engine.Options(opts))
	if e != nil {
		return nil, publicError(e)
	}
	return &Replica{replica: r}, nil
}
func (r *Replica) Status() Status {
	s := r.replica.Status()
	out := Status{PendingUpBytes: s.PendingUpBytes, PendingDownBytes: s.PendingDownBytes, Conflicts: s.Conflicts, LastSync: s.LastSync, Errors: s.Errors, Placeholders: s.Placeholders, PlaceholderBytes: s.PlaceholderBytes}
	for _, f := range s.Rejected {
		out.Rejected = append(out.Rejected, Rejection{Path: f.Path, Reason: f.Reason})
	}
	for _, row := range s.Quarantined {
		name := row.Path
		if name == "" {
			name = row.PathID
		}
		out.Quarantined = append(out.Quarantined, name)
	}
	// Local skips are actionable, without exposing an additional status field.
	out.Errors = append(out.Errors, s.Skipped...)
	return out
}
func (r *Replica) Sync(ctx context.Context) error { return publicError(r.replica.Sync(ctx)) }
func (r *Replica) Close() error                   { return publicError(r.replica.Close()) }

// Retry clears rejection/quarantine backoff and acknowledges intentional mass
// removal for the next sync. It never overrides a missing or changed root identity.
func (r *Replica) Retry() { r.replica.RetryRejected() }

// Entry is one path a replica tracks.
type Entry struct {
	Path        string
	Size        int64
	Directory   bool
	Placeholder bool
}

// FetchResult reports one Fetch.
type FetchResult struct {
	// Fetched lists the placeholders this call downloaded; Bytes is their plaintext size.
	Fetched []string
	Bytes   int64
	// Present counts matched files that were already downloaded.
	Present int
	// Unmatched lists patterns that matched no tracked file.
	Unmatched []string
	Failed    []Rejection
}

// Entries lists tracked paths from local state, without the network. A pattern
// is an exact path, a folder (its whole subtree), or a glob with * ? [] and **.
// No patterns lists everything. Results are sorted by path.
func (r *Replica) Entries(patterns ...string) ([]Entry, error) {
	list, e := r.replica.Entries(patterns...)
	if e != nil {
		return nil, publicError(e)
	}
	out := make([]Entry, len(list))
	for i, v := range list {
		out[i] = Entry(v)
	}
	return out, nil
}

// Fetch downloads the on-demand placeholders matching patterns in parallel and
// returns once they are ordinary files. Downloaded files stay downloaded.
func (r *Replica) Fetch(ctx context.Context, patterns ...string) (FetchResult, error) {
	res, e := r.replica.Fetch(ctx, patterns...)
	out := FetchResult{Fetched: res.Fetched, Bytes: res.Bytes, Present: res.Present, Unmatched: res.Unmatched}
	for _, f := range res.Failed {
		out.Failed = append(out.Failed, Rejection{Path: f.Path, Reason: f.Reason})
	}
	return out, publicError(e)
}
