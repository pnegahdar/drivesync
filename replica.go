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
	out := Status{PendingUpBytes: s.PendingUpBytes, PendingDownBytes: s.PendingDownBytes, Conflicts: s.Conflicts, LastSync: s.LastSync, Errors: s.Errors}
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
