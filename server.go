package drivesync

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/pnegahdar/drivesync/internal/engine"
)

// ServerOptions configures authority policy and maintenance before serving.
// Zero TTLs select defaults: five-minute reservations and 30-day tombstones.
// A negative TombstoneTTL disables compaction. GCInterval defaults to one second.
type ServerOptions struct {
	Quotas                                   QuotaPolicy
	ReservationTTL, TombstoneTTL, GCInterval time.Duration
	Clock                                    func() time.Time
	// MaxWaitsPerPrincipal caps concurrent long-polls for one principal.
	// Zero keeps the default of 256. Embedders that share one principal
	// across many clients set this higher.
	MaxWaitsPerPrincipal int
}

// Server authorizes every operation independently of transport authentication.
type Server struct {
	server   *engine.Server
	interval time.Duration
}

func NewServer(meta *MetaStore, blobs BlobStore, opts ServerOptions) *Server {
	s := engine.NewServer(meta.store, blobs)
	if opts.Quotas != nil {
		s.Quotas = quotaAdapter{opts.Quotas}
	}
	if opts.ReservationTTL != 0 {
		s.ReservationTTL = opts.ReservationTTL
	}
	if opts.TombstoneTTL != 0 {
		s.TombstoneTTL = opts.TombstoneTTL
	}
	if opts.Clock != nil {
		s.Now = opts.Clock
	}
	if opts.MaxWaitsPerPrincipal > 0 {
		s.SetMaxWaitsPerPrincipal(opts.MaxWaitsPerPrincipal)
	}
	return &Server{server: s, interval: opts.GCInterval}
}

// Run recovers interrupted uploads and maintains expired reservations, garbage
// and tombstones until ctx is cancelled. SQLite metadata is one authority
// process per database file. Postgres metadata can be shared: an upload lease
// is renewed while bytes flow, and recovery retires only an expired lease.
// Cancel and join Run before closing the stores.
func (s *Server) Run(ctx context.Context) error { return publicError(s.server.Run(ctx, s.interval)) }

type Authenticator func(*http.Request) (Principal, error)

func (s *Server) Handler(auth Authenticator) http.Handler {
	return s.server.Handler(func(r *http.Request) (engine.Principal, error) {
		if auth == nil {
			return engine.Principal{}, ErrDenied
		}
		p, e := auth(r)
		return internalPrincipal(p), e
	})
}

// Client trusts p, which the embedding app must authenticate before calling.
func (s *Server) Client(p Principal) *Client {
	return &Client{client: s.server.Client(internalPrincipal(p))}
}

type quotaAdapter struct{ policy QuotaPolicy }

func (a quotaAdapter) Quota(ctx context.Context, p engine.Principal) (engine.Quota, error) {
	q, e := a.policy.Quota(ctx, Principal(p))
	return engine.Quota(q), e
}
func internalPrincipal(p Principal) engine.Principal { return engine.Principal(p) }
func publicFolder(f engine.Folder) Folder {
	return Folder{ID: f.ID, Name: f.Name, Description: f.Description, Owner: Principal(f.Owner), Role: Role(f.Role), Limits: Limits(f.Limits), Usage: Usage{Bytes: f.Usage.Bytes, Files: f.Usage.Files, Reserved: f.Usage.Reserved}}
}
func publicError(e error) error {
	var limit *engine.LimitError
	if errors.As(e, &limit) {
		return &LimitError{Limit: limit.Limit, Maximum: limit.Maximum, Requested: limit.Requested, cause: e}
	}
	return e
}
