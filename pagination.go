package drivesync

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ChangesPage returns at most 512 rows. The continuation carries the initial
// full/incremental decision and horizon; compaction cannot switch modes mid-pull.
// Full pages use path order so concurrent updates remain in the current set.
func (s *Server) ChangesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	if reader, ok := s.Meta.(interface {
		changesPage(context.Context, Principal, string, uint64, uint64, string) (Delta, error)
	}); ok {
		return reader.changesPage(ctx, p, id, after, until, page)
	}
	d, e := s.Changes(ctx, p, id, 0)
	if e != nil {
		return Delta{}, e
	}
	if after > d.Version {
		return Delta{}, ErrInvalid
	}
	if until == 0 {
		until = d.Version
	}
	if until < after || until > d.Version {
		return Delta{}, ErrInvalid
	}
	mode, e := pageMode(page, after, until, d.Horizon)
	if e != nil {
		return Delta{}, e
	}
	sort.Slice(d.Rows, func(i, j int) bool {
		if mode.full || d.Rows[i].Version == d.Rows[j].Version {
			return d.Rows[i].PathID < d.Rows[j].PathID
		}
		return d.Rows[i].Version < d.Rows[j].Version
	})
	out := Delta{Version: until, Horizon: mode.horizon, Full: mode.full}
	for _, r := range d.Rows {
		if mode.full {
			if r.PathID <= mode.path {
				continue
			}
		} else if r.Version <= after || r.Version > until || r.Version < mode.version || r.Version == mode.version && r.PathID <= mode.path {
			continue
		}
		if len(out.Rows) == 512 {
			out.Next = mode.next(out.Rows[511])
			break
		}
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}

type pageCursor struct {
	full             bool
	horizon, version uint64
	path             string
}

func pageMode(page string, after, until, horizon uint64) (pageCursor, error) {
	c := pageCursor{full: after == 0 || after < horizon, horizon: horizon, version: after}
	if page == "" {
		return c, nil
	}
	parts := strings.Split(page, "/")
	if len(parts) != 4 || !validPathID(parts[3]) || (parts[0] != "0" && parts[0] != "1") {
		return c, ErrInvalid
	}
	var e error
	c.full = parts[0] == "1"
	c.horizon, e = strconv.ParseUint(parts[1], 10, 64)
	if e != nil || c.horizon > horizon {
		return c, ErrInvalid
	}
	c.version, e = strconv.ParseUint(parts[2], 10, 64)
	if e != nil || c.version > until || !c.full && c.version < after || c.full != (after == 0 || after < c.horizon) {
		return c, ErrInvalid
	}
	c.path = parts[3]
	return c, nil
}
func (c pageCursor) next(r Row) string {
	full := 0
	if c.full {
		full = 1
	}
	version := r.Version
	if c.full {
		version = 0
	}
	return fmt.Sprintf("%d/%d/%d/%s", full, c.horizon, version, r.PathID)
}
