package drivesync

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// ChangesPage returns at most 512 current rows, with an opaque continuation.
// Until pins the initial folder version. Rows concurrently updated beyond it
// are returned by the next incremental sync. Changes aggregates pages for callers.
func (s *Server) ChangesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	if reader, ok := s.Meta.(interface {
		changesPage(context.Context, Principal, string, uint64, uint64, string) (Delta, error)
	}); ok {
		return reader.changesPage(ctx, p, id, after, until, page)
	}
	d, e := s.Changes(ctx, p, id, after)
	if e != nil {
		return Delta{}, e
	}
	if until == 0 {
		until = d.Version
	}
	if until < after || until > d.Version {
		return Delta{}, ErrInvalid
	}
	var cv uint64
	var cp string
	if page != "" {
		parts := strings.Split(page, "/")
		if len(parts) != 2 || !validPathID(parts[1]) {
			return Delta{}, ErrInvalid
		}
		cv, e = strconv.ParseUint(parts[0], 10, 64)
		if e != nil || cv < after || cv > until {
			return Delta{}, ErrInvalid
		}
		cp = parts[1]
	}
	out := Delta{Version: until}
	for _, r := range d.Rows {
		if r.Version > until || r.Version < cv || (r.Version == cv && r.PathID <= cp) {
			continue
		}
		if len(out.Rows) == 512 {
			last := out.Rows[len(out.Rows)-1]
			out.Next = fmt.Sprintf("%d/%s", last.Version, last.PathID)
			break
		}
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}
func parsePage(page string, after, until uint64) (uint64, string, error) {
	if page == "" {
		return after, "", nil
	}
	parts := strings.Split(page, "/")
	if len(parts) != 2 || !validPathID(parts[1]) {
		return 0, "", ErrInvalid
	}
	cv, e := strconv.ParseUint(parts[0], 10, 64)
	if e != nil || cv < after || cv > until {
		return 0, "", ErrInvalid
	}
	return cv, parts[1], nil
}
