package engine

import (
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

// Entry is one path the replica tracks, read from local state without the network.
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

// Entries lists tracked paths matching patterns: an exact path, a folder (its
// whole subtree), or a glob with * ? [] and **. No patterns lists everything.
// Results are sorted by path.
func normalizePatterns(patterns []string) ([]string, error) {
	out := make([]string, len(patterns))
	for i, raw := range patterns {
		if raw == "." || raw == "./" {
			out[i] = "."
			continue
		}
		p := strings.TrimSuffix(strings.TrimPrefix(raw, "./"), "/")
		if p == "." || p == "" {
			return nil, ErrInvalid
		}
		if _, e := path.Match(strings.ReplaceAll(p, "**", "*"), "check"); e != nil {
			return nil, ErrInvalid
		}
		check := strings.NewReplacer("*", "x", "?", "x").Replace(p)
		if _, e := NormalizePath(check); e != nil {
			return nil, e
		}
		out[i] = p
	}
	return out, nil
}
func entryMatches(p, pattern string) bool {
	return pattern == "." || p == pattern || strings.HasPrefix(p, pattern+"/") || globPath(strings.Split(pattern, "/"), strings.Split(p, "/"))
}
func matchesAny(p string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if entryMatches(p, pattern) {
			return true
		}
	}
	return false
}
func (r *Replica) Entries(patterns ...string) ([]Entry, error) {
	normalized, e := normalizePatterns(patterns)
	if e != nil {
		return nil, e
	}
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if e = r.ctx.Err(); e != nil {
		return nil, e
	}
	out := []Entry{}
	for _, i := range r.index {
		if i.Deleted || !matchesAny(i.Path, normalized) {
			continue
		}
		size := i.Size
		if !i.Placeholder && !i.Directory {
			if e = r.safe(i.Local); e != nil {
				return nil, e
			}
			info, se := r.root.Lstat(i.Local)
			if errors.Is(se, os.ErrNotExist) {
				continue
			}
			if se != nil {
				return nil, se
			}
			if !info.Mode().IsRegular() {
				continue
			}
			size = info.Size()
		}
		out = append(out, Entry{Path: i.Path, Size: size, Directory: i.Directory, Placeholder: i.Placeholder})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

type fetchFlight struct {
	done chan struct{}
	err  error
}

// Fetch claims paths under syncMu, transfers without it, then validates the
// index version and inode again under the lock before publishing plaintext.
func (r *Replica) Fetch(ctx context.Context, patterns ...string) (FetchResult, error) {
	result := FetchResult{}
	normalized, e := normalizePatterns(patterns)
	if e != nil {
		return result, e
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	r.syncMu.Lock()
	if e = r.ctx.Err(); e != nil {
		r.syncMu.Unlock()
		return result, e
	}
	if e = r.validRoot(); e != nil {
		r.syncMu.Unlock()
		return result, e
	}
	if e = r.prepareGuides(); e != nil {
		r.syncMu.Unlock()
		return result, e
	}
	if e = r.prepareRemoteOnly(ctx); e != nil {
		r.syncMu.Unlock()
		return result, e
	}
	if r.fetching == nil {
		r.fetching = map[string]*fetchFlight{}
	}
	claimed := []IndexEntry{}
	owned := map[string]*fetchFlight{}
	joined := map[string]*fetchFlight{}
	matched := make([]bool, len(patterns))
	for _, i := range r.index {
		if e = ctx.Err(); e != nil {
			break // The common cancellation path releases all claims already made.
		}
		if i.Deleted {
			continue
		}
		for n, p := range normalized {
			if entryMatches(i.Path, p) {
				matched[n] = true
			}
		}
		if !matchesAny(i.Path, normalized) {
			continue
		}
		if i.Directory {
			if i.RemoteOnly {
				if r.ignore(i.Local, true, r.patterns()) {
					result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: "path is ignored"})
					continue
				}
				if e = r.materializeDirectory(i); e != nil {
					result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: e.Error()})
				}
			}
			continue
		}
		if !i.Placeholder {
			result.Present++
			continue
		}
		if r.ignore(i.Local, false, r.patterns()) {
			result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: "path is ignored"})
			continue
		}
		if !r.fetchTargetUnchanged(i, i.Local) {
			result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: "placeholder was removed or replaced"})
			continue
		}
		row := r.contentRow(i)
		if q, ok := r.quarantine[row.PathID]; ok && q.Version == i.Version {
			result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: "remote content quarantined; Retry to try again"})
			continue
		}
		if time.Now().Before(r.retryAt[row.PathID]) {
			result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: "download retry backoff", RetryAt: r.retryAt[row.PathID]})
			continue
		}
		if f, ok := r.fetching[i.Path]; ok {
			joined[i.Path] = f
			continue
		}
		flight := &fetchFlight{done: make(chan struct{})}
		r.fetching[i.Path] = flight
		owned[i.Path] = flight
		claimed = append(claimed, i)
	}
	r.syncMu.Unlock()
	for n, ok := range matched {
		if !ok {
			result.Unmatched = append(result.Unmatched, patterns[n])
		}
	}
	sort.Slice(claimed, func(i, j int) bool { return claimed[i].Path < claimed[j].Path })
	r.hydrateBatches(ctx, claimed, true, func(i IndexEntry, s stagedDownload) {
		r.syncMu.Lock()
		e := r.publishPlaceholder(ctx, i, i.Local, s)
		// A concurrent sync may have refreshed the placeholder. Recollect once.
		next, ok := r.index[i.Path]
		retry := errors.Is(e, ErrBusy) && ok && next.Placeholder && next.Version != i.Version
		if errors.Is(e, ErrBusy) && ok && !next.Placeholder && !next.Deleted {
			e = nil
		}
		r.syncMu.Unlock()
		if retry {
			fresh := r.stage(ctx, r.contentRow(next))
			r.syncMu.Lock()
			e = r.publishPlaceholder(ctx, next, next.Local, fresh)
			r.syncMu.Unlock()
			fresh.cleanup()
			i = next
		}
		r.syncMu.Lock()
		flight := owned[i.Path]
		flight.err = e
		delete(r.fetching, i.Path)
		delete(owned, i.Path)
		close(flight.done)
		if e != nil {
			r.placeholderRetry(i, e)
		}
		r.syncMu.Unlock()
		if e == nil {
			result.Fetched = append(result.Fetched, i.Path)
			result.Bytes += i.Size
		} else {
			result.Failed = append(result.Failed, Rejection{Path: i.Path, Reason: e.Error()})
		}
	})
	// Cancellation also releases claims that never entered the bounded window.
	r.syncMu.Lock()
	for p, f := range owned {
		f.err = ctx.Err()
		if f.err == nil {
			f.err = ErrBusy
		}
		delete(r.fetching, p)
		close(f.done)
		result.Failed = append(result.Failed, Rejection{Path: p, Reason: f.err.Error()})
	}
	r.mu.Lock()
	r.status.Quarantined = r.quarantinedStatus()
	r.mu.Unlock()
	if r.ctx.Err() == nil {
		if e := r.refreshGuides(); e != nil {
			r.addError(e)
		}
		_ = r.persistStatus()
	}
	r.syncMu.Unlock()
	for p, f := range joined {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-f.done:
			if f.err == nil {
				result.Present++
			} else {
				result.Failed = append(result.Failed, Rejection{Path: p, Reason: f.err.Error()})
			}
		}
	}
	sort.Strings(result.Fetched)
	sort.Slice(result.Failed, func(i, j int) bool { return result.Failed[i].Path < result.Failed[j].Path })
	return result, ctx.Err()
}

func (r *Replica) quarantinedStatus() []QuarantinedRow {
	out := make([]QuarantinedRow, 0, len(r.quarantine))
	for _, row := range r.quarantine {
		m, _ := OpenMetadata(r.key, r.folder, row)
		if len(row.Metadata) == 0 {
			m.Path = r.byID[indexHash(row.PathID)]
		}
		out = append(out, QuarantinedRow{Path: m.Path, PathID: row.PathID, Version: row.Version})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PathID < out[j].PathID })
	return out
}
