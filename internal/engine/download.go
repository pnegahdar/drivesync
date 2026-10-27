package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const downloadWindowBytes int64 = 512 << 20
const downloadURLTTL = 5 * time.Minute

type blobURL struct {
	url    string
	issued time.Time
}
type stagedDownload struct {
	name    string
	err     error
	release func()
}

func (s stagedDownload) cleanup() {
	if s.name != "" {
		_ = os.Remove(s.name)
	}
	if s.release != nil {
		s.release()
	}
}

// Reserve a whole window atomically. A concurrent publisher may need syncMu,
// so a caller holding it must fall back to streaming instead of waiting here.
func (r *Replica) reserveStaging(rows []Row) []func() {
	var bytes int64
	for _, row := range rows {
		if row.SealedSize < 0 || row.SealedSize > downloadWindowBytes-bytes {
			return nil
		}
		bytes += row.SealedSize
	}
	r.stagingMu.Lock()
	defer r.stagingMu.Unlock()
	if r.stagingFiles+len(rows) > 2*r.opts.Downloads || bytes > downloadWindowBytes-r.stagingBytes {
		return nil
	}
	r.stagingFiles += len(rows)
	r.stagingBytes += bytes
	releases := make([]func(), len(rows))
	for n, row := range rows {
		var once sync.Once
		releases[n] = func() {
			once.Do(func() {
				r.stagingMu.Lock()
				r.stagingFiles--
				r.stagingBytes -= row.SealedSize
				close(r.stagingChanged)
				r.stagingChanged = make(chan struct{})
				r.stagingMu.Unlock()
			})
		}
	}
	return releases
}

type slotBody struct {
	io.ReadCloser
	release func()
}

func (b slotBody) Close() error { e := b.ReadCloser.Close(); b.release(); return e }
func (r *Replica) download(ctx context.Context, row Row) (io.ReadCloser, error) {
	select {
	case r.downloadSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	release := func() { once.Do(func() { <-r.downloadSlots }) }
	stream, e := r.openDownload(ctx, row)
	if e != nil {
		release()
		return nil, e
	}
	return slotBody{stream, release}, nil
}

// No authority transport, headers, cookies, or credentials enter this client.
var blobHTTP = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}

func (r *Replica) prepareURLs(ctx context.Context, rows []Row) {
	r.urlMu.Lock()
	defer r.urlMu.Unlock()
	if r.noURLs {
		return
	}
	if len(r.urls) > 1024 {
		for id, cached := range r.urls {
			if time.Since(cached.issued) >= downloadURLTTL/2 {
				delete(r.urls, id)
			}
		}
		if len(r.urls) > 1024 {
			r.urls = nil
		}
	}
	if r.urls == nil {
		r.urls = map[string]blobURL{}
	}
	for start := 0; start < len(rows); start += 256 {
		end := min(start+256, len(rows))
		ids := []string{}
		for _, row := range rows[start:end] {
			cached, ok := r.urls[row.BlobID]
			if !ok || time.Since(cached.issued) >= downloadURLTTL/2 {
				ids = append(ids, row.BlobID)
			}
		}
		if len(ids) == 0 {
			continue
		}
		issued := time.Now()
		urls, err := r.client.DownloadURLs(ctx, r.folder, ids)
		if errors.Is(err, ErrInvalid) {
			r.noURLs = true
			r.urls = nil
			return
		}
		if err != nil || len(urls) != len(ids) {
			continue
		}
		for i, id := range ids {
			r.urls[id] = blobURL{urls[i], issued}
		}
	}
}

// Errors returned by net/http may contain the bearer URL. Never retain them.
func directError(err error) error {
	var u *url.Error
	for errors.As(err, &u) {
		err = u.Err
	}
	// An arbitrary transport error can itself contain a URL; expose only a fixed reason.
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("direct download: transfer failed")
}

type directBody struct{ io.ReadCloser }

func (b directBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e != nil && e != io.EOF {
		e = directError(e)
	}
	return n, e
}
func (r *Replica) openDownload(ctx context.Context, row Row) (io.ReadCloser, error) {
	r.prepareURLs(ctx, []Row{row})
	r.urlMu.Lock()
	u := r.urls[row.BlobID]
	r.urlMu.Unlock()
	if u.url != "" && time.Since(u.issued) < downloadURLTTL/2 {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.url, nil)
		if e == nil && req.URL.User == nil && (req.URL.Scheme == "http" || req.URL.Scheme == "https") {
			resp, e := blobHTTP.Do(req)
			if e == nil {
				if resp.StatusCode == http.StatusOK {
					return directBody{resp.Body}, nil
				}
				resp.Body.Close()
			}
		}
	}
	return r.client.Download(ctx, r.folder, row.BlobID)
}
func (r *Replica) stage(ctx context.Context, row Row) stagedDownload {
	releases := r.reserveStaging([]Row{row})
	if releases == nil {
		return stagedDownload{}
	}
	s := r.stageSealed(ctx, row)
	s.release = releases[0]
	return s
}
func (r *Replica) stageSealed(ctx context.Context, row Row) (result stagedDownload) {
	defer func() {
		var pathError *os.PathError
		if errors.As(result.err, &pathError) {
			result.err = fmt.Errorf("%w: %w", errAfterDownload, result.err)
		}
	}()
	f, e := os.CreateTemp(r.opts.StateDir, "download-")
	if e != nil {
		return stagedDownload{err: e}
	}
	result = stagedDownload{name: f.Name()}
	stream, e := r.download(ctx, row)
	if e == nil {
		var n int64
		n, e = io.Copy(f, io.LimitReader(stream, row.SealedSize+1))
		_, direct := stream.(slotBody).ReadCloser.(directBody)
		stream.Close()
		if e != nil && direct && ctx.Err() == nil && isTransferError(e) {
			// A signed URL whose body fails gets the same single proxy fallback.
			if e = f.Truncate(0); e == nil {
				_, e = f.Seek(0, 0)
			}
			if e == nil {
				stream, e = r.proxyDownload(ctx, row)
			}
			if e == nil {
				n, e = io.Copy(f, io.LimitReader(stream, row.SealedSize+1))
				stream.Close()
			}
		}
		if e == nil && n > row.SealedSize {
			e = ErrIntegrity
		}
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	result.err = e
	return result
}

// Each batch bounds both the worker count and total staged ciphertext. Serial
// publication drains it completely before the next batch can use disk space.
func (r *Replica) stageRows(ctx context.Context, rows []Row) map[string]stagedDownload {
	out := make(map[string]stagedDownload, len(rows))
	releases := r.reserveStaging(rows)
	if releases == nil {
		return out
	}
	r.prepareURLs(ctx, rows)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for n, row := range rows {
		wg.Add(1)
		go func(n int, row Row) {
			defer wg.Done()
			staged := r.stageSealed(ctx, row)
			staged.release = releases[n]
			mu.Lock()
			out[row.BlobID] = staged
			mu.Unlock()
		}(n, row)
	}
	wg.Wait()
	return out
}
func cleanupDownloads(staged map[string]stagedDownload) {
	for _, v := range staged {
		v.cleanup()
	}
}
func (r *Replica) proxyDownload(ctx context.Context, row Row) (io.ReadCloser, error) {
	select {
	case r.downloadSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	release := func() { once.Do(func() { <-r.downloadSlots }) }
	stream, e := r.client.Download(ctx, r.folder, row.BlobID)
	if e != nil {
		release()
		return nil, e
	}
	return slotBody{stream, release}, nil
}
func (r *Replica) contentStream(ctx context.Context, row Row) (io.ReadCloser, error) {
	if v, ok := r.prefetched[row.BlobID]; ok {
		if v.err != nil {
			return nil, v.err
		}
		f, e := os.Open(v.name)
		if e != nil {
			return nil, e
		}
		return &checkedReadCloser{checkedReader{f, ctx.Err}, f}, nil
	}
	return r.download(ctx, row)
}

// needsDownload excludes rows that serial apply can adopt or represent locally.
func (r *Replica) needsDownload(row Row, m FileMetadata) bool {
	if row.Deleted || m.Directory {
		return false
	}
	if r.opts.OnDemand && r.remotePlan[m.Path] {
		return false
	}
	if q, ok := r.quarantine[row.PathID]; ok && q.Version == row.Version {
		return false
	}
	if prior, ok := r.retryRows[row.PathID]; ok && prior.Version == row.Version && time.Now().Before(r.retryAt[row.PathID]) {
		return false
	}
	i, known := r.index[m.Path]
	if known && i.Version >= row.Version && !i.Awaiting {
		return false
	}
	local := r.localPath(m.Path, row.PathID)
	if r.ignore(m.Path, false, r.patterns()) || r.ignore(local, false, r.patterns()) {
		return false
	}
	var info os.FileInfo
	var statErr error
	if r.pullIndex != nil && r.opts.OnDemand {
		info, statErr = r.pullIndex.lookup(local)
	} else {
		if r.safe(local) != nil {
			return false
		}
		info, statErr = r.root.Lstat(local)
	}
	if e := statErr; e == nil {
		if r.ownsPlaceholder(i, info) {
			return !r.opts.OnDemand
		}
		if v, e := r.inspect(m.Path, local); e != nil || (!v.Directory && v.Hash == m.Hash && v.Size == m.Size) {
			return false
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return false
	} else if r.opts.OnDemand && (!known || i.Deleted || i.Placeholder) {
		return false
	}
	return true
}

// Fetch can wait for disk headroom outside syncMu. Sync itself must stream
// instead of waiting, because a staged Fetch may be waiting to publish.
func (r *Replica) stageFetchRows(ctx context.Context, rows []Row) map[string]stagedDownload {
	for {
		r.stagingMu.Lock()
		changed := r.stagingChanged
		r.stagingMu.Unlock()
		staged := r.stageRows(ctx, rows)
		if len(rows) == 0 || len(staged) > 0 || ctx.Err() != nil {
			return staged
		}
		select {
		case <-ctx.Done():
			return staged
		case <-changed:
		}
	}
}
