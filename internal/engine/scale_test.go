package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Seed authenticated rows in one authority transaction. There is intentionally
// no content blob: an on-demand attach must never try downloading any of them.
func seedScaleRows(t testing.TB, s *Server, f Folder, k FolderKey, dirs, files int) int {
	t.Helper()
	count := 0
	e := s.Meta.Transaction(context.Background(), func(m *Metadata) error {
		add := func(p string, directory bool) error {
			pid, e := PathID(k, f.ID, p)
			if e != nil {
				return e
			}
			meta := FileMetadata{Path: p, BlobID: randomID(), Hash: strings.Repeat("a", 64), Size: 10, Mode: 0644, Directory: directory}
			if directory {
				meta.Hash, meta.Size, meta.Mode = "directory", 0, 0755
			}
			sealed, e := SealMetadata(k, f.ID, pid, meta)
			if e != nil {
				return e
			}
			m.Files[f.ID][pid] = Row{FolderID: f.ID, PathID: pid, BlobID: meta.BlobID, Metadata: sealed, Version: 1, SealedSize: SealedSize(meta.Size)}
			count++
			return nil
		}
		for d := 0; d < dirs; d++ {
			parent := fmt.Sprintf("folder%03d", d)
			if e := add(parent, true); e != nil {
				return e
			}
			for n := 0; n < files; n++ {
				if e := add(fmt.Sprintf("%s/file%06d", parent, n), false); e != nil {
					return e
				}
			}
		}
		folder := m.Folders[f.ID]
		folder.Folder.Version = 1
		m.Folders[f.ID] = folder
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return count
}

// DRIVESYNC_MEASURE=1 runs every timing shape; normal suites retain the
// 100k-row snapshot attach and its guide/index assertions.
func TestOnDemandScale(t *testing.T) {
	measure := os.Getenv("DRIVESYNC_MEASURE") == "1"
	for _, size := range []struct {
		name        string
		dirs, files int
	}{{"1k", 1, 1000}, {"10k", 1, 10000}, {"10k-expanded", 1, 10000}, {"10k-100folders", 100, 100}, {"100k", 1, 100000}, {"256x400", 256, 400}} {
		if !measure && size.name != "100k" {
			continue
		}
		t.Run(size.name, func(t *testing.T) {
			s, _ := testServer(t)
			base := s.Client(owner)
			f, k := folderFor(t, base, Limits{})
			n := seedScaleRows(t, s, f, k, size.dirs, size.files)
			store := &signedTestStore{BlobStore: s.Blobs}
			store.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
				if req.Header.Get("Authorization") != "" {
					t.Error("authority headers in snapshot")
				}
				stream, e := store.Open(req.Context(), parts[0], parts[1])
				if e != nil {
					http.Error(w, "missing", 404)
					return
				}
				defer stream.Close()
				io.Copy(w, stream)
			}))
			defer store.server.Close()
			s.Blobs = store
			for _, snapshot := range []bool{false, true} {
				if !measure && !snapshot {
					continue
				}
				if snapshot && size.name != "100k" {
					continue
				}
				counter := &changesCounter{Client: base, noSnapshot: !snapshot}
				c := &lazyCounter{Client: counter}
				d := t.TempDir()
				start := time.Now()
				perFolder := 1000
				if size.name == "10k-expanded" {
					perFolder = 10000
				}
				r, e := Attach(syncCtx, c, f.ID, k, filepath.Join(d, "files"), Options{MaxFolderPlaceholders: perFolder, Manual: true, OnDemand: true, GuideName: "DRIVE-GUIDE.md", Guide: "Fetch files before reading them.", StateDir: filepath.Join(d, "state")})
				if e != nil {
					t.Fatal(e)
				}
				if e = r.Sync(syncCtx); e != nil {
					r.Close()
					t.Fatal(e)
				}
				elapsed := time.Since(start)
				if size.files > 1000 && r.collapsed["folder000"] {
					guide, e := os.ReadFile(filepath.Join(r.dir, "folder000", "DRIVE-GUIDE.md"))
					if e != nil || !strings.Contains(string(guide), fmt.Sprintf("… and %d more", size.files-1000)) {
						t.Fatal("guide inventory was not bounded", e)
					}
				}
				if len(r.index) != n || c.downloads.Load() != 0 || c.reserves.Load() != 0 {
					t.Fatal(len(r.index), n, c.downloads.Load(), c.reserves.Load())
				}
				physical, remote := 0, 0
				for _, i := range r.index {
					if physicalPlaceholder(i) {
						physical++
					}
					if i.RemoteOnly {
						remote++
					}
				}
				if physical > 20000 || r.Status().PlaceholderBytes != int64(size.dirs*size.files*10) {
					t.Fatal(physical, r.Status())
				}
				wantRows := int64(n)
				if snapshot {
					wantRows = 0
				}
				if counter.rows.Load() != wantRows {
					t.Fatal(counter.rows.Load(), wantRows)
				}
				t.Logf("snapshot=%v attach=%s rows=%d physical_placeholders=%d remote_only=%d collapsed=%d Changes_rows=%d", snapshot, elapsed, n, physical, remote, len(r.collapsed), counter.rows.Load())
				if e = r.Close(); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
