package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreationChallengeCannotSelectOrStealFolderID(t *testing.T) {
	for _, httpMode := range []bool{false, true} {
		t.Run(fmt.Sprint(httpMode), func(t *testing.T) {
			s, _ := testServer(t)
			now := time.Now()
			s.Now = func() time.Time { return now }
			factory := clients(t, s, httpMode)
			c := factory(owner)
			key := NewFolderKey()
			challenge, e := c.PrepareFolder(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			spec := FolderSpec{Name: "checked", ID: challenge.ID, CreationProof: challenge.Proof, KeyCheck: KeyCheck(key, challenge.ID)}
			other := factory(Principal{"elsewhere", owner.Subject})
			if _, e = other.CreateFolder(context.Background(), spec); !errors.Is(e, ErrInvalid) {
				t.Fatal("stolen challenge", e)
			}
			modified := spec
			modified.ID = randomID()
			modified.KeyCheck = KeyCheck(key, modified.ID)
			if _, e = c.CreateFolder(context.Background(), modified); !errors.Is(e, ErrInvalid) {
				t.Fatal("chosen ID", e)
			}
			now = now.Add(6 * time.Minute)
			if _, e = c.CreateFolder(context.Background(), spec); !errors.Is(e, ErrInvalid) {
				t.Fatal("expired challenge", e)
			}
			now = now.Add(-6 * time.Minute)
			f, e := c.CreateFolder(context.Background(), spec)
			if e != nil || f.ID != challenge.ID || CheckKey(key, f.KeyCheck) != nil {
				t.Fatal("valid challenge", f, e)
			}
			spec.Name = "replayed"
			if _, e = c.CreateFolder(context.Background(), spec); !errors.Is(e, ErrConflict) {
				t.Fatal("challenge replay", e)
			}
			folders, e := c.ListFolders(context.Background())
			if e != nil || len(folders) != 1 || folders[0].Name != "checked" {
				t.Fatal("replay overwrote folder", folders, e)
			}
			if bytes.Equal(KeyCheck(key, f.ID), f.KeyCheck) {
				t.Fatal("salt reused")
			}
		})
	}
}

func TestMassDeleteAcknowledgmentAndFreshRootAdoption(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, key := folderFor(t, c, Limits{})
	r := replicaFor(t, c, f, key, "root", true)
	for i := range 6 {
		writeLocal(t, r, fmt.Sprint(i), "keep")
	}
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	for i := range 6 {
		if e := os.Remove(filepath.Join(r.dir, fmt.Sprint(i))); e != nil {
			t.Fatal(e)
		}
	}
	if e := r.Sync(context.Background()); e == nil || len(r.Status().Errors) == 0 {
		t.Fatal("unacknowledged mass delete", e)
	}
	got, _ := c.GetFolder(context.Background(), f.ID)
	if got.Usage.Files != 6 {
		t.Fatal("mass delete propagated", got.Usage)
	}
	r.RetryRejected()
	if e := r.Sync(context.Background()); e != nil {
		t.Fatal(e)
	}
	got, _ = c.GetFolder(context.Background(), f.ID)
	if got.Usage.Files != 0 {
		t.Fatal("acknowledged removal not committed", got.Usage)
	}
	marker, e := os.ReadFile(filepath.Join(r.dir, rootMarker))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(r.dir, r.dir+"-old"); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(r.dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(r.dir, rootMarker), marker, 0600); e != nil {
		t.Fatal(e)
	}
	if e = r.Sync(context.Background()); e == nil {
		t.Fatal("changed inode was not paused")
	}
	r.RetryRejected()
	if e = r.Sync(context.Background()); e != nil {
		t.Fatal("fresh adoption failed", e)
	}
}

func TestStateSymlinkIntoAttachmentIsRejectedBeforeMutation(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	link := filepath.Join(base, "state")
	if e := os.Symlink(dir, link); e != nil {
		t.Fatal(e)
	}
	if _, e := prepareState(dir, randomID(), filepath.Join(link, "nested")); e == nil {
		t.Fatal("state alias into attachment accepted")
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) != 0 {
		t.Fatal("invalid state path mutated attachment", entries, e)
	}
}

type compactAfterFirstPage struct {
	testStore
	compact func() error
	fired   bool
}

func (m *compactAfterFirstPage) changesPage(ctx context.Context, p Principal, id string, after, until uint64, page string) (Delta, error) {
	d, e := m.testStore.changesPage(ctx, p, id, after, until, page)
	if e == nil && after > 0 && page == "" && d.Next != "" && !m.fired {
		m.fired = true
		e = m.compact()
	}
	return d, e
}

func TestInProcessChangesRestartsFullAfterCompactionBetweenPages(t *testing.T) {
	s, meta := testServer(t)
	ctx := context.Background()
	c := s.Client(owner)
	f, key := folderFor(t, c, Limits{})
	row := put(t, c, f, key, "retained", 0, []byte("keep"))
	for start := 0; start < 600; start += 200 {
		mut := []Mutation{}
		for i := start; i < start+200; i++ {
			pid, _ := PathID(key, f.ID, fmt.Sprintf("gone/%d", i))
			mut = append(mut, Mutation{PathID: pid, Deleted: true})
		}
		if _, e := c.Commit(ctx, f.ID, mut); e != nil {
			t.Fatal(e)
		}
	}
	s.TombstoneTTL = time.Hour
	s.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	reader := &compactAfterFirstPage{testStore: meta}
	// Use a separate authority on the same WAL store to keep the reader stable.
	maintenance := NewServer(meta, s.Blobs)
	maintenance.Now, maintenance.TombstoneTTL = s.Now, s.TombstoneTTL
	reader.compact = func() error { return maintenance.CompactTombstones(ctx) }
	s.Meta = reader
	d, e := c.Changes(ctx, f.ID, row.Version)
	if e != nil || !reader.fired || !d.Full || len(d.Rows) != 1 || d.Rows[0].PathID != row.PathID {
		t.Fatal("incremental result after compaction", d.Full, len(d.Rows), e)
	}
}

func TestMaintenanceRetiresFailedUploadAcknowledgmentWithoutCancellation(t *testing.T) {
	s, meta := testServer(t)
	c := s.Client(owner)
	ctx := context.Background()
	f, key := folderFor(t, c, Limits{MaxTotalBytes: RowCost + 1, MaxFileBytes: 1, MaxRows: 1})
	pid, _ := PathID(key, f.ID, "retry")
	req := UploadRequest{PathID: pid, SealedSize: 1}
	ticket, e := c.Reserve(ctx, f.ID, req)
	if e != nil {
		t.Fatal(e)
	}
	sqlite, ok := meta.(*SQLiteMetaStore)
	if !ok {
		// Postgres has no SQLite trigger. Lease recovery is TestMultiAuthorityRecovery.
		t.Skip("SQLite trigger aborts the acknowledgement write")
	}
	if _, e = sqlite.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE ON tickets WHEN json_extract(NEW.data,'$.Uploaded')=1 BEGIN SELECT RAISE(ABORT,'failed acknowledgment'); END`); e != nil {
		t.Fatal(e)
	}
	if e = c.Upload(ctx, f.ID, ticket, strings.NewReader("x")); e == nil {
		t.Fatal("ack fault did not fire")
	}
	if _, e = sqlite.db.Exec("DROP TRIGGER fail_ack"); e != nil {
		t.Fatal(e)
	}
	// No CancelUpload and no clock advance: ordinary maintenance must recover.
	if e = s.CollectGarbage(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Reserve(ctx, f.ID, req); e != nil {
		t.Fatal("completed publication pins capacity", e)
	}
}
