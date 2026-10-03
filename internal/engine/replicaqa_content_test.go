package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// T1-H. A remote delete is applied without the ignore check. The user stops
// sharing .env (adds it to .drivesyncignore) and puts a personal token in it.
// When a teammate deletes the shared .env, apply() preserves the locally
// modified file under " (conflict from a ...).env", which no longer matches
// the ignore pattern, so the next scan uploads the secret to every grantee.
// An unmodified ignored file is simply deleted from the user's disk.
func TestQARemoteDeleteLeaksIgnoredSecretViaConflictCopy(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, ".env", "SHARED_FLAG=1\n")
	writeLocal(t, a, "keep.conf", "shared\n")
	qaSync(t, a, b)
	writeLocal(t, a, ".drivesyncignore", ".env\nkeep.conf\n")
	writeLocal(t, a, ".env", "SHARED_FLAG=1\nPERSONAL_TOKEN=ghp_private_do_not_share\n")
	qaSync(t, a)
	os.Remove(filepath.Join(b.dir, ".env"))
	os.Remove(filepath.Join(b.dir, "keep.conf"))
	qaSync(t, b)
	_ = a.Sync(qaCtx)
	_ = a.Sync(qaCtx)
	_ = b.Sync(qaCtx)
	for p, content := range filesOn(t, b) {
		if strings.Contains(content, "PERSONAL_TOKEN") {
			t.Errorf("ignored secret reached peer b as %q", p)
		}
	}
	if _, e := os.Stat(filepath.Join(a.dir, "keep.conf")); e != nil {
		t.Errorf("ignored local file keep.conf was deleted by a remote delete: %v", e)
	}
}

// T2. ScanLocal stores out[p] = v, so two local files that map to one remote
// path silently replace each other (later WalkDir name wins). After a case
// alias is renamed back to its real name (natural once the colliding file is
// gone), the next remote update is written to the stale alias and the older
// local copy is then uploaded over it.
func TestQACaseAliasRenameRollsBackRemoteUpdate(t *testing.T) {
	probe := t.TempDir()
	os.WriteFile(filepath.Join(probe, "a"), nil, 0600)
	if _, e := os.Stat(filepath.Join(probe, "A")); e != nil {
		t.Skip("needs a case-insensitive filesystem (macOS default)")
	}
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	stale := qaPut(t, c, f, k, "README.md", 0, []byte("stale readme"), 0644)
	v1 := qaPut(t, c, f, k, "Readme.md", 0, []byte("readme v1"), 0644)
	m := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "m"), "m")
	qaSync(t, m)
	alias := ""
	for p := range filesOn(t, m) {
		if strings.Contains(p, "case conflict") {
			alias = p
		}
	}
	if !strings.HasPrefix(alias, "Readme (case conflict") {
		t.Fatalf("setup: expected Readme.md to be aliased: %v", filesOn(t, m))
	}
	pid, _ := PathID(k, f.ID, "README.md")
	if _, e := c.Commit(qaCtx, f.ID, []Mutation{{PathID: pid, BaseVersion: stale.Version, Deleted: true}}); e != nil {
		t.Fatal(e)
	}
	qaSync(t, m) // README.md removed locally; the collision is gone
	if e := os.Rename(filepath.Join(m.dir, alias), filepath.Join(m.dir, "Readme.md")); e != nil {
		t.Fatal(e)
	}
	qaSync(t, m)
	qaPut(t, c, f, k, "Readme.md", v1.Version, []byte("readme v2 from a peer"), 0644)
	qaSync(t, m)
	if got := qaRemote(t, c, f, k)["Readme.md"]; got != "readme v2 from a peer" {
		t.Fatalf("peer's Readme.md update was overwritten without a conflict: remote Readme.md=%q, files on m=%v", got, filesOn(t, m))
	}
}

// T2. Peer-chosen group/other bits are applied verbatim (m.Mode|0600 and
// m.Mode|0700). One peer with umask 000/002, or a malicious writer, makes
// files and directories group/world-writable on every replica; on macOS every
// local user is in group staff.
func TestQAPeerModesMakeLocalFilesWorldWritable(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	qaPut(t, c, f, k, "tools/run.sh", 0, []byte("#!/bin/sh\necho hi\n"), 0777)
	pid, _ := PathID(k, f.ID, "tools")
	tk, e := c.Reserve(qaCtx, f.ID, UploadRequest{PathID: pid, SealedSize: SealedSize(0)})
	if e != nil {
		t.Fatal(e)
	}
	var sealed bytes.Buffer
	SealContent(&sealed, strings.NewReader(""), k, f.ID, tk.BlobID, pid)
	if e = c.Upload(qaCtx, f.ID, tk, &sealed); e != nil {
		t.Fatal(e)
	}
	meta, _ := SealMetadata(k, f.ID, pid, FileMetadata{Path: "tools", BlobID: tk.BlobID, Mode: 0777, Directory: true, Hash: "directory"})
	if _, e = c.Commit(qaCtx, f.ID, []Mutation{{PathID: pid, TicketID: tk.ID, Metadata: meta}}); e != nil {
		t.Fatal(e)
	}
	r := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "r"), "r")
	qaSync(t, r)
	fi, _ := os.Stat(filepath.Join(r.dir, "tools/run.sh"))
	di, _ := os.Stat(filepath.Join(r.dir, "tools"))
	if fi.Mode().Perm()&0022 != 0 || di.Mode().Perm()&0022 != 0 {
		t.Fatalf("peer modes applied verbatim: tools=%v tools/run.sh=%v", di.Mode().Perm(), fi.Mode().Perm())
	}
}

// T2. A file that grows while it is synced (agent logs, heartbeat/status files,
// downloads in progress) never uploads: inspect hashes to EOF past the Stat
// size, and upload requires both the same size and the same hash. Every sync
// returns ErrBusy, so Status.Errors never clears either.
func TestQAGrowingFileNeverUploads(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	logPath := filepath.Join(a.dir, "agent.log")
	qaWrite(t, logPath, strings.Repeat("boot line\n", 400000)) // ~4 MB
	ctx, cancel := context.WithCancel(qaCtx)
	defer cancel()
	var appends atomic.Int64
	go func() {
		fh, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0600)
		defer fh.Close()
		for ctx.Err() == nil {
			fh.WriteString(time.Now().String() + " heartbeat\n")
			appends.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	var last error
	for i := 0; i < 15; i++ {
		last = a.Sync(qaCtx)
		if _, ok := qaRemote(t, c, f, k)["agent.log"]; ok {
			return
		}
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	_ = a.Sync(qaCtx)
	_, uploadedAfterStop := qaRemote(t, c, f, k)["agent.log"]
	t.Fatalf("growing file never uploaded after 15 syncs (%d appends); last error: %v; uploads once growth stops: %v", appends.Load(), last, uploadedAfterStop)
}

// T2. path.Match is not gitignore: anchored ("/secrets.env") and "**/" patterns
// never match, so files users explicitly excluded are uploaded to grantees
// without any status signal.
func TestQAGitignoreStylePatternsSilentlyUploadExcludedFiles(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	writeLocal(t, a, ".drivesyncignore", "/secrets.env\n**/.env\n/build/\n")
	writeLocal(t, a, "secrets.env", "API_KEY=1")
	writeLocal(t, a, ".env", "TOKEN=2")
	writeLocal(t, a, "build/out.bin", "artifact")
	writeLocal(t, a, "notes.md", "ok")
	_ = a.Sync(qaCtx)
	remote := qaRemote(t, c, f, k)
	for _, p := range []string{"secrets.env", ".env", "build/out.bin"} {
		if _, ok := remote[p]; ok {
			t.Errorf("%s matched an ignore line but was uploaded; status errors=%q", p, a.Status().Errors)
		}
	}
}

// T2. Rows ignored locally are re-decrypted, re-resolved (localPath does
// Lstat/ReadDir) and the whole pending table is deleted and rewritten on
// every pull, even when nothing changed. A peer that syncs node_modules makes
// every idle sync of an ignoring replica proportional to that tree.
func TestQAIgnoredRowsMakeIdleSyncLinear(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	// Seed the same 3,101 authenticated rows in one transaction. Uploading
	// every fixture file tests an unrelated path and dominates race-runtime.
	const n = 3000
	if e := s.Meta.Transaction(qaCtx, func(meta *Metadata) error {
		paths := []string{"node_modules"}
		for i := 0; i < 100; i++ {
			paths = append(paths, fmt.Sprintf("node_modules/p%03d", i))
		}
		for i := 0; i < n; i++ {
			paths = append(paths, fmt.Sprintf("node_modules/p%03d/f%d.js", i%100, i))
		}
		for _, p := range paths {
			pid, _ := PathID(k, f.ID, p)
			directory := !strings.HasSuffix(p, ".js")
			hash := "directory"
			if !directory {
				hash = hashBytes([]byte(p))
			}
			blob := randomID()
			sealed, err := SealMetadata(k, f.ID, pid, FileMetadata{Path: p, BlobID: blob, Mode: 0600, Directory: directory, Hash: hash})
			if err != nil {
				return err
			}
			meta.Files[f.ID][pid] = Row{FolderID: f.ID, PathID: pid, Version: 1, BlobID: blob, Metadata: sealed, SealedSize: SealedSize(0)}
		}
		record := meta.Folders[f.ID]
		record.Folder.Version = 1
		meta.Folders[f.ID] = record
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	r, e := Attach(qaCtx, c, f.ID, k, filepath.Join(t.TempDir(), "r"), Options{Name: "r", Manual: true, StateDir: filepath.Join(t.TempDir(), "st"), Ignore: []string{"node_modules/"}})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	qaSync(t, r)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if e := r.Sync(qaCtx); e != nil {
			t.Fatal(e)
		}
	}
	idle := time.Since(start) / 5
	var pending int
	r.db.QueryRow("SELECT count(*) FROM pending").Scan(&pending)
	if pending != 3101 {
		t.Fatalf("fixture lost ignored rows: %d", pending)
	}
	t.Logf("idle sync with %d ignored rows: %v", pending, idle)
	if idle > 50*time.Millisecond {
		t.Fatalf("idle sync with an empty local tree takes %v; %d ignored rows are reprocessed and rewritten each pull", idle, pending)
	}
}

// T2. Local deletes are committed one RPC/transaction (and one notification
// fan-out) per entry; `rm -rf` of a 400-file subtree issues 401 commits.
func TestQADeletesAreCommittedOneAtATime(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	hook := &qaHookClient{Client: base}
	a := qaAttach(t, hook, f, k, filepath.Join(t.TempDir(), "a"), "a")
	for i := 0; i < 400; i++ {
		writeLocal(t, a, fmt.Sprintf("old/f%d.txt", i), fmt.Sprint(i))
	}
	for i := 0; i < 600; i++ {
		writeLocal(t, a, fmt.Sprintf("keep/f%d.txt", i), fmt.Sprint(i))
	}
	qaSync(t, a)
	os.RemoveAll(filepath.Join(a.dir, "old"))
	hook.commits = 0
	start := time.Now()
	if e := a.Sync(qaCtx); e != nil {
		t.Fatal(e)
	}
	if hook.commits > 8 {
		t.Fatalf("rm -rf of 401 entries took %d Commit calls (%v)", hook.commits, time.Since(start))
	}
}

// T2. .git is synced by default, including lock files. A transient
// .git/index.lock (or HEAD.lock/refs lock) captured by a scan is created on
// every peer, where git then refuses to run until the delete arrives (or
// forever if the origin's deletes are paused).
func TestQAGitLockFilesPropagate(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "repo/.git/HEAD", "ref: refs/heads/main\n")
	writeLocal(t, a, "repo/.git/index.lock", "")
	qaSync(t, a, b)
	if _, e := os.Stat(filepath.Join(b.dir, "repo/.git/index.lock")); e == nil {
		t.Fatalf("a transient git lock file was materialized on the peer")
	}
}

// T2. NormalizePath accepts 4096-byte paths (valid on Linux), and downloads
// go through os.Root component by component, but ScanLocal walks absolute
// paths. On macOS (PATH_MAX 1024) such files are written yet never scanned:
// local edits never upload and the skip error is permanent.
func TestQALongRemotePathNeverRescanned(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	parts := []string{}
	for i := 0; i < 5; i++ {
		parts = append(parts, fmt.Sprintf("%d-%s", i, strings.Repeat("d", 200)))
	}
	long := strings.Join(append(parts, "notes.txt"), "/")
	qaPut(t, c, f, k, long, 0, []byte("v1"), 0644)
	r := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "r"), "r")
	e := r.Sync(qaCtx)
	t.Logf("first sync (%d-byte path): %v", len(long), e)
	fh, e := r.root.OpenFile(long, os.O_WRONLY|os.O_TRUNC, 0)
	if e != nil {
		t.Skipf("download did not materialize the long path: %v", e)
	}
	fh.WriteString("v2 edited locally")
	fh.Close()
	_ = r.Sync(qaCtx)
	if got := qaRemote(t, c, f, k)[long]; got != "v2 edited locally" {
		t.Fatalf("file materialized by a download is never rescanned: remote=%q status errors=%d skipped=%q", got, len(r.Status().Errors), r.Status().Skipped)
	}
}

// T2. Default state lives in the user cache (~/Library/Caches is excluded from
// Time Machine and purged under disk pressure; ephemeral container homes lose
// ~/.cache). With state gone and the marker still in the root, Attach adopts
// the root silently and resurrects every file peers deleted meanwhile.
func TestQALostCacheStateResurrectsRemoteDeletes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, ".cache"))
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "shared")
	a, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	writeLocal(t, a, "obsolete-plan.md", "old")
	writeLocal(t, a, "current.md", "new")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	qaSync(t, a, b)
	state := a.opts.StateDir
	a.Close()
	os.Remove(filepath.Join(b.dir, "obsolete-plan.md"))
	qaSync(t, b)
	if e = os.RemoveAll(state); e != nil { // cache purge / container restart
		t.Fatal(e)
	}
	a, e = Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	_ = a.Sync(qaCtx)
	_ = b.Sync(qaCtx)
	if _, ok := filesOn(t, b)["obsolete-plan.md"]; ok {
		t.Fatalf("losing cache state silently resurrected a file deleted by a peer (state was %s)", state)
	}
}

// T2. Rename-away saves (emacs default backup-by-copying=nil, vim
// backupcopy=auto/no: rename file -> file~, create file O_TRUNC, delete file~)
// leave a gap with no file. A remote update pulled in that gap is written into
// the path, after the editor's own "changed on disk" check, and the editor's
// O_TRUNC then destroys it with no conflict copy. The window is the save gap;
// this test syncs inside it deterministically.
func TestQARemoteUpdateInRenameAwaySaveGapIsLost(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "b"), "b")
	writeLocal(t, a, "doc.md", "v1")
	qaSync(t, a, b)
	writeLocal(t, b, "doc.md", "peer edit")
	_ = b.Sync(qaCtx)
	full := filepath.Join(a.dir, "doc.md")
	os.Rename(full, full+"~")
	_ = a.Sync(qaCtx)
	os.WriteFile(full, []byte("local save"), 0600)
	_ = a.Sync(qaCtx)
	os.Remove(full + "~")
	qaSync(t, a, b)
	values := map[string]bool{}
	for _, v := range filesOn(t, b) {
		values[v] = true
	}
	if !values["peer edit"] || !values["local save"] {
		t.Fatalf("lost a version: %v", filesOn(t, b))
	}
}

// T2. A directory rename is a delete+create per file: every byte is re-sealed
// and re-uploaded, and every peer deletes its copies and re-downloads them
// (adoption only works at the same path). Moving a 4 MB tree costs 4 MB up
// plus 4 MB down per peer; for large trees the files are missing on peers
// for the whole re-download.
func TestQABulkMoveReuploadsAndRedownloadsEverything(t *testing.T) {
	s, _ := testServer(t)
	base := s.Client(owner)
	f, k := folderFor(t, base, Limits{})
	up := &qaByteCounter{Client: base}
	down := &qaByteCounter{Client: base}
	a := qaAttach(t, up, f, k, filepath.Join(t.TempDir(), "a"), "a")
	b := qaAttach(t, down, f, k, filepath.Join(t.TempDir(), "b"), "b")
	for i := 0; i < 64; i++ {
		writeLocal(t, a, fmt.Sprintf("datasets/raw/f%d.bin", i), strings.Repeat(fmt.Sprint(i), 64<<10/len(fmt.Sprint(i))))
	}
	qaSync(t, a, b)
	up.n, down.n = 0, 0
	if e := os.Rename(filepath.Join(a.dir, "datasets"), filepath.Join(a.dir, "archive-2026")); e != nil {
		t.Fatal(e)
	}
	qaSync(t, a, b)
	if down.n > 1<<20 {
		t.Fatalf("renaming a 4 MB directory uploaded %d and re-downloaded %d bytes", up.n, down.n)
	}
}

type qaByteCounter struct {
	Client
	n int64
}

func (c *qaByteCounter) Upload(ctx context.Context, id string, t Ticket, r io.Reader) error {
	cr := &countReader{Reader: r}
	e := c.Client.Upload(ctx, id, t, cr)
	c.n += cr.n
	return e
}
func (c *qaByteCounter) Download(ctx context.Context, id, blob string) (io.ReadCloser, error) {
	rc, e := c.Client.Download(ctx, id, blob)
	if e != nil {
		return rc, e
	}
	return &qaCountingRC{rc, c}, nil
}

type qaCountingRC struct {
	io.ReadCloser
	c *qaByteCounter
}

func (r *qaCountingRC) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	r.c.n += int64(n)
	return n, e
}

// T2. Ordinary local names are excluded forever: ISO timestamps with ':',
// '?', '*', '|', '"', names ending in '.' or ' ', and macOS "Icon\r". Sync
// returns nil; only a skip line in Status says the file will never sync.
func TestQACommonLocalNamesNeverSync(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	a := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "a"), "a")
	names := []string{"logs/2026-10-03T10:00:00.log", "Why?.md", "draft.", "Icon\r"}
	for _, n := range names {
		writeLocal(t, a, n, "content")
	}
	e := a.Sync(qaCtx)
	remote := qaRemote(t, c, f, k)
	missing := []string{}
	for _, n := range names {
		if _, ok := remote[n]; !ok {
			missing = append(missing, fmt.Sprintf("%q", n))
		}
	}
	for _, name := range missing {
		reported := false
		for _, rejection := range a.Status().Rejected {
			if fmt.Sprintf("%q", rejection.Path) == name && rejection.Reason != "" {
				reported = true
			}
		}
		if !reported {
			t.Fatalf("unsyncable name %q has no rejection (Sync=%v)", name, e)
		}
	}
}
