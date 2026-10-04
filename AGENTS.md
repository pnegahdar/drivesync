# Contributor guide

drivesync is a standalone Go module. Keep direct dependencies to fsnotify,
purego, blake3, modernc SQLite, pgx, embedded-postgres, golang.org/x/text, and
the standard library.
Support pure-Go Linux and macOS builds. Security takes priority over convenience.
[SECURITY.md](SECURITY.md) is the public statement of these rules.

## Security

- Authorize every authority operation inside the server, including reserve,
  upload, cancellation, commit, download, and subscriptions. Transport
  authentication is an independent boundary. Denied and nonexistent stay
  indistinguishable.
- Principals are opaque nonempty UTF-8 tenant/subject pairs. Authorize with
  both components. Reject invalid UTF-8. Bound each component to 256 bytes and
  grants to 256 per folder. Only an owner may manage grants, delete a folder,
  or change limits. The creator stays the immutable primary owner and quota
  account.
- Tickets and blobs bind principal, folder, path, and base version. Callers
  cannot reuse a reference across folders, paths, tenants, or principals.
  Revocation and deletion invalidate open waits and outstanding tickets.
- The server never receives plaintext file names, contents, or file metadata.
  Separate keys with HKDF. Bind metadata and chunk authentication to the
  folder, blob, path, and chunk position. Reject truncation, reordering,
  extension, and substitution. Check the folder key before attachment.
- Decrypted peer paths are untrusted. Validate portable relative paths. Reject
  traversal, absolute paths, NUL, and reserved names. Refuse symlinked
  components and confine operations with `os.Root`. Case collisions keep both
  contents.
- Stream file bytes. Authenticate downloads before publishing them with fsync
  and rename. Keep replica state outside the shared tree, by default in the
  user cache. Refuse unrelated nonempty state, nested or overlapping
  attachments, and state aliases into an attachment. Reserve `.drivesync*`
  names. Bind state to a random root marker and inode. Device numbers only
  bound one traversal. Never propagate deletes from a changed root. `Retry`
  rebinds by dropping the old index and adopting files by authenticated hash.
  Refuse symlinked roots. Hold a lifetime flock on the root marker.
- Pause mass removal until `Retry`, and keep that acknowledgement until the
  guard passes. At least five tracked entries and an 80% unexplained
  disappearance of files or bytes is a mass removal. Matching moves are not.
  Block foreign root and state markers. A walk error on the root aborts the
  scan. Filesystem errors other than ENOENT mean the entry may still be
  present. Unsupported entries block their whole subtree. Never delete
  ignored contents because a remote tombstone arrived. Only `.DS_Store` and
  `Thumbs.db` may be removed as known junk. `.git/` is ignored by default.
- Preserve losing writes as conflict copies before replacing them. Compare-and-swap
  batches are atomic. Keep tombstone versions so a path can be deleted and
  created again.
- Reserve quota atomically, including concurrent folder and owner usage. Inspect
  stored bytes at commit. Reservations expire. Recheck changed limits at commit.
  Allow deletes above a lowered quota. Keep rejected files locally, with a
  reason and backoff in replica status.
- A folder with any grantee besides its primary owner, including a same-tenant
  grantee, needs byte and row caps. Charge exactly those capacities to the
  primary owner. Folder garbage and tickets never consume private headroom.
  Only the primary owner changes allocations. Other principals see folder limits
  and generic owner-quota failures. Saturate accounting and cap request sizes.
- Charge sealed metadata, the fixed row cost, tombstones, and queued garbage.
  Enforce owner row budgets with folder-scoped queries and transactional owner
  counters. Collect garbage in the background. Keep an in-flight publication
  charged until it finishes. Cancelling an unused ticket charges nothing.
  Persist cleanup before freeing a physical blob. Collect cancelled and expired
  uploads and their staging files.
- Preserve busy paths. Retry only actual compare-and-swap losers. Commit renames
  atomically. A full-folder rename stays local and pending, and does not delete
  the old remote path. Quarantine unreadable or unauthenticated rows without
  blocking unrelated sync. Do not download an unchanged quarantined row again.
  Retry transient transfers separately. Lstat before opening, and use
  nonblocking no-follow opens. Renew a ticket only after more than half of its
  TTL has elapsed.
- Sharing requires an explicit per-file cap within the plan. Reject allocation
  or limit changes below current folder bytes and rows, including garbage and
  reservations. `GetFolder` and `ListFolders` return stored limits and do not
  call pricing. Wait only on that folder's events, cap waits per principal, and
  propagate events across authorities in the same process. Compact expired
  tombstones. Reconcile cursors behind the horizon before writes. Adopt files
  whose authenticated hash, type, and size already match. Apply case-rename
  deletes before creates.
- An early stream EOF is transient. Authentication and framing failures are
  quarantined. If every advertised sealed byte arrives but the framing is
  truncated, quarantine the stored blob. Otherwise retry with backoff. Local
  obstacles stay retryable. Revocation, downgrades, limit reductions, and
  deletes of existing rows do not query quota. Only allocation growth queries
  pricing. Allocations persist until deletion and cleanup.
- Sync parents of a new directory before acknowledging it. Match ignores under
  NFC and case folding, and revisit skipped rows after a rule change.
- macOS uses FSEvents through purego. Linux uses inotify. Periodic rescans
  remain the safety net. A kqueue watch per file is not sufficient.

Protocol and accounting records stay in `internal/engine`. The public package
manages folders and attaches replicas. `Folder`, `Usage`, and `Status` are the
caller-facing state. Policy, clock, and TTLs belong in `ServerOptions`.
Maintenance is `Run`. Metadata is SQLite or Postgres. `BlobStore` and `QuotaPolicy`
stay pluggable.

## Testing

Behavior changes need all of the following. The race and pure-Go commands run
the public package, `internal/engine`, and `reviewtests`.

```sh
go test -race ./... -count=1 -timeout 180s
CGO_ENABLED=0 go test ./... -count=1 -timeout 120s
CGO_ENABLED=0 GOOS=linux go vet ./...
outdir=$(mktemp -d)
CGO_ENABLED=0 GOOS=linux go test -c -o "$outdir/drivesync.test" .
CGO_ENABLED=0 GOOS=linux go test -c -o "$outdir/engine.test" ./internal/engine
CGO_ENABLED=0 GOOS=linux go build -o "$outdir/drivesync" ./cmd/drivesync
rm -rf "$outdir"
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
go test -race ./internal/engine -run '^TestWatcherPropagation$' -count=1
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x .
```

`TestOneUnstageableFileBlocksAllLaterUploadsAndDeletes` changes a process-wide
limit. Run it alone:

```sh
ISOLATED_RLIMIT=1 go test ./reviewtests/disk -run '^TestOneUnstageableFile' -count=1
```

Keep the owner, writer, reader, other-tenant, ungranted, revoked, and anonymous
matrix on both transports. Exercise guessed ids, stolen blobs and tickets,
revocation and deletion, crypto tampering, malicious paths, exact quota
boundaries, racing reservations, expiry, size lies, lowered quotas, batch
rollback, and crash restart. Keep seeded three-replica simulations with
concurrent writes, create, modify, delete, rename, and mkdir, plus network
failures, convergence, and retained conflict contents. Record the seed when a
simulation fails.

Native watcher coverage is `TestWatcherPropagation` in `internal/engine`. Run
it on the operating system you claim. A cross-build or a manual sync is not
watcher coverage. Root fuzz and benchmark adapters must keep calling the real
algorithms. Every test uses a temporary directory. Tests of default state set
HOME and the cache variables into that directory.
