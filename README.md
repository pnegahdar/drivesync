# drivesync

A standalone Go library for encrypted, Dropbox-like shared folders. Replicas keep
ordinary files on disk, so application reads need no network call or FUSE mount.
Linux uses inotify; macOS uses one FSEvents stream through purego. Both have a
periodic rescan safety net and build with `CGO_ENABLED=0`.

```go
meta, err := drivesync.OpenSQLiteMetaStore("metadata.sqlite")
if err != nil { panic(err) }
defer meta.Close()
blobs, err := drivesync.OpenDirectoryBlobStore("blob-data")
if err != nil { panic(err) }
defer blobs.Close()
server := drivesync.NewServer(meta, blobs)
// After a process restart, first stop all old writers using these stores:
if err := server.RecoverUploads(ctx); err != nil { panic(err) }
gcCtx, stopGC := context.WithCancel(ctx)
gcDone := make(chan struct{})
go func() { defer close(gcDone); server.RunGC(gcCtx, time.Second) }()
defer func() { stopGC(); <-gcDone }() // before closing stores
client := server.Client(drivesync.Principal{Tenant: "acme", Subject: "alice"})
key := drivesync.NewFolderKey() // distribute securely to authorized replicas
folder, err := client.CreateFolder(ctx, drivesync.FolderSpec{
    Name: "reference", Description: "Shared reference files",
    Limits: drivesync.Limits{MaxFileBytes: 20 << 20, MaxTotalBytes: 100 << 20, MaxFiles: 10000, MaxRows: 20000},
    KeyCheck: drivesync.KeyCheck(key),
})
if err != nil { panic(err) }
replica, err := drivesync.Attach(ctx, client, folder.ID, key, "./reference", drivesync.Options{
    Name: "laptop", StateDir: "./reference-state", // outside the attachment
})
if err != nil { panic(err) }
defer replica.Close()
status := replica.Status()
_ = status
```

Import `github.com/pnegahdar/drivesync`. Go 1.27.1 or newer is required by the
pinned dependency set. Direct dependencies are fsnotify, purego, blake3, and
modernc SQLite, and golang.org/x/text; remaining module requirements are their transitive dependencies.

The authenticated `Client` interface exposes `CreateFolder`, `GetFolder`,
`ListFolders`, `Grant`, `Revoke`, `SetLimits`, `DeleteFolder`, `Reserve`, `Upload`,
`CancelUpload`, `Commit`, `Changes`, `Download`, and long-poll `Wait`.
`Changes` aggregates bounded pages over HTTP; `Server.ChangesPage` exposes a
512-row continuation for integrations. `Server.RunGC(ctx, interval)` runs maintenance in a background worker;
`CollectGarbage(ctx)` runs one explicit pass, including tombstone compaction. Requests never collect garbage. Every server
operation also accepts an explicit principal. `Server.Subscribe` supplies an
in-process event channel with a terminal error on revocation or deletion.
`Attach` starts automatic sync; `Options.Manual` and `Replica.Sync(ctx)` allow
an embedder to schedule it. `Close`, `Status`, and `RetryRejected` complete the
replica API. Status is also saved to the private state directory for the CLI.

For HTTP, mount `server.Handler(authenticator)` and use
`NewHTTPClient(baseURL, headers)`. The authenticator must derive the opaque
principal from trusted credentials; identity fields in a request cannot choose
the caller. Use HTTPS outside localhost. RPC requests are limited to 1 MiB;
HTTP clients default to a 64 MiB metadata response limit, configurable through
`MaxResponseBytes` (negative disables it). Blob bodies stream independently.
The HTTP subscription transport is long polling rather than SSE. Waits are idle
until that folder changes, with at most 32 concurrent waits per principal.
SQLite authorities in the same process share notifications. A multi-process
embedder must distribute durable folder/grant events through `NotificationSource`
and `Notifications.Notify`; there is no polling fallback for external mutations.

`MetaStore.Transaction` is the embedding seam for durable, atomic metadata
storage. Its callback sees folder records, current file rows, reservations, a
durable garbage queue and primary-owner counters; errors roll back all changes.
Alternate stores read `ScopeFromContext`, load just that folder and its owner
counter, then call `Metadata.Prepare` before the callback and `Metadata.Finish`
afterward. Persist records and counters in the same transaction. SQLite folder
operations never load the owner's other folders or rows. Listing reads cached
folder usage; streaming authorization and paginated changes use targeted queries.
Indexed owner/grant and ticket/garbage folder columns avoid unrelated-tenant scans.
Read-only WAL connections serve folder reads, changes and authorization without
acquiring the writer. Writes select touched file rows and use a durable usage cache.
Accounting groups tickets/garbage by folder in one pass. Maintenance surveys
records on the read pool, then writes only folders needing cleanup; compaction
loads only folders with expired, physically collected tombstones. Replicas
currently hash the full local tree on each sync; incremental dirty-path scans
remain future performance work. `BlobStore` creates immutable objects, opens
streams, reports actual sizes and deletes objects. Memory and local-directory
stores are included; the memory backend holds complete objects for tests or
small in-process use, while the directory backend streams them. An embedder can
add S3/R2 without an AWS SDK dependency.
Configure quota policy, TTL and clock before serving concurrent requests.

External quota-policy and blob-size hooks are bounded to two seconds per call,
and authority transactions to five seconds. They still run while the SQLite
writer transaction is open, so slow hooks delay other writers up to that bound.
Implementations must honor context cancellation; a bounded worker pool prevents
unbounded goroutine growth if an embedding hook does not. Blob deletion runs
outside the metadata write transaction. Custom stores must implement the same
atomic rollback, immutable object and accurate size contracts.

Security and behavior:

- No grant and no folder produce the same `ErrDenied`. Principals granted owner access manage
  grants and limits; writers edit files; readers download. Explicit grants can
  cross tenants. Grants name the complete `{Tenant, Subject}` pair of nonempty
  UTF-8 strings of at most 256 bytes each (identities are never normalized).
  Raw HTTP JSON rejects malformed UTF-8 and unpaired surrogate escapes. Folders
  allow at most 256 explicit grants. The creator
  remains the immutable primary owner and quota account; owner grants delegate
  management without transferring that account. Non-primary principals see
  configured folder limits and generic owner-quota errors, never the owner's
  plan limits or private usage numbers.
- Folder labels/descriptions are control-plane fields visible to the authority.
  File paths, contents, types, modes, hashes and other file metadata are encrypted.
  Sizes, file counts, path IDs and access relationships remain visible.
- The caller supplies a 32-byte folder key. HKDF separates content, metadata and
  path MAC keys. AES-GCM chunks authenticate folder, blob, path, chunk index and
  final marker, with a fresh random salt deriving each stream key. Metadata authenticates folder and path and checks the blob binding.
  Key checks reject a wrong key before any local changes.
- Revocation/deletion terminate waits and invalidate tickets immediately. Streaming
  blob reads recheck authority on every read. Bytes already delivered or cached
  cannot be revoked; removing a principal does not rotate the shared key.
- CAS conflicts keep a durable local `name (conflict from replica timestamp-id).ext`
  copy before taking the winner. Conflict copies sync as regular files. Renames
  use atomic CAS-bound create-plus-delete commits without credit. If the new
  path cannot fit, both halves remain pending and the old remote path survives.
  Rename pairs are batched but remain indivisible when retrying limits or CAS;
  detection indexes hashes and folded paths once per sync.
  Empty directories use encrypted directory markers. Busy upload paths retry
  without conflict copies; batches preserve only paths whose versions lost.
  Tombstones retain versions until the retention horizon (`Server.TombstoneTTL`,
  30 days by default, zero disables compaction). Deletes keep the old blob's
  cleanup responsibility and charge on their row, adding no bytes or rows.
  Collection clears that charge only after successful deletion; compaction
  then removes expired tombstones. Full/incremental mode is pinned in page
  continuations. A Full pull reconciles the index and all pending maps against
  the current set before writes, preserving unsynced content. Failed local deletes
  retry independently. Missing compacted paths accept bases at/below the horizon.
  Files already matching authenticated remote hashes are adopted in
  sync when attaching an existing directory; no duplicate conflict copies.
- Downloads use authenticated staging, fsync, rename and directory fsync. An
  `os.Root` confines filesystem operations, including when parents change. Paths
  are validated after decryption; symlinks are skipped/rejected. Case collisions
  get unique stable local aliases with one remote path per local path. Alias
  selection checks exact directory-entry spelling, including parent components.
  Downloads retain at least owner read/write (0600), or directory access (0700).
- `.drivesyncignore` accepts shell-style globs, comments and directory patterns.
  Defaults exclude `.DS_Store`, editor temporary files and internal staging files.
  Ignore and collision matching conservatively fold case and normalize NFC on
  all platforms, so rules remain safe when moving to a case-insensitive volume.
  Skipped remote rows persist outside the cursor and reappear after unignoring,
  including after restart. Undecryptable or unapplicable rows are quarantined
  in durable state and `Status.Quarantined`, without re-downloading unchanged
  versions. Truncated streams and transport errors, including at the final EOF
  check, retry separately; only authentication/framing failures or actual trailing
  bytes are integrity failures. Ignored-only contents are removed on directory
  deletion; remaining tracked children keep its tombstone pending. Network and filesystem
  failures retry separately; `RetryRejected`
  also explicitly retries quarantined rows. Unreadable local files are
  reported in `Status.Skipped`. Other uploads and downloads continue.
  State stays outside the attachment and is locked against a second attachment.
- Byte limits count sealed blob bytes, sealed metadata length, and `RowCost`
  (256 bytes) per file/tombstone or queued garbage record. `MaxFileBytes` counts
  the sealed content stream; use `SealedSize` for its overhead. `MaxFiles` limits
  live entries, including directory markers. `Limits.MaxRows` caps all current
  rows and garbage records; zero selects a safety budget of 16 times `MaxFiles`,
  capped at one million (one million when `MaxFiles` is unlimited). Tombstones
  retain their row charge and any pending blob cleanup. `Quota.MaxFiles` bounds
  rows across the owner's folders.
  Other zero limits mean unlimited. Individual uploads are capped at 2^50 bytes,
  and accounting saturates instead of overflowing.
- Reservations bind principal, folder, path and base version. Set
  `UploadRequest.SessionID` to a random 128-bit hex ID per replica/state directory
  to replace your session's stale ticket. A different or absent session gets
  `ErrBusy` while a sibling ticket exists, including for the same principal.
  Replicas persist their session ID outside the shared directory. Renewals write only
  after more than half the TTL has elapsed, while every read still checks access.
  `UploadRequest.MetadataBytes` reserves metadata space; commit checks the actual
  length even if understated. Uploads reserve a complete additional copy, with
  no replacement or rename credits. Replaced blobs, queued records and staging
  remain charged until collection succeeds. Cancels that never started uploading
  charge nothing. An in-flight `Put` remains durably charged after cancellation
  until publication finishes; failed cleanup stays queued. Start `RunGC` in a
  background worker and cancel/join it before closing stores. After a process
  restart, call `RecoverUploads` only after all old writers using those stores
  have stopped, then let GC remove interrupted staging/publications. A generic
  BlobStore cannot prove that a different process has stopped publishing.
- **Every shared folder is allocated**, including grants within the same tenant.
  The first grant requires explicit `MaxTotalBytes`, `MaxRows`, and
  `MaxFileBytes` within the owner's plan. The owner's
  quota reserves exactly the byte cap and `min(MaxRows, MaxTotalBytes / RowCost)`
  rows, never a maximum of that capacity and current usage. Garbage, reservations
  and rows consume only that folder's headroom; grantee activity cannot block
  private writes or probe private usage. Allocation and limit changes reject byte
  or row caps below actual folder usage, including garbage and reservations.
  File ceilings are explicit folder choices, never copied from plan values.
  `GetFolder` / `ListFolders` return stored limits without calling pricing.
  Later plan decreases
  grandfather existing shared capacity; private writes obey the current quota.
- Only the primary owner may grow or change allocated limits. Delegated owners
  can manage grants, including revoking themselves or the last grantee. Allocation
  remains until folder deletion and physical cleanup, so revoking the last grant
  does not consult pricing policy. Quota is checked only when capacity grows.
  Revocation, downgrades, reductions and existing-row deletes never consult quota.
  Limit reductions check only folder usage and reject caps below it. Existing-row
  deletes remain legal during plan outages and over-plan usage.
  Creating new tombstones still consumes quota. Deleted folders disappear from
  access/listing immediately; queued cleanup remains durable and charged.
- Rejected files stay locally and appear in `Status`. Automatic retries wait at
  least a minute by default; a content change or `RetryRejected` retries sooner.

This prototype requires fresh authority and replica databases; no migration or
legacy-folder compatibility is provided.

Deferred: chunking/dedup, snapshots/history, FUSE, key rotation, presigned URL
issuance, and distributed push infrastructure. Encryption authenticates received
content, but does not establish freshness or availability: a malicious authority
can delete files or replay an older valid row/blob to roll them back. Keep external
backups if the authority itself is outside your trust boundary.
Synchronization preserves observed concurrent edits, but arbitrary writes through
an already-open descriptor during a filesystem replacement are not transactional
with the replica. It is also a current-row protocol: ordinary later edits and
intentional deletes do not provide historical recovery. Network retries may create
extra conflict copies, favoring retention over deduplicating identical content.

Manual testing:

```sh
export DRIVESYNC_TOKEN='your-local-test-token'
go run ./cmd/drivesync serve -data ./server-data
# In another terminal; save the returned ID and key to a private file:
go run ./cmd/drivesync create -name reference
go run ./cmd/drivesync attach -folder ID -key-file ./folder-key.hex -dir ./a -state ./a-state -name a
# In another terminal:
go run ./cmd/drivesync attach -folder ID -key-file ./folder-key.hex -dir ./b -state ./b-state -name b
go run ./cmd/drivesync status -state ./a-state
```

The CLI's server authenticator intentionally maps one bearer token to one
principal. Production applications supply their own authenticator and key delivery.
See [AGENTS.md](AGENTS.md) for security invariants and verification commands.

Verification runs on macOS arm64 with Go 1.27.1. Linux is cross-built and vetted;
its runtime tests have not been executed here. See [SECURITY_FIXES.md](SECURITY_FIXES.md)
for adversarial regressions, commands, results and performance observations.
