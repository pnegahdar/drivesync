# Contributor rules

This is a standalone generic Go library. Do not depend on product/application
code. Keep direct dependencies to fsnotify, purego, blake3, modernc SQLite, golang.org/x/text and the
standard library. Support pure-Go Linux and macOS builds.

Security takes priority over convenience:

- Authorize every authority operation inside Server, including reserve, upload,
  cancellation, commit, download and subscriptions. Transport authentication is
  an independent boundary. Denied and nonexistent must remain indistinguishable.
- Principals are opaque nonempty UTF-8 tenant/subject pairs; never authorize by
  subject alone. Reject invalid UTF-8; JSON replacement must not alias identities.
  Bound principal components to 256 bytes and grants to 256 per folder.
  Only principals with owner access may manage grants, delete folders and change
  limits. Keep the creator as immutable primary owner and quota account.
- Server-generated tickets and blobs bind principal, folder, path and base version.
  Never let callers steal references across folders, paths, tenants or principals.
  Revocation and deletion must invalidate open waits and outstanding tickets.
- The server must never receive plaintext file names, contents or metadata. Keep
  key separation with HKDF, keyed path IDs, folder/path-bound metadata AEAD and
  chunk AEAD binding folder, blob, path, index and final flag. Reject truncation,
  reordering, extension and substitution. Check the folder key before attachment.
- Decrypted peer paths are untrusted. Validate portable relative paths, reject
  traversal, absolute paths, NUL and reserved names, refuse symlinked components,
  and confine operations with os.Root. Case collisions must preserve both contents.
- Stream file bytes; stage and authenticate downloads before publishing them with
  fsync and rename. Keep replica SQLite state outside the shared tree.
- Preserve losing writes in conflict copies before replacing them. CAS batches
  are atomic. Keep tombstone versions so deleting and recreating a path works.
- Reserve quota atomically, including concurrent folder and owner usage. Never
  trust uploaded size claims; inspect stored bytes at commit. Reservations expire.
  Recheck changed limits at commit, and allow deletes even above lowered quota.
  Keep rejected files locally, with reasons and backoff in replica status.
- Allocate every folder with a grantee besides its primary owner, including same-tenant
  grantees. Require byte and row caps and charge exactly those capacities to the
  primary owner; folder garbage and tickets never consume private headroom.
  Only the primary owner may change allocations; other principals see folder
  limits and generic owner-quota failures. Never expose owner activity through
  reservation or allocation probes. Saturate accounting and cap request sizes.
- Charge sealed metadata, fixed row costs, tombstones and queued garbage. Enforce
  owner row budgets, folder-scoped SQLite queries and transactional owner counters.
  Run GC in the background; keep in-flight publication charged until it finishes.
  Cancelling an unused ticket charges nothing. Persist cleanup before freeing
  physical blob charges; collect cancelled/expired uploads and staging files.
- Preserve busy paths, retry only actual CAS losers, commit renames atomically without credits,
  persist pending winner/retry state, and maintain a one-to-one local path map.
  Full-folder renames remain local and pending; do not delete the old remote path.
  Quarantine bad peers/unreadable files without blocking unrelated sync. Never
  re-download an unchanged quarantined row; retry transient transfers separately.
  Lstat before opening and use nonblocking/no-follow opens against replacement.
  Renew tickets only after more than half their TTL has elapsed.
- Revocation, downgrades, limit reductions and existing-row deletes never query
  quota. Only allocation growth queries pricing policy. Allocations persist until
  deletion and cleanup, including when the last grant is revoked.
- Sync parents of newly created directories before index/authority acknowledgement;
  retry the parent barrier when a root survives a failed creation.
  Match ignores under NFC/case folding and revisit skipped rows after unignore.
- Maintain native FSEvents on macOS through purego; kqueue's descriptor per file
  is unsuitable. Use inotify on Linux and keep periodic rescans as a safety net.

Required behavior checks for changes:

```sh
go test -race ./... -timeout 180s
CGO_ENABLED=0 go test ./... -timeout 120s
CGO_ENABLED=0 GOOS=linux go vet ./...
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux.test .
CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-linux ./cmd/drivesync
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x
```

Keep the owner/writer/reader/other-tenant/ungranted/revoked/anonymous matrix for
both transports. Exercise guessed IDs, stolen blobs/tickets, revocation/deletion,
crypto tampering, malicious paths, exact quota boundaries, racing reservations,
expiry, size lies, lowered quotas, batch rollback and crash/restart. Keep seeded
three-replica simulations with concurrent writes, create/modify/delete/rename/mkdir,
network failures, convergence and retained conflict contents. Real native watcher
integration must run on macOS. Record seeds and benchmark observations when
reporting failures or performance changes. Do not claim coverage from cross-builds
as if tests ran on that operating system.
