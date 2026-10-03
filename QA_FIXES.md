# Last-QA repairs

Reviewed base: **5365ec9**. Every Opus/Astra reproduction is retained in
`reviewtests/lastqaopus`, `lastqapublic` and `lastqaastra`: 25 defect tests plus
the passing public authorization matrix. Original and current adapted proofs
fail by assertion on that base; the process-wide RLIMIT proof is run alone.
Provenance and adaptation details are in [reviewtests/README.md](reviewtests/README.md).
No backward compatibility or migrations were added. Fresh metadata/state is
required. No remote or push.

Net change: **+3,512 lines** (3,930 added, 418 removed), including proofs/logs.
Production Go source: **+857 lines**.

## Tier 1

| Item | Repair and evidence |
| --- | --- |
| 1. Empty root mass-deletes | A random reserved root marker and saved device/inode bind state to the actual root. Missing/changed identity pauses sync, even after Retry. At least five tracked entries with 80% unexplained disappearance pause deletes; matching moves are excluded. Public empty-mountpoint proof plus copied-marker/inode and deliberate-removal controls pass. |
| 2. Remote delete destroys ignored data | Remove only known junk (.DS_Store, Thumbs.db). Preserve user-ignored data, including .git; accept the directory tombstone and leave the directory untracked once tracked children are gone. Both ignored-data proofs and tracked-child retry tests pass. |
| 3. Default state exposes nested paths | Default state is a private per-user cache directory by folder/root hash. Reserve/ignore .drivesync* entries; markers refuse attachments inside or over another attachment. The nested-folder proof now accepts safe attach refusal. |
| 4. Whole-folder writer stalls | Compaction selects expired tombstones on WAL readers and writes 128-path batches. DeleteFolder marks deleted/revokes grants without loading file rows; background retirement preserves complete charges. TransferUsage caches allow mutations/GC to load only touched tickets/garbage. Composite folder/ID indexes avoid backlog scans. Original 60,000-row and 20,000-garbage proofs pass without reducing counts. |
| 5. Shared state deletes unrelated files | A dedicated matching state marker is required before chmod/cleanup. Reject unrelated nonempty state, bindings to another replica, and symlink aliases into attachments before mutation. Shared-state and alias controls pass. |
| 6. Skipped symlink tombstones children | Unsupported/symlink entries block their whole subtree from rename/delete detection, including folded/aliased names. Both-transport symlink proof passes. |
| 7. Denied writes wait on writer | Indexed read-pool authorization precedes the writer queue; transaction authorization still rechecks access. The held-writer denied-request proof passes through both transports. |
| 8. Oversize bytes uncharged | A reservation-sized reader probes the next byte without forwarding it. Reader errors prevent publication in both built-in blob stores; their staging is discarded. Oversize proof covers memory/directory stores through both transports and accepts correct rejection without a blob. Custom BlobStore.Put must publish only after clean EOF. |

## Tier 2

| Item | Repair and evidence |
| --- | --- |
| Compaction between incremental pages | Re-read horizon on each page. An advanced horizon returns a private restart signal; HTTP and in-process clients discard partial results and pull Full. Original HTTP proof and deterministic in-process control pass. |
| One file cannot stage | Removed plaintext upload staging. Stream encryption from an Lstat/open/identity-verified handle and verify the scanned hash before commit. Failures reject/back off that path while siblings continue. Isolated RLIMIT proof passes. |
| Truncated stored blob / retry pinning | Count received sealed bytes. Incomplete framing after receiving the advertised size is integrity failure; short transport delivery retries with backoff. Local edits may replace quarantined versions; valid pending winners retain CAS/conflict protection. Stored-truncation and existing interrupted/post-final transport proofs pass. |
| Local obstacles | FIFO/symlink/parent-file obstacles stay retryable rather than quarantining authenticated rows. No-follow/nonblocking opens remain in use. Public obstacle proof passes. |
| Public retry / quarantine names | Replica.Retry clears rejection/quarantine backoff and wakes sync. Status uses decrypted paths when authenticated metadata is available, otherwise path IDs. Both-transport public retry proof passes. |
| Tombstone recreation scaling | Cache path-ID/tombstone versions from pulls and persist only received tombstones in bounded SQL batches. No full listing per recreated file or cache rewrite on idle pulls. Original 1,000-recreation proof and 61,440-row busy-folder proof pass. |
| Wait cap | Distinct ErrWaitLimit, five-second admission backoff, and 256 waits per principal. The fixed 64-folder station retains push updates; admission exhaustion has its own wire error. |
| Attach context | Setup honors caller cancellation; replica workers use a lifetime owned by Close. Context-expiry proof passes. |
| Persistent errors | Successful Sync clears Status.Errors; actionable skips/quarantine remain independently visible. Recovery proof passes. |
| Empty blob directories | Successful deletion removes empty per-folder directories with parent fsync; concurrent publication retries directory creation/open. Blob-directory and durability tests pass. |
| Permission-only changes | Apply authenticated mode with minimum 0600/0700, fsync, then persist index state, including content-hash adoption. Both-transport permission proof passes. |
| Failed publication acknowledgement | A same-process live-publication registry distinguishes active Put from completed failed acknowledgements. Durable charge survives faults; bounded maintenance retires finished publications and collects them, including without cancellation or expiry. Original cancellation proof and no-cancel fault control pass. Recovery reuses the ordinary collector. |

## API and security contracts

Zero keys are rejected at creation/attachment. ParseFolderKey checks 64-character
hex imports and rejects zero keys. NewFolderKey is the supported random-key source;
passwords and password hashes are unsuitable. Checks use random salts bound to an
authority-issued folder ID. A stateless, expiring, principal-bound creation
challenge permits local computation without client-chosen IDs, persisted drafts,
or sending the key to the authority. Stolen/altered/expired/replayed challenges
are tested through both transports.

RPC requires application/json Content-Type, closing form/simple-request CSRF.
Malformed-JSON tests explicitly set this header to retain decoder coverage.
Server.Client trusts the embedding authenticator's principal; HTTP clients should
use HTTPS. These contracts and dedicated state/root behavior are in README.

Salt fixes deterministic folder linkage; **offline candidate verification remains
inherent** to key checks and authenticated ciphertext. The reproduction retains
correct-key acceptance and tests unlinkability, rather than claiming a salt
prevents password guessing. No requested repair is intentionally deferred.

An actual ACL change invalidates all outstanding tickets in that folder via an
authorization epoch, allowing immediate bounded revocation. Other writers retry;
idempotent grants/revokes do not invalidate tickets. Expired/invalidated tickets
retain their charge until maintenance retires them. Allocations remain charged
through bounded folder deletion and physical cleanup. Only one authority process
may publish to the store. Full-tree hashing and malicious-server rollback/deletion
remain documented prototype limits.

## Validation

All checks below pass on macOS arm64, Go 1.27.1 / Apple M6. Linux was vetted and
cross-built, not executed. All scale fixture counts and latency assertions remain
intact; no large blob fixtures were added. Simulation seeds remain 42, 9817 and
20261003, with short configured retry intervals to exercise transient failures
without minute-long test sleeps.

```sh
go test -race ./... -count=1 -timeout 180s
CGO_ENABLED=0 go test ./... -count=1 -timeout 120s
CGO_ENABLED=0 GOOS=linux go vet ./...
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux.test .
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux-engine.test ./internal/engine
CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-linux ./cmd/drivesync
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
QA_RLIMIT=1 go test ./reviewtests/lastqapublic -run '^TestOneUnstageableFile' -count=1 -v
CGO_ENABLED=0 go test -run '^TestWatcherPropagation$' -count=1 -v ./internal/engine
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x .
```

Race: engine 69.589 s, confirmation Opus 173.191 s, last-QA Opus 116.944 s;
pure-Go: engine 40.027 s, confirmation Opus 39.888 s, last-QA Opus 38.117 s.
Fuzz targets execute 4,858 / 1,319 / 73,794 cases respectively. Native FSEvents
propagation is 372 ms. Three-iteration benchmarks: small-file propagation
275 ms/op, initial 10k scan 355 ms/op, indexed scan 359 ms/op.

Initial race runs exposed two genuine scaling defects in the repair: selected
garbage queries needed composite folder/ID indexes, and idle pulls rewrote the
complete tombstone cache. The 20,000-row GC proof improved from 1,408 to 14,464
objects per five-second race pass, with worst victim write 407 ms -> 46 ms;
the 61,440-row busy-folder proof improved from 99 s to 6.9 s for five syncs.
Final full race/pure-Go runs pass without weakening either proof or raising the
required timeouts. The isolated RLIMIT run now propagates the small sibling and
delete; its last sync returns nil because uploads no longer stage plaintext.
