# Final-review repairs

Reviewed base: `c584e8d`. All original proofs were imported and run before
production edits. Adapted/strengthened versions also fail on a detached copy of
that base; see [reviewtests/README.md](reviewtests/README.md) and the three final
before logs. All ten findings and both requested design changes are addressed.
There are no legacy-schema migrations or compatibility paths.
Net change: **+1,996 lines** (2,128 added, 132 removed), including proofs/logs.

| Item | Change and evidence |
| --- | --- |
| 1. Undercharged allocations / reduced caps | Allocation and limit changes check folder bytes plus garbage/reservations and the complete row budget before pricing. Caps below usage are refused. Both transport storage attacks and separate ticket/garbage/row-floor regressions pass. Reductions still never call pricing. |
| 2 and 7. Unrelated-tenant scans / denied timing | Real indexed `owner`, ticket/garbage `folder` columns and a `grants(folder, principal)` table replace JSON grant searches. Listing starts from the principal index. Streaming checks and folder-version reads authorize through one indexed query; a missing grant never decodes the folder's grant set. Scale/timing proofs and EXPLAIN plans pass. |
| 3. Truncated downloads | Early EOF returns `io.ErrUnexpectedEOF`, a transient retry; authenticated/framing corruption still returns `ErrIntegrity`. HTTP aborts the connection when copying a stream fails. The one-abort proof subsequently downloads the unchanged row. |
| 4. Pending rename holds other uploads | Ordinary files go first, each rename pair commits separately, and independent batches failing limits retry one mutation at a time. CAS pairs remain indivisible. The row-ceiling wedge and its controls pass. |
| 5. Rename onto tombstoned name | Rename discovery skips only nondeleted known targets; failed target reservations hold the source deletion. Both full-folder rename proofs pass. |
| 6. Pricing outage stops sync | GetFolder/ListFolders return stored limits with no pricing call. The outage proof propagates an existing-row delete and continues listing. |
| 8. Idle long-poll amplification | Per-folder idle wake channels and 32 waits per principal replace global broadcasts and the 250 ms query loop. Hubs are shared by same-process SQLite authorities, including canonicalized database paths. Wait admission, idle query counts, cross-authority changes and revocation/deletion pass. |
| 9. Plan per-file cap exposure | Sharing requires an explicit positive MaxFileBytes within the plan. No pricing value is copied into visible folder limits. The missing-cap proof is refused at allocation. |
| 10. Case-only aliases | Pull applies replaced deletes before creates, children before deleted parents, then directories before live children. The strengthened APFS proof requires the exact renamed path without aliases. |
| Tombstone retention | Default 30-day TombstoneTTL, disabled with zero. Background collection compacts expired rows, updates the horizon and frees usage. Old cursors reconcile a stable full current set before writes; local dirty files survive as conflicts. Both transport offline/recreate proofs pass. |
| Existing-directory attachment | Matching authenticated hashes/types/sizes are adopted and fsynced before saving the index. Both transports attach identical files without changing the authority version or creating copies, then propagate edits. |

The before logs show the original assertions, followed by strengthened case,
wait-admission and denial assertions and the two new design tests. Setup for
large fixtures now uses one public admission and one bulk storage transaction,
with the same 20,000 folders, 3,000 tickets and 30,720/20,480 tombstones. Measured
requests and assertions are unchanged. The optimized folder/ticket proofs still
fail on c584e8d. Opus2's full race package is **50.677 s**, versus the preceding
round's **137.334 s**, without reducing cardinalities or quota/CAS coverage.
An intermediate race run timed out in the original per-folder setup; another
hit the five-second request deadline while checking all fixture folders. Those
setup operations were replaced; the final complete suite passes.

Executed on macOS arm64, Go 1.27.1:

```sh
go test -race ./... -count=1 -timeout 180s
CGO_ENABLED=0 go test ./... -count=1 -timeout 120s
CGO_ENABLED=0 GOOS=linux go vet ./...
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux.test .
CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-linux ./cmd/drivesync
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
go test -run '^TestWatcherPropagation$' -count=1 -v .
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x
```

All pass. Main race package: 57.183 s; final-review race package: 30.828 s.
Pure-Go main package: 20.660 s. Fuzz executions: path 21,894; wire 2,411;
content 56,718. Seeded simulations (42, 9817, 20261003) pass. Native FSEvents
propagation: 358 ms. Three-iteration benchmarks: propagation 274 ms/op, initial
10k scan 359 ms/op, indexed scan 357 ms/op. These are short local observations.
Linux was vetted/cross-built; its runtime tests were not executed here.

No requested item was deferred. Fresh databases are required. Multi-process
embedders must distribute durable folder/grant events through NotificationSource
and Notifications.Notify; SQLite hubs alone cover same-process authorities.
Existing constraints remain: bounded pricing/blob-size hooks hold the writer
transaction; full local scans and folder-local write scans remain; no historical
recovery or malicious-server freshness guarantee; exclusive restart coordination
is required for RecoverUploads. An existing-row delete can briefly add
bookkeeping charges until background GC/compaction, but cap changes can never
hide those charges. No remote or push.

## Earlier round-2 repairs

Reviewed base: `2b29848`. Original proofs were run before production edits and
imported under `reviewtests/`; adapted proofs were also run against a detached
checkout of that base. See [reviewtests/README.md](reviewtests/README.md) and its
before logs. Original failures: eight Opus assertions, six Astra assertions and
the private fsync retry overlay. Controls and performance observations that were
already passing remain in the suite; they are not claimed as failing regressions.

| Finding / decision | Change and regression evidence |
| --- | --- |
| 1. Reused rename credits / row-ceiling overrun | Deleted `UploadRequest.Deletes` and `Ticket.Deletes`, replacement-growth credits and server credit validation. Each upload reserves a complete copy and row. Replica renames use one atomic create/delete batch, including paired rollback on CAS loss. Full-folder renames remain pending locally; no remote deletion. Opus/Astra credit proofs, reserved-row attack and pending-rename test. |
| 2. Same-tenant usage probing | Every grant requires byte and row caps and allocates capacity, regardless of tenant. Non-primary writers check only their folder. Same-tenant binary search is unchanged across a private delete even after a plan decrease. |
| 3. Shared garbage consuming private headroom | Owner counters charge exactly the allocated byte cap and its row capacity, never `max(cap, usage)`. Staged uploads and retired garbage are included before accepting writes. Garbage and row attacks reject inside the shared folder; private reservations continue. |
| 4. Revocation / reductions / delete wedges | Allocations remain after the last grant is revoked, until folder deletion and cleanup. Grant downgrades, revokes, limit reductions and existing-row tombstones bypass pricing policy. Primary-owner-only growth remains enforced. Replica tombstone errors are reported after unrelated uploads/downloads proceed. Both-transport plan-decrease proofs, delegated-owner proof, policy-unavailable test and failed-tombstone test. |
| 5. Request-path GC / publication race | Removed all automatic request-path collection. `RunGC` is an embedder-owned background worker. A durable `Writing` cleanup record survives cancellation/expiry until `Put` returns; failed cleanup remains queued/charged. Unused tickets charge zero. `RecoverUploads` is explicit restart maintenance after old writers stop. Paused-publication proof, slow-delete assertion, background GC and restart/staging tests. |
| 6. Account-wide loads / timing | Folder-scoped SQL plus durable owner counter rows updated in the same transaction. Cached file usage serves listing/getters/maintenance; other private file rows are not loaded. Alternate stores use `ScopeFromContext`, `Metadata.Prepare` and `Finish`. The 20,480-row timing proof and a malformed same-owner private row check pass. |
| 7. Smaller defects | Renew only after half TTL: 3,000 one-byte reads make two start/finish transactions, versus 6,005 on the base. Persist separate quarantine, ignored and transient retry kinds; unchanged quarantined rows are not downloaded again. Honor unreadable-directory guards in rename detection. Lstat before nonblocking/no-follow opens rejects FIFOs and replacement races. Retry existing-root parent fsync barriers. Native FIFO, rename guard, restart/unignore, renewal and durability regressions pass. |
| 8. Unicode maintenance | Removed the 4,020-line generated table and generator. Use `golang.org/x/text` v0.42.0 [NFC](https://pkg.go.dev/golang.org/x/text/unicode/norm) and [case folding](https://pkg.go.dev/golang.org/x/text/cases); retained the Unicode 17 conformance corpus/tests and ignore/collision proofs. |

Net change: **-1,880 lines** (2,682 added; 4,562 removed), including tests/docs.
No confirmed finding remains. Legacy support is removed: use fresh databases. The original
legacy reproduction remains a fixture, while its independent download wedge is
a runnable regression. No remote was created and nothing was pushed.

## Verification

Executed on macOS arm64, Go 1.27.1:

```sh
go test -race ./... -count=1 -timeout=180s
go test -race . -count=1 -timeout=180s
go test -race ./reviewtests/opus2 -run '^TestDeleteFailureDoesNotBlockDownloads$' -count=1
CGO_ENABLED=0 go test ./... -count=1 -timeout=120s
CGO_ENABLED=0 GOOS=linux go vet ./...
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-round2-linux.test .
CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-round2-linux ./cmd/drivesync
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
go test -run '^TestWatcherPropagation$' -count=1 -v .
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x
```

All passed. Full race run: main package 47.489 s, largest reviewer package
137.334 s. The final core race rerun passed in 47.538 s after threading the
transaction deadline through external hooks. The final failed-tombstone
adaptation also passed under race. Simulation seeds `42`, `9817`, `20261003`
converged and retained write contents. Native FSEvents propagation was 407 ms.
Linux was cross-vetted/built only; Linux runtime tests did not run.

Three-second fuzz runs: path 23,450 executions; wire 3,852; content 62,888; no
failures. Final three-iteration benchmarks: small-file propagation 349 ms/op,
initial 10k scan 352 ms/op, indexed 10k scan 353 ms/op. These are short local
observations, not production capacity claims.

The private-row timing probe measured 526 microseconds before and 257 after
20,480 unrelated private rows; the base measured about 585 microseconds before
and 85 milliseconds after. Honest 64 MiB streams measured 455–1,418 MB/s across
16–64 KiB reads. One-byte callers still cause contention through fresh SQL
per-read authorization; the eliminated cost is durable renewal writes.

## Design limits

- Pricing and stored-size hooks remain inside the SQLite writer transaction,
  bounded to two seconds each and the transaction's five-second deadline.
  Folder write operations still materialize that folder's current rows; they do
  not materialize the account's other rows. Replicas still hash the whole tree.
- Allocation persists until deletion/cleanup. Allocation and cap changes now
  reject capacity below folder usage (including tickets/garbage); see the final
  review repairs below. Existing-row deletes may temporarily add garbage/tombstone
  bookkeeping until background collection and compaction.
- `RecoverUploads` requires exclusive restart coordination: all prior writers
  using the stores must have stopped. Ordinary GC never assumes a writer stopped
  merely because its ticket expired. Directory fsync checks inject failures and
  verify ordering/retry, rather than simulate a physical power loss.
- Existing documented protocol limits remain: no historical recovery, key
  rotation, dedup/chunk reuse or presigned URL issuance. A malicious server can
  delete files or replay old valid encrypted data; encryption does not establish
  freshness. Writes through an already-open replaced inode are not transactional
  with replica publication.
