# Round-2 security repairs

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
- Allocation persists until deletion/cleanup. Lowering allocated limits below
  stored usage keeps existing content and blocks that folder's new writes. Exact
  cap accounting can then reserve less than physical grandfathered storage;
  this follows the requested exact-cap/reduction model. Existing-row deletes can
  also temporarily increase tombstone/garbage charges. Future hard physical
  billing needs an explicit policy for grandfathered capacity.
- `RecoverUploads` requires exclusive restart coordination: all prior writers
  using the stores must have stopped. Ordinary GC never assumes a writer stopped
  merely because its ticket expired. Directory fsync checks inject failures and
  verify ordering/retry, rather than simulate a physical power loss.
- Existing documented protocol limits remain: no historical recovery, key
  rotation, dedup/chunk reuse or presigned URL issuance. A malicious server can
  delete files or replay old valid encrypted data; encryption does not establish
  freshness. Writes through an already-open replaced inode are not transactional
  with replica publication.
