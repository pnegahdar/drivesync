# Adversarial repair record

Repairs relative to `e8d8ed2`, 2026-10-03. All 21 imported Opus/Astra behavior
regressions fail against that commit and pass with these changes. Proof-only
reviewer assertions were converted into safety assertions. Byte-boundary tests
now include metadata and row charges instead of assuming sealed content is the
entire storage cost.

| Finding | Repair | Regression evidence |
| --- | --- | --- |
| 1. Owner usage/quota leakage and overflow probing | Primary-owner-only quota numbers, configured limits for other principals, preallocated cross-tenant capacity, 2^50 upload cap and saturating arithmetic. Only the primary owner can alter allocation; delegated owners cannot probe private usage by resizing it. Legacy unallocated cross-tenant grants fail closed until repaired with `SetLimits`. | Both reviewer leak tests; `TestAllocatedSharedFolderHasNoPrivateUsageOracle`, `TestOnlyPrimaryOwnerSeesOwnerQuotaNumbers`, `TestDelegatedOwnerCannotProbeAllocationChanges`, `TestLegacyCrossTenantGrantFailsClosedUntilAllocated`, overflow regression. |
| 2. Uncharged metadata/rows and global decoding | Charge sealed metadata and 256 bytes per row, including tombstones/garbage; `Limits.MaxRows`, owner `Quota.MaxFiles`, principal/grant bounds; query folder/account scopes and indexed SQL pages. | Reviewer metadata/tombstone/grant spam regressions; `TestRowBudgetIncludesTombstonesAndOwnerFolders`, `TestGrantAndPrincipalBounds`, `TestUnrelatedTenantRowsAreNotDecoded`. |
| 3. Orphaned blobs | Persistent charged cleanup queue, collection outside the write lock, cancel/expiry/revoke/delete cleanup, in-flight upload state and ticket-bound staging filenames. Failed deletion retains both byte and row charges. | Reviewer physical-storage regression; `TestGarbageRemainsChargedUntilCollected`, `TestUploadedReservationExpiryCollectsBlob`, `TestExpiredInFlightUploadCollectsStagingFile`. |
| 4. Raw JSON identity aliases | Reject malformed UTF-8 and unpaired UTF-16 escapes before decoding. | Both raw HTTP cases in `TestRawJSONPrincipalAlias`. |
| 5. Ignore bypass and unignore | Case/NFC matching, exact parent spelling, unique aliases; persist ignored rows separately from the cursor. | Reviewer ignored `.git/hooks` and unignore regressions; `TestUnignoreAfterRestart`, `TestCanonicalIgnoreAndCollisions`; full NFC conformance corpus. |
| 6. Path loss and rename races | Separate busy from stale bases, replace own tickets, persist pending-winner intent before conflict renames, report conflicting paths, retry unrelated batch writes, bind rename credit to atomic create/delete, inspect exact directory names. | Reviewer network blip, concurrent edit, batch conflict, full-folder rename and case-only rename regressions; `TestBusyAndOwnReservationReplacement`. |
| 7. Wedged replicas | Durable quarantined retry rows and status, unreadable-local skips, minimum downloaded permissions, streaming renewal and early batch flushes, independent transfer failures, distinct live-ticket expiry errors. | Reviewer malformed row/file shape/mode/TTL regressions; `TestOneFailedUploadDoesNotBlockOtherTransfers`, `TestSlowSmallBatchDoesNotExpireEarlierTickets`, `TestUnreadableLocalDoesNotBlockOtherPaths`, interrupted-download restart test. |
| 8. Directory durability | Sync every created component's parent before publication/indexing; retry barriers for already-created components. Sync blob unlink before clearing its garbage charge. | `TestNewDirectoryFsyncFailurePreventsAcknowledgement`, staging-expiry test, existing interrupted upload/download tests. These inject failed barriers; they do not emulate a physical power failure. |
| 9. Alias collisions | Generate aliases until unused; reject duplicate live local mappings and omit tombstones from reverse mappings. | Astra `TestCaseAliasCollision`, existing case/Unicode collision tests. |
| 10. Smaller transport/operational defects | Indexed 512-row HTTP pages, bounded quota/size hooks, CLI key file/environment, empty-read-safe trailing content check, correct limit wording, documented authority rollback/deletion capability. | `TestHTTPChangesPagination`, `TestOpenContentAllowsEmptyReadsBeforeEOF`, CLI Linux build, existing wire/content fuzz targets; README documents the callback and authority limitations. |

Public additions: `Quota.MaxFiles`, `Limits.MaxRows`, `Usage.Rows`,
`Usage.ReservedRows`, `Usage.GarbageRows`, `UploadRequest.MetadataBytes` and
`UploadRequest.Deletes`, `ConflictError.Paths`, `ErrBusy`, `ErrExpired`, `ErrQuota`,
`Server.ChangesPage`, `Delta.Next`, `Server.CollectGarbage`, `Status.Quarantined`.
Use keyed struct literals and `errors.Is(err, ErrConflict)` for conflict errors.
Once an expired ticket has been collected, its unknown ID returns `ErrDenied`;
expiry details are returned only for a still-known ticket belonging to the caller.

The callback finding uses the requested bounded-time alternative: quota and size
hooks remain inside the atomic write transaction, capped at two seconds per
call/five seconds per transaction. A custom metadata backend must supply its own
atomicity and rollback; the built-in SQLite backend scopes owner write reads but
still materializes that account's current rows. No confirmed finding is deferred.

Cross-tenant capacity is reserved in advance, including its potential row budget.
Lowering an owner plan does not withdraw previously allocated shared capacity;
new private writes respect the lowered plan. As specified, allocation isolation
applies to cross-tenant grantees; same-tenant write outcomes still depend on the
aggregate owner quota (numeric totals remain primary-owner-only). Metadata and retained tombstones
now consume quota, so an old account may need a larger limit before further
writes. Renames consume a new path row while retaining the old tombstone, and
active replacements require temporary storage for another sealed copy. Run GC
periodically; failed collection keeps retired bytes charged. Custom blob adapters
must delete both published objects and their interrupted staging resources.

NFC tables and their complete 20,034-case canonical conformance corpus are frozen
from [Unicode 17 UCD](https://www.unicode.org/Public/17.0.0/ucd/), with its license
in `UNICODE_LICENSE.txt`. Regenerate with `python3 internal/generate_unicode.py`
and `gofmt -w unicode_tables.go`; ordinary Go builds require neither Python nor
network access. Direct Go dependencies remain unchanged.

Verification on macOS arm64, Go 1.27.1:

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

All pass. Three-replica simulation seeds remain `42`, `9817`, `20261003`, with
concurrent writes, network failures, convergence and conflict-content retention.
Native FSEvents propagation measured 363 ms. A three-iteration benchmark measured
333 ms small-file propagation, 361 ms initial 10,000-file scan, and 333 ms indexed
rescan (Apple M6; approximate, dependent on load). Earlier `e8d8ed2` measurements
were 274/280/275 ms; the added isolation/accounting work and broader matching add
cost. Incremental local scanning remains deferred. Linux was vetted and compiled,
not runtime-tested on this Mac.

To reproduce the imported before/after evidence without changing the main tree:

```sh
git worktree add --detach /tmp/drivesync-review-baseline e8d8ed2
cp -R reviewtests /tmp/drivesync-review-baseline/
(cd /tmp/drivesync-review-baseline && go test ./reviewtests/... -count=1)
# 21 failing safety tests on e8d8ed2
go test ./reviewtests/... -count=1
# pass with repairs
```
