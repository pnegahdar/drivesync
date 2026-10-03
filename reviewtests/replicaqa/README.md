# Replica/local-disk QA repairs

Baseline: `c32b886`. The original reviewer reproductions were run from a clean
export using `/tmp/drivesync-replica-qa/run.sh c32b886 '^TestQA'`.
`before-c32b886.txt` records all 27 failures and the passing controls.
Every proof is executable in `internal/engine/replicaqa_*_test.go`, where it can
inspect private replica state without expanding the public API. Run:

```sh
go test ./internal/engine -run '^Test(QA|ReplicaQA)' -count=1 -v
```

The eight original Tier 1 findings are addressed as follows:

| Finding | Repair |
|---|---|
| Unlistable root sends deletes | Root walk errors abort; non-ENOENT listing errors mean potentially present. |
| Removal during pull bypasses guard | Pull first, scan once through `os.Root.FS`, and use that scan for upload, rename and delete safety. |
| Nested volume disappears | Cross-device directories are blocked and reported in attachment admission, scan and peer path checks. |
| Removed marker wedges forever | Retry drops entries and pending state, binds the restored root, and adopts afresh. Local changed files stay canonical; authenticated differing remote bytes survive in conflict copies. |
| Symlinked root silently stalls | Lstat must identify a real directory matching the open root handle; Retry cannot rebind a symlink. |
| Remount changes device number | Identity is folder/token/inode. Device numbers bound traversal only. |
| Enclosing attachment leaks state | Admission recognizes state markers as well as root markers. Dedicated temporary test state and sandboxed default-cache tests avoid plaintext cache leftovers. |
| Moving an attachment leaks files | Foreign marker directories block their entire subtree in scans and peer application. |

All sixteen original Tier 2 findings are addressed:

| # | Repair |
|---|---|
| 1 | Peer modes carry only owner-execute; local group/other bits remain local. |
| 2 | Ambiguous local aliases upload neither file and report both rejections; an exact canonical name can adopt a missing alias's matching indexed contents. |
| 3 | Updates into a missing tracked path defer for one sync, preserving both versions across an editor's rename-away save gap. |
| 4 | Tombstones keep sealed metadata and its charge until compaction. Fresh state recognizes matching deleted contents. |
| 5 | Exclusive root-marker flock lasts until Close; shared ancestor/exclusive directory admission locks serialize concurrent overlapping Attach calls. |
| 6 | Index stores observed mode after chmod/fsync, including synthetic exFAT modes. |
| 7 | Hashing and encryption stream exactly the statted size; later appended bytes wait for the next scan. |
| 8 | Ignore matching supports anchored `/`, `**`, directory globs and comments. Unsupported negation/escape/syntax rules appear in status. |
| 9 | Retry acknowledgement survives failed syncs and delete batches until a successful guarded sync. |
| 10 | Attach skips unreadable subdirectories while refusing root walk errors. |
| 11 | Root-relative filesystem walking rescans paths longer than macOS's absolute-path limit. |
| 12 | `.git/**/*.lock` is ignored at every depth. |
| 13 | Unsupported portable local names are kept and rejected with a reason. |
| 14 | Ignore fingerprints persist; only rule changes revisit ignored rows. Pending SQL updates touch affected path IDs instead of rewriting the entire table. |
| 15 | Local deletes use atomic commits of up to 256 paths; CAS failures retry only unaffected paths. |
| 16 | Peers move verified matching plaintext for atomic rename pairs; no content download is required. Ciphertext is path-bound, so uploads remain necessary. |

Remote tombstones for ignored paths update the index without touching local
contents or making conflict copies. Only known junk may be auto-removed from
otherwise deleted directories.

Proof adaptations follow the explicit requested decisions, rather than relaxing
assertions: the mount proof now asserts exclusion/reporting and preservation of
unrelated files; portable-name proofs require explicit rejection; rename transfer
proofs constrain download bytes (uploads remain cryptographically necessary).
Root-change controls still assert no derived remote deletes, and now permit
explicit fresh adoption via Retry. The ignored-row timing fixture seeds exactly
3,101 authenticated rows in one transaction, verifies their count, and keeps the
original 50 ms idle-sync bound. The old quota boundaries now include retained
sealed tombstone metadata. Added tests cover anchored ignore semantics,
unsupported rules, and a failed first delete batch after acknowledgement.

CI runs both Ubuntu and macOS, including native inotify/FSEvents propagation.
Timing probes retain their original record counts and internal goroutine races;
packages run serially (`GOFLAGS=-p=1`) to avoid unrelated stress packages competing
with the latency measurements. SQLite writes use bounded parameterized batches
inside the same transactions, reducing stress-fixture and multi-path write costs.

Verified test-output cache directories removed: 141. No application state was
removed; every deleted directory's marker pointed into a Go test temporary path.

Final validation (macOS arm64, Go 1.27.1):

- `GOFLAGS=-p=1 go test -race ./... -count=1 -timeout=180s`: passed every package;
  engine 173.571 s, confirmation suite 152.996 s, opus2 26.627 s.
- `CGO_ENABLED=0 GOFLAGS=-p=1 go test ./... -count=1 -timeout=120s`: passed every
  package; engine 62.748 s. Packages serialize; each test's races remain intact.
- `CGO_ENABLED=0 GOOS=linux go vet ./...`: passed.
- `CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux.test .` and the
  equivalent build for `./internal/engine`: passed.
- `CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-linux ./cmd/drivesync`: passed.
- All three documented fuzz targets: 3 s each, two workers, passed.
- Documented benchmarks, 3 iterations: small-file propagation 321 ms; scan of
  10,000 files 594 ms; indexed scan 591 ms. Native FSEvents integration passed
  (437 ms in the explicit race run); Linux native runtime awaits CI.
- Follow-up root recovery/acknowledgement race checks passed after clearing the
  transient missing-path deferral cache on rebind. GC drained 14,080/20,000 queued
  blobs within its five-second race-test budget, with victim writes below 45 ms.

Default-package parallelism originally timed out the stress suites; CI and the
successful runs use serial packages. No test cardinalities or concurrency
assertions were reduced. Three interrupted QA temporary directories and the
cross-build binaries were removed; the reviewer source/report directories remain.
