# Adversarial regressions

`opus/` and `astra/` retain the round-1 proofs. `opus2/` imports every Opus
round-2 test; `astra2/` imports Astra's second tests and their original helpers.
All runnable tests use the current prototype API:

- Shared fixtures declare explicit byte, row and file caps. The same-tenant probe lowers the
  plan after allocating, then observes a legitimate private delete; this makes
  the old owner-headroom oracle reachable without allocating huge test blobs.
- Removed `Deletes` fields are supplied through JSON in the credit attack. The
  old request decodes and credits them; the current request has no credit field.
- Storage attacks stop when correctly rejected and still assert physical/folder
  budgets and unaffected private headroom. Cleanup is explicit/background.
- The legacy injection is archived in `fixtures/opus2_legacy_test.go.txt`.
  `TestDeleteFailureDoesNotBlockDownloads` retains its independently relevant
  tombstone/download failure without adding legacy compatibility.
- The original private fsync overlay is archived in `fixtures/root_fsync_test.go.txt`.
  Its executable regression is `../root_fsync_review_test.go`, where the private
  fsync hook is available. It also verifies the first failed attempt left a root
  which survives into the retry.

`round2-before.txt` records the original failures on `2b29848`, before production
edits: eight Opus failures, six Astra failures, plus the fsync overlay. The
performance and malformed-input controls that already passed are retained too.
`round2-adapted-before.txt` records the current API adaptations running against a
detached `2b29848` checkout. Its appended runs verify the final same-tenant and
failed-tombstone adaptations fail on that commit as well. `../round2_test.go`
adds deterministic structural checks for renewal transaction counts, unrelated
private row decoding, policy-free revocation/deletion, quarantine/restart,
background GC, unused-ticket charges and full-folder pending renames.

`final/` imports every c584e8d final-review proof. `final-before.txt` records the
original run before edits; `final-adapted-before.txt` records the runnable tests
against a detached c584e8d checkout, including assertions strengthened for case
aliases, denied timing and wait admission. All ten findings fail there (2 and 7
share indexed-access fixes). The two added design tests fail there too. Cap
attacks accept a correct folder-local LimitError; the missing-file-cap test
accepts refusal to allocate. Neither adaptation masks the original vulnerability.

The folder-list fixture retains all 20,000 folders after one public creation.
The ticket timing fixture admits one ticket through Reserve and seeds the other
2,999 equivalent legal tickets in one transaction; the old scan still fails.
Opus2's two scale fixtures likewise admit one 256-row CAS batch publicly, seed
remaining identical tombstones once, and assert exact 30,720 / 20,480 row counts.
Measured operations still use the public API. This removes quadratic setup costs
under the race detector without reducing scale, timing assertions or separate
quota/CAS coverage. Original slow before-runs are retained.
