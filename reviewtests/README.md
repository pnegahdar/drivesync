# Adversarial regressions

`opus/` and `astra/` retain the round-1 proofs. `opus2/` imports every Opus
round-2 test; `astra2/` imports Astra's second tests and their original helpers.
All runnable tests use the current prototype API:

- Shared fixtures declare byte and row caps. The same-tenant probe lowers the
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
