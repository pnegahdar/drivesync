# Security

drivesync encrypts file paths, contents, and file metadata before they reach
the authority. Transport authentication and folder-key access are separate
boundaries. See [AGENTS.md](AGENTS.md) for the rules that changes must keep.

## Threat model

### Authority

The authority stores folder records and immutable ciphertext. It can see folder
ids, names, descriptions, owners, grants, roles, limits, and usage, plus
ciphertext sizes, row counts, versions, blob ids, and opaque path ids.

It cannot see file paths, file contents, or file metadata. Those use separate
keys derived from the folder key. Callers compute the key check locally, with a
random salt bound to the authority-issued folder id. The folder key is never
sent to the authority. A salt stops deterministic linkage between folders. It
does not stop offline guessing, so use a random key from `NewFolderKey`.
Passphrases and hashes of passphrases are not suitable keys.

Use one authority process per metadata and blob store. `Server.Client` trusts
the principal it is given; call it only after the embedding application has
authenticated that principal. HTTP clients should use HTTPS. `/rpc` requires
`Content-Type: application/json`.

### Peer with the folder key

A peer who holds the folder key can decrypt every name and byte in that folder.
A writer can publish changes up to the folder limits and their role. Those
changes sync to other replicas, including conflict copies when both sides edit
a path. Receiving replicas treat decrypted paths as untrusted: they reject
traversal, absolute paths, NUL, and reserved names, refuse symlinked path
components, and confine writes with `os.Root`.

A key holder cannot read a folder whose key they do not have, and cannot
impersonate another principal. The authority authorizes the authenticated
tenant and subject on every operation. Unknown folders and denied access return
the same error. Revocation and deletion invalidate open waits and outstanding
upload tickets. Bytes already delivered stay readable, because revocation does
not rotate the key.

### Malicious authority

A malicious authority can delete files or roll them back to older ciphertext
that still authenticates. Encryption does not prove freshness. Historical
snapshots, key rotation, and detecting a rollback are outside this version.
The authority still cannot invent ciphertext that a holder of the key will
accept, and it cannot read the files.

## Isolation and quotas

Principals are nonempty UTF-8 tenant and subject pairs, each at most 256 bytes.
Authorization always uses both components. Grants are capped at 256 per folder.
The creator remains the immutable primary owner and the quota account. Only an
owner can manage grants, delete the folder, or change limits.

Every shared folder, including a grant inside the same tenant, needs an
explicit byte cap, row cap, and per-file cap within the owner's plan. That
capacity is charged to the primary owner at the cap and stays charged until
the folder is deleted and cleaned up. Grantee activity cannot read private
usage or consume private headroom. Non-primary principals receive a generic
owner-quota error. Reductions, revocations, and deletes of existing rows do
not consult pricing.

Byte budgets charge sealed bytes, metadata, and 256 bytes per retained row.
`MaxFileBytes` is the sealed size, so allow for encryption overhead. `MaxFiles`
counts live entries, including directory markers. `MaxRows` bounds retained
rows and garbage. A zero `MaxRows` selects 16 times `MaxFiles`, capped at one
million. Other zero limits mean unlimited, except that sharing requires
positive caps. Reservations are atomic. Commit checks the stored size, so an
uploaded size claim is not trusted. Deletes add no new charge. Tombstones keep
their blob cleanup until physical deletion succeeds.

Upload tickets bind the principal, folder, path, and base version. One
principal and session may replace its own stale ticket. Another session gets
`ErrBusy`.

## Local disk

Sync pauses deletes when at least five tracked entries exist and 80% of the
tracked files or bytes disappear without a matching move. `Retry` acknowledges
an intentional removal, and that acknowledgement lasts until the guard passes.
Inspect the disk before acknowledging.

Each root has a random `.drivesync-root` marker. Saved state binds that marker
to the folder, token, and inode. Device numbers can change across remounts and
are not part of the identity. A missing or replaced root pauses sync, so an
unmounted disk does not delete peer files. `Retry` on a changed root adopts it
with an empty index: local writes stay local, differing authenticated remote
contents are kept as conflict copies, and no deletes are derived from the
discarded index. A symlinked root cannot be attached or rebound. A lifetime
flock on the marker prevents two replicas from using one root.

Ignored files are not deleted. A remote directory delete preserves user-ignored
contents, accepts the directory tombstone, and leaves the directory untracked
once its tracked children are gone. The only names removed automatically are
`.DS_Store` and `Thumbs.db`. A remote tombstone for an ignored file updates the
index and does not touch local bytes or create a conflict copy. Unsupported
entries and symlinks block delete and rename detection for their whole subtree.

`.git/` is ignored by default, including case-folded paths such as `.GIT`.
Repositories belong in git. Copying `.git` would turn concurrent commits into
conflict files. `.drivesync*` names are reserved. Default ignore rules also
skip editor and Finder temporary files.

Replica state stores plaintext paths. The default directory is under the user
cache, private to that folder and root. An explicit state directory must be
dedicated to that attachment and outside every attachment. Nested and
overlapping attachments are refused.

## Reporting a vulnerability

Report vulnerabilities through a GitHub private security advisory:

https://github.com/pnegahdar/drivesync/security/advisories/new

Include what you found, the version or commit, and a way to reproduce it.
A private advisory keeps the report unpublished until a fix is ready.
