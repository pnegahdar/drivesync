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

Use one authority process per SQLite database file. Several processes may share
a Postgres metadata store. Upload leases keep recovery from retiring a
publication that is still writing. Those leases assume the authorities' clocks
are reasonably synced. Garbage collection claims a lease row instead of holding
a session lock. Anyone who can connect to the database can LISTEN on the
NOTIFY channel. Its payloads show folder ids and when those folders change.
`Server.Client` trusts
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

An authorized principal may receive a short-lived signed URL for one sealed
blob when the blob store supports it. These URLs expire within five minutes.
Revocation and deletion cannot recall URLs already issued; the bearer can read
that ciphertext until expiry. The bytes remain encrypted and authenticated, and
replicas send no authority credentials to the blob URL. Signed URLs must never
appear in status, errors, or logs.

The same authorization and five-minute lifetime apply to packed file-list
snapshots. They carry opaque paths and sealed metadata, never plaintext names
or contents. The fixed blob key `snapshot` is derived data outside folder quota
and is removed during folder cleanup. Ordinary download/upload operations do
not grant access to that key. Replicas validate its folder, version and bounded
framing, authenticate each row, and catch up through the regular change feed.

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

On-demand replicas represent unfetched files with empty regular files whose
permission bits are `0000`. Placeholders contain no content, and their real
sizes, hashes, modes and blob references from authenticated metadata live in
private replica state. Ordinary
unprivileged reads fail until `Fetch` authenticates and publishes the content.
Privileged processes can bypass permissions and must consult replica state.
Scans identify placeholders by device and inode and never upload their empty
bytes. A move is hydrated before its atomic rename commit. An empty `0000`
file matching an authenticated incoming row can be adopted as its placeholder;
other unrecognized empty `0000` files are skipped. Unlocking a placeholder
requests hydration, and a real local write is treated as a normal edit.

Placeholder creation batches at most 1,000 index entries. Empty files need no
data flush; each affected parent directory is synced before the index batch
commits. A crash can leave unindexed placeholders, which a replayed incoming
row adopts, while the pull cursor stays behind uncommitted entries. Downloaded
contents still authenticate and fsync before their index entries can commit.

The private index shares directory prefixes and uses a versioned binary entry
format. Existing JSON indexes migrate in one SQLite transaction. Entries do not
retain sealed metadata: hydration derives the original path id and sealed size,
authenticates the content stream bound to that folder, blob and path, and checks
the plaintext size and hash against the index before publication. A moved
placeholder keeps its original path binding until the rename commits. Unapplied
remote updates retain their sealed metadata until authenticated and applied.

Large on-demand folders may collapse: their directory exists, while unfetched
descendants live only in the private index. Their intentional absence never
becomes a local delete or contributes to the mass-removal baseline. Removing
the containing collapsed directory requests deletion of its subtree under the
same deletion guard, counting the removed container's unfetched files as missing.
A new local file at an unseen remote path is preserved as
a conflict copy. Fetching one path leaves other remote-only entries untouched.

An optional guide is local derived data, atomically replaced and regenerated
after restart; it contains plaintext names, counts and sizes already available
in the private index. Its configured name is reserved at every directory level,
and matching remote entries are ignored locally. Guides never upload or enter
replica accounting. Placeholder publication and scans use verified per-directory
handles, refusing symlinks and foreign markers throughout the traversal. A pull
rechecks each directory's identity before making its index entries durable.

## Reporting a vulnerability

Report vulnerabilities through a GitHub private security advisory:

https://github.com/pnegahdar/drivesync/security/advisories/new

Include what you found, the version or commit, and a way to reproduce it.
A private advisory keeps the report unpublished until a fix is ready.
