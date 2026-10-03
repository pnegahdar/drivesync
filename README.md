# drivesync

Encrypted shared folders in ordinary local directories. Create a folder with a
name and a key, then attach it on each node. Reads use native files; replicas sync
both ways without FUSE. Pure-Go Linux and macOS builds are supported.

```go
meta, err := drivesync.OpenSQLiteMetaStore("authority.sqlite")
if err != nil { return err }
defer meta.Close()
blobs, err := drivesync.OpenDirectoryBlobStore("blobs")
if err != nil { return err }
defer blobs.Close()

srv := drivesync.NewServer(meta, blobs, drivesync.ServerOptions{Quotas: plans})
ctx, cancel := context.WithCancel(context.Background())
done := make(chan error, 1)
go func() { done <- srv.Run(ctx) }()
defer func() { cancel(); <-done }()
mux.Handle("/drives/", http.StripPrefix("/drives", srv.Handler(auth)))

// On a client node (or use srv.Client(principal) in the authority process):
c := drivesync.NewHTTPClient("https://example.com/drives", headers)
key := drivesync.NewFolderKey()
f, err := c.CreateFolder(ctx, drivesync.FolderSpec{
    Name: "workspace", Description: "Files shared across nodes",
    Limits: drivesync.Limits{
        MaxFileBytes: 64 << 20, MaxTotalBytes: 1 << 30,
        MaxFiles: 10000, MaxRows: 50000,
    },
}, key)
if err != nil { return err }
if err := c.Grant(ctx, f.ID, bob, drivesync.Writer); err != nil { return err }
r, err := drivesync.Attach(ctx, c, f.ID, key, "./workspace", drivesync.Options{})
if err != nil { return err }
defer r.Close()
status := r.Status() // pending bytes, conflicts, rejections, quarantine and errors
```

The example combines authority/client setup for brevity; usually they run on
different nodes. `auth` authenticates each HTTP request and returns an opaque
`Principal{Tenant, Subject}`. `plans` implements `QuotaPolicy`. Preserve the key
securely and distribute it to each attachment; creation computes its check
locally, with a random salt bound to the authority-issued folder ID; the key
never goes to the authority. Folder IDs are random 128-bit IDs.
Names are unique per primary owner. `Grant`, `Revoke`, `SetLimits`, `ListFolders`,
`GetFolder` and `DeleteFolder` are the folder management API.

`Folder` contains ID, name, description, owner, role, configured limits and usage
(bytes, live entries and reserved bytes). `Replica.Sync(ctx)` forces one sync;
`Status()` reports actionable local state; `Close()` stops the replica. The
context passed to `Attach` bounds setup only; the replica lives until `Close`.
`Retry()` retries rejected/quarantined transfers and acknowledges an intentional
mass removal for the next sync. Root identity failures cannot be overridden.

Default state lives under `os.UserCacheDir()/drivesync/<folder>/<root-hash>`,
with private permissions. Explicit `Options.StateDir` must be dedicated to this
attachment, outside every attachment; a nonempty directory without its matching
state marker is refused. State contains plaintext paths: protect it and exclude
it from other sharing systems. `.drivesync*` control files are reserved and ignored.
Nested and overlapping attachments are refused, including closed attachments
whose root markers remain. Default automatic sync uses
native FSEvents through purego on macOS, inotify on Linux, a 150 ms debounce and
periodic rescans. Existing files matching authenticated remote hashes are adopted
without conflict copies. Symlinks, unsupported entries and unreadable files
appear in status errors and protect their subtrees from deletion detection.

The root contains a random `.drivesync-root` marker; saved state also binds its
device/inode. Missing or replaced roots pause sync, preventing an unmounted disk
from deleting peer files. Do not remove or copy this marker to relocate a replica;
attach a new directory with new dedicated state. If at least five tracked entries
exist and 80% disappear without matching moves, deletes pause with a status error.
Inspect the disk before calling `Retry()` to acknowledge a deliberate removal.

## Authority and extension points

`MetaStore` is a concrete SQLite handle, opened with `OpenSQLiteMetaStore` and
closed by the embedder. Its transactions, accounting and replication records
are internal. `BlobStore` and `QuotaPolicy` remain pluggable. Blob stores stream
immutable sealed objects under authority-generated IDs, report actual sizes,
open streams and delete objects. `Put` must stage until clean EOF and never publish
when its reader errors; oversize streams are terminated before publication.
Memory and durable local-directory backends
are included; an embedder can implement S3/R2 without an AWS SDK dependency.

`ServerOptions` holds quota policy, clock, reservation/tombstone TTLs and maintenance
interval. Zero TTLs use five-minute reservations and 30-day tombstones; a negative
`TombstoneTTL` disables compaction. `GCInterval` defaults to one second. Configure
these before serving. `Run(ctx)` is the single maintenance entry: interrupted
upload recovery, expiry, garbage collection and tombstone compaction. Handle its
returned errors; cancel and join it before closing stores.

**Use one authority process per metadata/blob store.** Recovery cannot prove that
another process has stopped publishing. Same-process authorities coordinate
publication and notifications. Startup recovery excludes active publications;
failed cleanup remains durable and charged. Expiry and invalidated tickets are
retired by background maintenance; they retain their charge until then. Folder
deletion revokes access in a small transaction, then retires rows in bounded
background batches. Compaction selects on readers and writes bounded path batches;
GC updates only the cleanup records it touches. Reads and authorization use separate
read-only WAL connections. Writes select touched file rows and cached usage;
accounting is linear. External pricing/size hooks honor cancellation and are
bounded to two seconds inside a five-second write transaction. Replicas currently
hash the complete local tree on sync.

## Security, limits and conflicts

Every authority operation checks access independently of HTTP authentication.
Unknown folders and denied access return the same error. Principals are nonempty
UTF-8 tenant/subject pairs, each component at most 256 bytes. Malformed raw JSON
cannot alias identities. Grants are capped at 256 per folder; the immutable
primary owner remains the quota account. Revocation and deletion invalidate
subscriptions and upload tickets immediately, including streaming access checks.
An actual grant change invalidates all outstanding tickets in that folder, so
other writers may need to retry; unchanged grants do not invalidate tickets.
Delivered/cached bytes cannot be revoked, and revocation does not rotate the key.
`Server.Client(p)` trusts the supplied principal: use it only after authenticating
inside the embedding app. Use HTTPS for HTTP clients. `/rpc` requires
`Content-Type: application/json`; HTML form requests are refused.

Use random keys only, generated by `NewFolderKey`. `ParseFolderKey` imports their
64-character hex encoding and rejects malformed and zero keys. Creation and
attachment reject zero keys too. Passphrases or their hashes are unsuitable:
salted key checks and encrypted files still permit offline candidate verification.

File paths, contents and metadata use separated keys and authenticated encryption.
The authority sees control-plane labels, sizes, counts and relationships.
Untrusted peer paths are validated and confined with `os.Root`; symlink parents
are refused. Case collisions receive unique local aliases. Downloads authenticate
staging files before fsync and rename. Losing writes survive as conflict files.
Renames commit their create/delete pairs atomically; if full, they remain local
and pending. Remote directory deletes preserve user-ignored contents, including
`.git`; the directory becomes untracked when no tracked children remain. Only
known junk (`.DS_Store`, `Thumbs.db`) is automatically removed. Tracked children
keep the delete pending. `.drivesyncignore` accepts globs,
comments and directory patterns, with case-folded NFC matching. Defaults exclude
Finder/editor temporary files and staging files. Unignored rows are fetched later.

Byte budgets charge sealed bytes, metadata and 256 bytes per retained row.
`MaxFileBytes` measures sealed content, so allow encryption overhead. `MaxFiles`
counts live entries (including directory markers); `MaxRows` bounds retained
rows and garbage. Zero rows select a safety budget of 16 times `MaxFiles`, capped
at one million; other zero limits mean unlimited. Owner `Quota.MaxFiles` bounds
retained rows across folders. Exact stored sizes are checked at commit; quota
reservations are atomic. Deletes add no charge; tombstones retain blob cleanup
until physical deletion succeeds and their row charge until compaction.

Every shared folder—including same-tenant sharing—requires explicit byte, row
and per-file caps within the owner's plan. Its capacity is allocated to that
owner, charged exactly at its cap, and remains allocated until deletion/cleanup.
Grantee activity cannot probe private usage or consume private headroom. Changes
below current bytes/rows (including reservations/garbage) are refused. Revocation,
downgrades, reductions and existing-row deletes never consult pricing. Folder
reads return stored limits. Non-primary principals receive generic owner-quota
errors. Rejected files remain local with a reason and retry backoff. Corrupt rows
are quarantined by decrypted path when available; transient transfers and local
obstacles retry independently. A short network stream retries; a truncated stored
blob whose advertised bytes were all received is quarantined. Local edits can
replace quarantined versions. Permission-only changes also sync. Status errors
clear after a successful sync. Up to 256 waits per principal keep folder push
updates active; excess waits return `ErrWaitLimit` and back off.

A malicious authority can delete or roll back files; encryption cannot prevent
this. Historical recovery, snapshots, key rotation, chunk deduplication and
presigned URL issuance are outside this prototype. Fresh databases are required.

## Manual testing

```sh
export DRIVESYNC_TOKEN='local-test-token'
go run ./cmd/drivesync serve -data ./drivesync-data
# In another terminal, using the same token:
go run ./cmd/drivesync create -name shared
# Store the printed hex key in a private file, then use the printed folder ID:
go run ./cmd/drivesync attach -folder ID -key-file ./folder.key -dir ./shared -state ./replica-state
# Or provide the key through DRIVESYNC_KEY; never put it in argv.
go run ./cmd/drivesync status -state ./replica-state
```

The CLI's bearer authenticator represents one principal for local testing;
production applications supply their own hook. See [AGENTS.md](AGENTS.md) for
required checks, [API_SURFACE.md](API_SURFACE.md) for before/after documentation,
and [SECURITY_FIXES.md](SECURITY_FIXES.md) / [QA_FIXES.md](QA_FIXES.md) for
adversarial regression history and the latest repairs.
