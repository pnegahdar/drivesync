# drivesync

drivesync shares an encrypted folder through ordinary directories on macOS and
Linux. Create a folder with a name and a key, then attach it on each node.
Reads are normal local files. Replicas sync both ways. There is no FUSE layer.
Pure-Go builds are supported.

The authority stores ciphertext and control-plane records. It does not receive
the folder key, file paths, file contents, or file metadata.
[SECURITY.md](SECURITY.md) describes what the server can see, what a key holder
can do, and the local-disk guarantees.

## Quickstart

SQLite metadata is one authority process per database file. Postgres metadata,
opened with `OpenPostgresMetaStore`, can be shared by several authority
processes. The pool needs at least three connections: one stays on LISTEN and
two remain for transactions. Each process listens for the others' commits. An
upload records a lease renewed while bytes flow; recovery retires only a
ticket whose lease has expired. A garbage-collection pass claims a lease row
(a holder id and an expiry, renewed during the pass) or skips when another
process holds an unexpired lease. A frozen holder stops blocking once that
lease expires. `auth` authenticates each HTTP request and returns a
`Principal{Tenant, Subject}`. `plans` implements `QuotaPolicy`.

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
```

`Run` recovers interrupted uploads, expires reservations, collects garbage, and
compacts tombstones. Cancel it and wait before closing the stores.

On a client node, or with `srv.Client(principal)` inside the authority process:

```go
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
status := r.Status()
```

Preserve the key and distribute it to each attachment. Creation computes the
key check locally. Folder ids are random 128-bit ids. Names are unique per
primary owner. `Grant`, `Revoke`, `SetLimits`, `ListFolders`, `GetFolder`, and
`DeleteFolder` manage the folder.

`Attach`'s context bounds setup. The replica runs until `Close`. `Sync` forces
one pass. `Status` reports pending bytes, conflicts, rejections, quarantine,
and errors. `Retry` retries rejected or quarantined transfers and acknowledges
an intentional mass removal.

For a new node that needs only part of a drive, attach with
`Options{OnDemand: true}`. Sync lists remote files as empty regular files with
permission bits `0000`. These placeholders contain no file data. Fetch content
before opening it:

```go
entries, err := r.Entries("projects/**") // Local listing, including plaintext sizes.
result, err := r.Fetch(ctx, "projects/current", "notes/*.md")
// Check result.Failed; result.Unmatched reports patterns matching nothing.
```

Patterns select exact paths, whole folders, or globs (`*`, `?`, `[]`, `**`). No
patterns, or the root pattern `"."`, selects everything. A `*` stays within one
path component; `**` can span folders. `Fetch` authenticates bytes before replacing each
placeholder and reports fetched paths, bytes, and files already present.
Downloaded files stay downloaded and receive subsequent remote updates.
`Status.Placeholders` and `PlaceholderBytes` describe what remains unfetched.
Changing a placeholder's permissions requests its content on the next sync.
Moving a placeholder fetches its content before committing the move; copying an
empty placeholder is skipped. Reattaching with `OnDemand: false` fetches all
remaining placeholders. This API does not intercept ordinary file opens;
applications that need transparent reads can call `Fetch` from their mount layer.

On-demand replicas expand folders breadth first. `MaxFolderPlaceholders`
(default 1,000 direct entries) and `MaxPlaceholders` (default 20,000 entries
in total) bound local materialization. Larger folders collapse to an existing
directory: their children remain in the local index, with no filesystem entry
until fetched. `Entries` lists these remote-only paths and `Fetch` creates only
the requested contents and their parents. Removing a collapsed directory locally
deletes its remote subtree, subject to the usual mass-removal guard. New local
writes at unseen remote paths are preserved as conflict copies.

Set both `GuideName` and `Guide` to put a local guide at the root and in each
collapsed folder. It contains your text and an inventory of unfetched entries,
up to 1,000 lines, with folder file counts and plaintext sizes. Guides refresh
after sync and fetch, and disappear when their folder expands or on-demand mode
is disabled. `GuideName` is reserved at every level, including when on-demand
mode is disabled: remote entries with that name are ignored locally. Guides
never upload, count toward limits, or produce conflict copies.

New placeholders publish in batches of up to 1,000 entries, with one directory
barrier per affected parent before the index commits. An interrupted batch can
leave empty placeholders without index entries; the next sync adopts those
whose paths match authenticated incoming rows.

Replica state stores directory prefixes once and uses compact binary entries
with raw hashes and blob ids. Existing local indexes migrate automatically on
attach, in one transaction. Authority rows, encrypted metadata and the wire
format remain compatible with earlier stations.

`Options.Downloads` bounds parallel transfers (zero selects eight, at most 256).
Transfers stage ciphertext in bounded batches, with at most 512 MiB queued on
disk. Larger individual files stream directly through authentication. Publication
remains serial, with the usual conflict preservation and fsync guarantees.

Authorities with a `BlobURLStore` can serve fresh replicas a compressed packed
file list with one direct download, followed by `Changes` to catch up. Set
`ServerOptions.SnapshotRows` to the minimum live row count (zero selects 1,000;
negative disables snapshots). Snapshots contain encrypted metadata, are derived
data outside folder quota, and are removed with the folder. The blob store must
accept and sign the reserved blob key `snapshot`, alongside normal hex blob IDs.
They rebuild when missing or when more than the larger of 1,000 rows and 10%
of live rows changed. Older authorities and failed snapshots use `Changes`.

A small CLI exercises the same API with one local principal. Set
`DRIVESYNC_TOKEN`. Pass the key in `DRIVESYNC_KEY` or `-key-file`, never in
argv.

```sh
go run ./cmd/drivesync serve -data ./drivesync-data
go run ./cmd/drivesync create -name shared
go run ./cmd/drivesync attach -folder ID -key-file ./folder.key -dir ./shared -state ./replica-state
go run ./cmd/drivesync status -state ./replica-state
```

## Limits and quotas

`Limits` bounds sealed bytes, live entries, and retained rows. `MaxFileBytes`
is the sealed size, including encryption overhead. `MaxFiles` counts live
entries, including directory markers. `MaxRows` bounds retained rows and
garbage. Zero `MaxRows` selects 16 times `MaxFiles`, capped at one million.
Other zero limits mean unlimited.

Sharing, including a grant in the same tenant, requires a positive byte cap,
row cap, and per-file cap. That capacity is charged to the primary owner until
the folder is deleted and cleaned up. Reservations are atomic, and commit
checks the stored size. Rejected files stay on the local disk with a reason
and a retry backoff.

`Folder` reports id, name, description, owner, role, limits, and usage.
Usage is sealed bytes, live entries, and reserved bytes.

## Platform support

macOS and Linux are supported, including `CGO_ENABLED=0`. macOS watches the
tree with FSEvents through purego. Linux uses inotify. Both debounce for 150 ms
and rescan periodically. kqueue is not used. Other operating systems are not
supported.

## How it works

A replica scans its directory, encrypts new bytes, and uploads ciphertext.
Peers download into a staging file, authenticate it, then publish with fsync
and rename. Renames commit as one create/delete pair. A losing local write is
kept as a conflict copy. Matching authenticated content is adopted in place,
so an existing tree can be attached without uploading those bytes again.

Path ids, metadata, and content use separate keys. Chunks are bound to the
folder, blob, path, index, and a final flag. Case collisions keep both
contents under distinct local names. Only the owner execute bit syncs. Group
and other permission bits stay local.

Default state lives in `os.UserCacheDir()/drivesync/<folder>/<root-hash>` with
private permissions. It holds plaintext paths, so keep it outside other shared
folders. `Options.StateDir` must be dedicated to that attachment. A nonempty
directory without the matching state marker is refused. `.drivesync*` names
are reserved. `.drivesyncignore` accepts globs, root-anchored `/` patterns,
`**`, comments, and directory patterns, matched under case-folded NFC. Negation
and escapes are reported as unsupported.

`.git/` is ignored by default. Sync the working tree, and let git carry history.

The included blob stores are an in-memory store and a durable local directory.
`BlobStore.Put` must publish only after a clean EOF. An embedder can add an
object-store backend without taking an SDK dependency. Metadata is SQLite or
Postgres. `OpenPostgresMetaStore` needs a pool of at least three connections.
`QuotaPolicy` is the other extension point.

A blob store can also implement `BlobURLStore.DownloadURL` to return a URL for
one sealed blob. The authority authorizes each blob before issuing a URL with a
five-minute lifetime. Replicas download directly using an HTTP client with no
authority headers, falling back to the authority when signing or direct delivery
fails. Existing authorities without URL support continue to work. Treat signed
URLs as secrets; revocation cannot recall URLs already issued before they expire.

Fresh databases are required. This version does not migrate older schemas, and
it does not provide historical recovery, key rotation, or chunk deduplication.

## License

The code is licensed under Apache-2.0. Copyright 2026 Parham Negahdar. See
[LICENSE](LICENSE).

Unicode path handling is checked against the Unicode Consortium's
NormalizationTest data, which remains under the Unicode License. See
[UNICODE_LICENSE.txt](UNICODE_LICENSE.txt). The corpus is
`internal/engine/testdata/NormalizationTest.txt.gz`.
