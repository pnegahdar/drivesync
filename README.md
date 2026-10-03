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

Run one authority process per metadata and blob store. `auth` authenticates
each HTTP request and returns a `Principal{Tenant, Subject}`. `plans`
implements `QuotaPolicy`.

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
object-store backend without taking an SDK dependency. Metadata is SQLite.
`QuotaPolicy` is the other extension point.

Fresh databases are required. This version does not migrate older schemas, and
it does not provide historical recovery, key rotation, or chunk deduplication.

## License

The code is licensed under Apache-2.0. Copyright 2026 Parham Negahdar. See
[LICENSE](LICENSE).

Unicode path handling is checked against the Unicode Consortium's
NormalizationTest data, which remains under the Unicode License. See
[UNICODE_LICENSE.txt](UNICODE_LICENSE.txt). The corpus is
`internal/engine/testdata/NormalizationTest.txt.gz`.
