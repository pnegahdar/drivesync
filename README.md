# drivesync

A standalone Go library for encrypted, Dropbox-like shared folders. Replicas keep
ordinary files on disk, so application reads need no network call or FUSE mount.
Linux uses inotify; macOS uses one FSEvents stream through purego. Both have a
periodic rescan safety net and build with `CGO_ENABLED=0`.

```go
meta, err := drivesync.OpenSQLiteMetaStore("metadata.sqlite")
if err != nil { panic(err) }
defer meta.Close()
blobs, err := drivesync.OpenDirectoryBlobStore("blob-data")
if err != nil { panic(err) }
defer blobs.Close()
server := drivesync.NewServer(meta, blobs)
client := server.Client(drivesync.Principal{Tenant: "acme", Subject: "alice"})
key := drivesync.NewFolderKey() // distribute securely to authorized replicas
folder, err := client.CreateFolder(ctx, drivesync.FolderSpec{
    Name: "reference", Description: "Shared reference files",
    Limits: drivesync.Limits{MaxFileBytes: 20 << 20, MaxTotalBytes: 100 << 20, MaxFiles: 10000},
    KeyCheck: drivesync.KeyCheck(key),
})
if err != nil { panic(err) }
replica, err := drivesync.Attach(ctx, client, folder.ID, key, "./reference", drivesync.Options{
    Name: "laptop", StateDir: "./reference-state", // outside the attachment
})
if err != nil { panic(err) }
defer replica.Close()
status := replica.Status()
_ = status
```

Import `github.com/pnegahdar/drivesync`. Go 1.27.1 or newer is required by the
pinned dependency set. Direct dependencies are fsnotify, purego, blake3, and
modernc SQLite; remaining module requirements are their transitive dependencies.

The authenticated `Client` interface exposes `CreateFolder`, `GetFolder`,
`ListFolders`, `Grant`, `Revoke`, `SetLimits`, `DeleteFolder`, `Reserve`, `Upload`,
`CancelUpload`, `Commit`, `Changes`, `Download`, and long-poll `Wait`. Every server
operation also accepts an explicit principal. `Server.Subscribe` supplies an
in-process event channel with a terminal error on revocation or deletion.
`Attach` starts automatic sync; `Options.Manual` and `Replica.Sync(ctx)` allow
an embedder to schedule it. `Close`, `Status`, and `RetryRejected` complete the
replica API. Status is also saved to the private state directory for the CLI.

For HTTP, mount `server.Handler(authenticator)` and use
`NewHTTPClient(baseURL, headers)`. The authenticator must derive the opaque
principal from trusted credentials; identity fields in a request cannot choose
the caller. Use HTTPS outside localhost. RPC requests are limited to 1 MiB;
HTTP clients default to a 64 MiB metadata response limit, configurable through
`MaxResponseBytes` (negative disables it). Blob bodies stream independently.
The HTTP subscription transport is long polling rather than SSE.

`MetaStore.Transaction` is the embedding seam for durable, atomic metadata
storage. Its callback sees folder records, current file rows and reservations;
errors roll back all changes. The SQLite implementation persists separate rows,
serializes writers, and uses fresh targeted authorization queries during blob
streaming. Its general transaction API materializes metadata and is intended
for modest authorities; a very large service should implement a more specialized
backend/API before deploying at scale. Replicas currently hash the full local
tree on each sync; incremental dirty-path scans are future performance work
for large content sets. `BlobStore` creates immutable objects,
opens streams, reports actual sizes and deletes objects. Memory and local-directory
stores are included. An embedder can add S3/R2 or a presigned upload adapter
without pulling an AWS SDK into this module. Configure `Server.Quotas`, TTL and
clock before using the server concurrently.

Security and behavior:

- No grant and no folder produce the same `ErrDenied`. Principals granted owner access manage
  grants and limits; writers edit files; readers download. Explicit grants can
  cross tenants. Grants name the complete `{Tenant, Subject}` pair of nonempty
  UTF-8 strings (invalid UTF-8 is rejected, never normalized). The creator
  remains the immutable primary owner and quota account; owner grants delegate
  management without transferring that account.
- Folder labels/descriptions are control-plane fields visible to the authority.
  File paths, contents, types, modes, hashes and other file metadata are encrypted.
  Sizes, file counts, path IDs and access relationships remain visible.
- The caller supplies a 32-byte folder key. HKDF separates content, metadata and
  path MAC keys. AES-GCM chunks authenticate folder, blob, path, chunk index and
  final marker, with a fresh random salt deriving each stream key. Metadata authenticates folder and path and checks the blob binding.
  Key checks reject a wrong key before any local changes.
- Revocation/deletion terminate waits and invalidate tickets immediately. Streaming
  blob reads recheck authority on every read. Bytes already delivered or cached
  cannot be revoked; removing a principal does not rotate the shared key.
- CAS conflicts keep a durable local `name (conflict from replica timestamp-id).ext`
  copy before taking the winner. Conflict copies sync as regular files. Renames
  are create-plus-delete; empty directories use encrypted directory markers.
  Tombstones retain versions, including when a path is recreated.
- Downloads use authenticated staging, fsync, rename and directory fsync. An
  `os.Root` confines filesystem operations, including when parents change. Paths
  are validated after decryption; symlinks are skipped/rejected. Case collisions
  get stable local aliases, whose edits map back to the original remote path.
- `.drivesyncignore` accepts shell-style globs, comments and directory patterns.
  Defaults exclude `.DS_Store`, editor temporary files and internal staging files.
  State stays outside the attachment and is locked against a second attachment.
- Limits count **sealed bytes** (use `SealedSize` to calculate overhead) and live
  entries, including directory markers. Zero means unlimited. Reservations count
  replacement growth and new entries atomically, expire, and are bound to one
  principal, folder, path and base version. Commit verifies actual blob sizes and
  rechecks lowered quotas. Deletes remain possible above quota.
- Rejected files stay locally and appear in `Status`. Automatic retries wait at
  least a minute by default; a content change or `RetryRejected` retries sooner.

Deferred: chunking/dedup, snapshots/history, FUSE, key rotation, presigned URL
issuance, distributed push infrastructure, and blob garbage collection. Superseded,
expired and deleted objects can remain in the blob store even though logical usage
is freed; plan a reachability-based collector before operating long-lived stores.
Synchronization preserves observed concurrent edits, but arbitrary writes through
an already-open descriptor during a filesystem replacement are not transactional
with the replica. It is also a current-row protocol: ordinary later edits and
intentional deletes do not provide historical recovery. Network retries may create
extra conflict copies, favoring retention over deduplicating identical content.

Manual testing:

```sh
export DRIVESYNC_TOKEN='your-local-test-token'
go run ./cmd/drivesync serve -data ./server-data
# In another terminal; save the returned ID and key:
go run ./cmd/drivesync create -name reference
go run ./cmd/drivesync attach -folder ID -key HEX_KEY -dir ./a -state ./a-state -name a
# In another terminal:
go run ./cmd/drivesync attach -folder ID -key HEX_KEY -dir ./b -state ./b-state -name b
go run ./cmd/drivesync status -state ./a-state
```

The CLI's server authenticator intentionally maps one bearer token to one
principal. Production applications supply their own authenticator and key delivery.
See [AGENTS.md](AGENTS.md) for security invariants and verification commands.

Verified on macOS arm64 with Go 1.27.1: the race suite, CGO-disabled suite,
Linux vet and cross-builds, all three fuzz targets, and the real FSEvents/CLI
smoke tests pass. A three-iteration benchmark measured 274 ms for small-file
propagation, 280 ms for an initial 10,000-file scan, and 275 ms for an indexed
rescan (2026-10-03; results depend on hardware and concurrent load). Linux
was cross-built and vetted here; its runtime tests have not been executed.
