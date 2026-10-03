# Public API simplification

Base: 877c60e. Replication, encryption, metadata transactions and detailed state
now live in `internal/engine`. Public clients manage folders and attach replicas;
no public operation accepts or returns protocol records. `MetaStore` is concrete
SQLite; `BlobStore` and `QuotaPolicy` are the extension points. `Client` is a
concrete HTTP/in-process handle. TTLs/clock/policy move into `ServerOptions`.

`CreateFolder(ctx, spec, key)` computes the key check locally. Public Folder has
seven human-facing fields, Usage has three, and Status contains actionable state.
`Run` owns recovery/expiry/collection/compaction, with one authority process per
store. Publication barriers prevent same-process recovery from cancelling an
active stream and honor cancellation while waiting. Protocol tests remain intact
under internal/engine and reviewtests; root tests exercise the public facade over
both transports. Root fuzz/benchmark adapters exercise the same private algorithms.

## Before: go doc -short

```text
const ChunkSize = 64 * 1024
const MaxGrants = 256
const MaxRequestBytes int64 = 1 << 50
const MaxWaitsPerPrincipal = 32
const RowCost int64 = 256
var ErrDenied = errors.New("not found or access denied") ...
func CheckKey(k FolderKey, check []byte) error
func KeyCheck(k FolderKey) []byte
func NormalizePath(s string) (string, error)
func OpenContent(w io.Writer, r io.Reader, k FolderKey, folder, blob, pid string) error
func PathID(k FolderKey, folder, p string) (string, error)
func SealContent(w io.Writer, r io.Reader, k FolderKey, folder, blob, pid string) error
func SealMetadata(k FolderKey, folder, pid string, m FileMetadata) ([]byte, error)
func SealedSize(n int64) int64
type Account struct{ ... }
type Authenticator func(*http.Request) (Principal, error)
type BlobStore interface{ ... }
type Client interface{ ... }
type ConflictError struct{ ... }
type Delta struct{ ... }
type DirectoryBlobStore struct{ ... }
    func OpenDirectoryBlobStore(dir string) (*DirectoryBlobStore, error)
type Event struct{ ... }
type FileMetadata struct{ ... }
    func OpenMetadata(k FolderKey, folder string, row Row) (FileMetadata, error)
type Folder struct{ ... }
type FolderKey [32]byte
    func NewFolderKey() FolderKey
type FolderRecord struct{ ... }
type FolderSpec struct{ ... }
type Garbage struct{ ... }
type HTTPClient struct{ ... }
    func NewHTTPClient(base string, headers http.Header) *HTTPClient
type InProcessClient struct{ ... }
type LimitError struct{ ... }
type Limits struct{ ... }
type MemoryBlobStore struct{ ... }
    func NewMemoryBlobStore() *MemoryBlobStore
type MetaStore interface{ ... }
type Metadata struct{ ... }
type Mutation struct{ ... }
type NotificationSource interface{ ... }
type Notifications = notifications
    func NewNotifications() *Notifications
type Options struct{ ... }
type Principal struct{ ... }
type QuarantinedRow struct{ ... }
type Quota struct{ ... }
type QuotaFunc func(context.Context, Principal) (Quota, error)
type QuotaPolicy interface{ ... }
type Rejection struct{ ... }
type Replica struct{ ... }
    func Attach(ctx context.Context, c Client, id string, k FolderKey, dir string, o Options) (*Replica, error)
type Role string
    const Owner Role = "owner" ...
type Row struct{ ... }
type SQLiteMetaStore struct{ ... }
    func OpenSQLiteMetaStore(name string) (*SQLiteMetaStore, error)
type Scope struct{ ... }
    func ScopeFromContext(ctx context.Context) (Scope, bool)
type Server struct{ ... }
    func NewServer(meta MetaStore, blobs BlobStore) *Server
type Status struct{ ... }
type Ticket struct{ ... }
type UploadRequest struct{ ... }
type Usage struct{ ... }
```

## After: go doc -short

```text
var ErrDenied = engine.ErrDenied ...
type Authenticator func(*http.Request) (Principal, error)
type BlobStore interface{ ... }
type Client struct{ ... }
    func NewHTTPClient(url string, headers http.Header) *Client
type DirectoryBlobStore struct{ ... }
    func OpenDirectoryBlobStore(path string) (*DirectoryBlobStore, error)
type Folder struct{ ... }
type FolderKey [32]byte
    func NewFolderKey() FolderKey
type FolderSpec struct{ ... }
type LimitError struct{ ... }
type Limits struct{ ... }
type MemoryBlobStore struct{ ... }
    func NewMemoryBlobStore() *MemoryBlobStore
type MetaStore struct{ ... }
    func OpenSQLiteMetaStore(path string) (*MetaStore, error)
type Options struct{ ... }
type Principal struct{ ... }
type Quota struct{ ... }
type QuotaFunc func(context.Context, Principal) (Quota, error)
type QuotaPolicy interface{ ... }
type Rejection struct{ ... }
type Replica struct{ ... }
    func Attach(ctx context.Context, c *Client, id string, key FolderKey, dir string, ...) (*Replica, error)
type Role string
    const Owner Role = "owner" ...
type Server struct{ ... }
    func NewServer(meta *MetaStore, blobs BlobStore, opts ServerOptions) *Server
type ServerOptions struct{ ... }
type Status struct{ ... }
type Usage struct{ ... }
```

## Public handle methods

```go
func (s *Server) Run(context.Context) error
func (s *Server) Handler(Authenticator) http.Handler
func (s *Server) Client(Principal) *Client

func (c *Client) CreateFolder(context.Context, FolderSpec, FolderKey) (Folder, error)
func (c *Client) Grant(context.Context, string, Principal, Role) error
func (c *Client) Revoke(context.Context, string, Principal) error
func (c *Client) SetLimits(context.Context, string, Limits) error
func (c *Client) ListFolders(context.Context) ([]Folder, error)
func (c *Client) GetFolder(context.Context, string) (Folder, error)
func (c *Client) DeleteFolder(context.Context, string) error

func (r *Replica) Status() Status
func (r *Replica) Sync(context.Context) error
func (r *Replica) Close() error
```

## Verification

Executed on macOS arm64 / Go 1.27.1 (Apple M6), all passing:

```sh
go test -race ./... -count=1 -timeout 180s
go test -race ./cmd/drivesync -count=1
CGO_ENABLED=0 go test ./... -count=1 -timeout 120s
CGO_ENABLED=0 GOOS=linux go vet ./...
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux.test .
CGO_ENABLED=0 GOOS=linux go test -c -o /tmp/drivesync-linux-engine.test ./internal/engine
CGO_ENABLED=0 GOOS=linux go build -o /tmp/drivesync-linux ./cmd/drivesync
for target in FuzzNormalizePath FuzzWireDecode FuzzOpenContent; do
  go test -run '^$' -fuzz="^${target}$" -fuzztime=3s -parallel=2 .
done
go test -run '^TestWatcherPropagation$' -count=1 -v ./internal/engine
go test -run '^$' -bench 'Benchmark(SmallFilePropagation|Scan10K|IndexedScan10K)$' -benchtime=3x .
```

Race: public package 1.996 s, private engine 61.173 s, confirmation Opus
152.732 s, Opus2 30.325 s. The CLI status projection was completed during the
race run, then its new test passed separately under race. Pure-Go: engine
21.302 s, confirmation Opus 32.227 s. Simulations retain seeds 42, 9817 and
20261003. The three fuzz targets ran 6,489 / 3,983 / 60,038 executions.

Native FSEvents propagation: 371 ms. Three-iteration benchmarks: small-file
propagation 324 ms/op, initial 10k scan 441 ms/op, indexed scan 404 ms/op.
These are short local observations; the scanner algorithm is unchanged.
Linux was vetted and cross-built, including both public and private test
binaries; Linux runtime tests were not run.

New public tests cover folder management and two-way replication through both
transports, hidden protocol fields, local key-check computation, policy/clock
options and public limit errors. Maintenance tests exercise upload recovery,
active-publication exclusion, cancellation and shared worker admission.
CLI status projects the same actionable fields as Replica.Status.

Round-4 repairs are the preceding commit, 877c60e. Its complete per-finding
summary, proof provenance and net delta (+2,499 lines; +361 production Go lines)
remain in [SECURITY_FIXES.md](SECURITY_FIXES.md).

Last-QA additions on 5365ec9: `ParseFolderKey` imports checked hex keys;
`Replica.Retry()` clears actionable backoff and acknowledges deliberate mass
removal. `ErrWaitLimit` distinguishes exhausted push-wait admission. Replication
and crypto types remain private; folder creation salts its check after obtaining
an authority-issued, principal-bound creation challenge. The `go doc -short`
surface gains one line for `ParseFolderKey`; method documentation describes setup
contexts, dedicated state, random keys, trusted principals and HTTPS.
