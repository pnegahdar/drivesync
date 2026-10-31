package drivesync

import (
	"context"
	"net/http"

	"github.com/pnegahdar/drivesync/internal/engine"
)

// Client manages folders through HTTP or an authenticated in-process authority.
// Its replication and encryption protocol is private to the library.
type Client struct{ client engine.Client }

// NewHTTPClient connects to an authority. Use HTTPS outside local tests.
func NewHTTPClient(url string, headers http.Header) *Client {
	return &Client{client: engine.NewHTTPClient(url, headers)}
}

// NewHTTPClientWithClient is NewHTTPClient sending authority requests through
// client, such as one whose transport adds credentials that rotate. Blob
// transfers to presigned URLs never use it, so those credentials stay off
// blob hosts. A nil client behaves as NewHTTPClient.
func NewHTTPClientWithClient(url string, headers http.Header, client *http.Client) *Client {
	c := engine.NewHTTPClient(url, headers)
	if client != nil {
		c.HTTP = client
	}
	return &Client{client: c}
}

// CreateFolder computes the key check locally; the key is never sent to the
// authority. Keep it securely and supply the same key to every attachment.
func (c *Client) CreateFolder(ctx context.Context, spec FolderSpec, key FolderKey) (Folder, error) {
	if key == (FolderKey{}) {
		return Folder{}, ErrKey
	}
	f, e := engine.CreateFolder(ctx, c.client, engine.FolderSpec{Name: spec.Name, Description: spec.Description, Limits: engine.Limits(spec.Limits)}, engine.FolderKey(key))
	out := publicFolder(f)
	if e == nil {
		out.Role = Owner
	}
	return out, publicError(e)
}
func (c *Client) Grant(ctx context.Context, id string, p Principal, role Role) error {
	return publicError(c.client.Grant(ctx, id, internalPrincipal(p), engine.Role(role)))
}
func (c *Client) Revoke(ctx context.Context, id string, p Principal) error {
	return publicError(c.client.Revoke(ctx, id, internalPrincipal(p)))
}
func (c *Client) SetLimits(ctx context.Context, id string, limits Limits) error {
	return publicError(c.client.SetLimits(ctx, id, engine.Limits(limits)))
}
func (c *Client) DeleteFolder(ctx context.Context, id string) error {
	return publicError(c.client.DeleteFolder(ctx, id))
}
func (c *Client) GetFolder(ctx context.Context, id string) (Folder, error) {
	f, e := c.client.GetFolder(ctx, id)
	return publicFolder(f), publicError(e)
}
func (c *Client) ListFolders(ctx context.Context) ([]Folder, error) {
	fs, e := c.client.ListFolders(ctx)
	out := make([]Folder, 0, len(fs))
	for _, f := range fs {
		out = append(out, publicFolder(f))
	}
	return out, publicError(e)
}
