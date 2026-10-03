package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Authenticator func(*http.Request) (Principal, error)
type WireRequest struct {
	Op, Folder, Ticket, Blob string
	Spec                     FolderSpec
	Grantee                  Principal
	Role                     Role
	Limits                   Limits
	Upload                   UploadRequest
	Mutations                []Mutation
	After                    uint64
	Page                     string
	Until                    uint64
}
type wireError struct {
	Code  string
	Paths []string    `json:",omitempty"`
	Limit *LimitError `json:",omitempty"`
}
type wireResponse struct {
	Challenge FolderChallenge
	Folder    Folder
	Folders   []Folder
	Ticket    Ticket
	Delta     Delta
	Version   uint64
	Error     *wireError `json:",omitempty"`
}

func encodeError(e error) *wireError {
	if e == nil {
		return nil
	}
	var l *LimitError
	if errors.As(e, &l) {
		return &wireError{Code: "limit", Limit: l}
	}
	var conflict *ConflictError
	if errors.As(e, &conflict) {
		return &wireError{Code: "conflict", Paths: conflict.Paths}
	}
	code := "internal"
	switch {
	case errors.Is(e, errFullRestart):
		code = "full_restart"
	case errors.Is(e, ErrWaitLimit):
		code = "wait_limit"
	case errors.Is(e, ErrQuota):
		code = "quota"
	case errors.Is(e, ErrBusy):
		code = "busy"
	case errors.Is(e, ErrExpired):
		code = "expired"
	case errors.Is(e, ErrDenied):
		code = "denied"
	case errors.Is(e, ErrConflict):
		code = "conflict"
	case errors.Is(e, ErrInvalid):
		code = "invalid"
	case errors.Is(e, ErrIntegrity):
		code = "integrity"
	case errors.Is(e, ErrKey):
		code = "key"
	case errors.Is(e, context.Canceled):
		code = "canceled"
	case errors.Is(e, context.DeadlineExceeded):
		code = "deadline"
	}
	return &wireError{Code: code}
}
func decodeError(e *wireError) error {
	if e == nil {
		return nil
	}
	switch e.Code {
	case "full_restart":
		return errFullRestart
	case "wait_limit":
		return ErrWaitLimit
	case "denied":
		return ErrDenied
	case "quota":
		return ErrQuota
	case "busy":
		return ErrBusy
	case "expired":
		return ErrExpired
	case "conflict":
		return &ConflictError{Paths: e.Paths}
	case "invalid":
		return ErrInvalid
	case "integrity":
		return ErrIntegrity
	case "key":
		return ErrKey
	case "limit":
		if e.Limit != nil {
			return e.Limit
		}
	case "canceled":
		return context.Canceled
	case "deadline":
		return context.DeadlineExceeded
	}
	return errors.New("drivesync transport error")
}
func DecodeWire(r io.Reader, v any) error {
	b, e := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if e != nil || len(b) > 1<<20 {
		return ErrInvalid
	}
	if !validJSONString(b) {
		return ErrInvalid
	}
	return decodeJSON(bytes.NewReader(b), v)
}
func decodeJSON(r io.Reader, v any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return ErrInvalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrInvalid
	}
	return nil
}
func (s *Server) Handler(auth Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		reply := func(out wireResponse, e error) {
			out.Error = encodeError(e)
			w.Header().Set("Content-Type", "application/json")
			if errors.Is(e, ErrDenied) {
				w.WriteHeader(http.StatusNotFound)
			} else if e != nil {
				w.WriteHeader(http.StatusBadRequest)
			}
			_ = json.NewEncoder(w).Encode(out)
		}
		if auth == nil {
			reply(wireResponse{}, ErrDenied)
			return
		}
		p, e := auth(r)
		if e != nil || !p.valid() {
			reply(wireResponse{}, ErrDenied)
			return
		}
		c := s.Client(p)
		if r.URL.Path == "/rpc" && r.Method == http.MethodPost {
			media, _, me := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if me != nil || media != "application/json" {
				http.Error(w, "application/json required", http.StatusUnsupportedMediaType)
				return
			}
			var req WireRequest
			if e = DecodeWire(http.MaxBytesReader(w, r.Body, 1<<20), &req); e != nil {
				reply(wireResponse{}, e)
				return
			}
			var out wireResponse
			switch req.Op {
			case "prepare_folder":
				out.Challenge, e = s.PrepareFolder(r.Context(), p)
			case "create":
				out.Folder, e = c.CreateFolder(r.Context(), req.Spec)
			case "grant":
				e = c.Grant(r.Context(), req.Folder, req.Grantee, req.Role)
			case "revoke":
				e = c.Revoke(r.Context(), req.Folder, req.Grantee)
			case "delete":
				e = c.DeleteFolder(r.Context(), req.Folder)
			case "limits":
				e = c.SetLimits(r.Context(), req.Folder, req.Limits)
			case "list":
				out.Folders, e = c.ListFolders(r.Context())
			case "get":
				out.Folder, e = c.GetFolder(r.Context(), req.Folder)
			case "reserve":
				out.Ticket, e = c.Reserve(r.Context(), req.Folder, req.Upload)
			case "cancel":
				e = c.CancelUpload(r.Context(), req.Folder, req.Ticket)
			case "commit":
				out.Delta, e = c.Commit(r.Context(), req.Folder, req.Mutations)
			case "changes":
				out.Delta, e = s.ChangesPage(r.Context(), p, req.Folder, req.After, req.Until, req.Page)
			case "wait":
				ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
				out.Version, e = c.Wait(ctx, req.Folder, req.After)
				cancel()
				if errors.Is(e, context.DeadlineExceeded) && r.Context().Err() == nil {
					var f Folder
					f, e = c.GetFolder(r.Context(), req.Folder)
					out.Version = f.Version
				}
			default:
				e = ErrInvalid
			}
			reply(out, e)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) == 3 && validID(parts[1]) && validID(parts[2]) {
			if parts[0] == "upload" && r.Method == http.MethodPut {
				e = c.Upload(r.Context(), parts[1], Ticket{ID: parts[2]}, r.Body)
				reply(wireResponse{}, e)
				return
			}
			if parts[0] == "download" && r.Method == http.MethodGet {
				stream, e := c.Download(r.Context(), parts[1], parts[2])
				if e != nil {
					reply(wireResponse{}, e)
					return
				}
				defer stream.Close()
				w.Header().Set("Content-Type", "application/octet-stream")
				if _, e = io.Copy(w, stream); e != nil {
					panic(http.ErrAbortHandler)
				}
				return
			}
		}
		reply(wireResponse{}, ErrDenied)
	})
}

// HTTPClient carries caller-provided authentication headers. Use HTTPS outside local tests.
type HTTPClient struct {
	BaseURL          string
	HTTP             *http.Client
	Headers          http.Header
	MaxResponseBytes int64
}

func NewHTTPClient(base string, headers http.Header) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{}, Headers: headers.Clone(), MaxResponseBytes: 64 << 20}
}
func (c *HTTPClient) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	if _, e := url.ParseRequestURI(c.BaseURL); e != nil {
		return nil, e
	}
	r, e := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if e != nil {
		return nil, e
	}
	r.Header = c.Headers.Clone()
	if r.Header == nil {
		r.Header = make(http.Header)
	}
	if path == "/rpc" {
		r.Header.Set("Content-Type", "application/json")
	}
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(r)
}
func (c *HTTPClient) rpc(ctx context.Context, in WireRequest) (wireResponse, error) {
	var out wireResponse
	b, e := json.Marshal(in)
	if e != nil {
		return out, e
	}
	r, e := c.request(ctx, http.MethodPost, "/rpc", bytes.NewReader(b))
	if e != nil {
		return out, e
	}
	defer r.Body.Close()
	if e = c.decodeResponse(r.Body, &out); e != nil {
		return out, e
	}
	if out.Error != nil {
		return out, decodeError(out.Error)
	}
	if r.StatusCode != http.StatusOK {
		return out, fmt.Errorf("drivesync HTTP %d", r.StatusCode)
	}
	return out, nil
}
func (c *HTTPClient) CreateFolder(x context.Context, v FolderSpec) (Folder, error) {
	r, e := c.rpc(x, WireRequest{Op: "create", Spec: v})
	return r.Folder, e
}
func (c *HTTPClient) Grant(x context.Context, id string, p Principal, r Role) error {
	if !p.valid() {
		return ErrInvalid
	}
	_, e := c.rpc(x, WireRequest{Op: "grant", Folder: id, Grantee: p, Role: r})
	return e
}
func (c *HTTPClient) Revoke(x context.Context, id string, p Principal) error {
	if !p.valid() {
		return ErrInvalid
	}
	_, e := c.rpc(x, WireRequest{Op: "revoke", Folder: id, Grantee: p})
	return e
}
func (c *HTTPClient) DeleteFolder(x context.Context, id string) error {
	_, e := c.rpc(x, WireRequest{Op: "delete", Folder: id})
	return e
}
func (c *HTTPClient) SetLimits(x context.Context, id string, l Limits) error {
	_, e := c.rpc(x, WireRequest{Op: "limits", Folder: id, Limits: l})
	return e
}
func (c *HTTPClient) ListFolders(x context.Context) ([]Folder, error) {
	r, e := c.rpc(x, WireRequest{Op: "list"})
	return r.Folders, e
}
func (c *HTTPClient) GetFolder(x context.Context, id string) (Folder, error) {
	r, e := c.rpc(x, WireRequest{Op: "get", Folder: id})
	return r.Folder, e
}
func (c *HTTPClient) Reserve(x context.Context, id string, r UploadRequest) (Ticket, error) {
	v, e := c.rpc(x, WireRequest{Op: "reserve", Folder: id, Upload: r})
	return v.Ticket, e
}
func (c *HTTPClient) CancelUpload(x context.Context, id, tid string) error {
	_, e := c.rpc(x, WireRequest{Op: "cancel", Folder: id, Ticket: tid})
	return e
}
func (c *HTTPClient) Commit(x context.Context, id string, m []Mutation) (Delta, error) {
	v, e := c.rpc(x, WireRequest{Op: "commit", Folder: id, Mutations: m})
	return v.Delta, e
}
func (c *HTTPClient) Changes(x context.Context, id string, v uint64) (Delta, error) {
	var out Delta
	page := ""
	until := uint64(0)
	for {
		r, e := c.rpc(x, WireRequest{Op: "changes", Folder: id, After: v, Until: until, Page: page})
		if errors.Is(e, errFullRestart) {
			out = Delta{}
			page = ""
			until = 0
			v = 0
			continue
		}
		if e != nil {
			return Delta{}, e
		}
		out.Rows = append(out.Rows, r.Delta.Rows...)
		out.Version = r.Delta.Version
		out.Horizon = r.Delta.Horizon
		out.Full = r.Delta.Full
		until = out.Version
		if r.Delta.Next == "" {
			return out, nil
		}
		if r.Delta.Next == page {
			return Delta{}, ErrInvalid
		}
		page = r.Delta.Next
	}
}

// Validate the raw JSON before encoding/json can replace malformed UTF-8 or
// unpaired UTF-16 escapes with U+FFFD and accidentally alias an identity.
func validJSONString(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		i++
		if i >= len(b) {
			return false
		}
		if b[i] != 'u' {
			continue
		}
		if i+4 >= len(b) {
			return false
		}
		u, e := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if u >= 0xd800 && u <= 0xdbff {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			v, e := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
			if e != nil || v < 0xdc00 || v > 0xdfff {
				return false
			}
			i += 6
		} else if u >= 0xdc00 && u <= 0xdfff {
			return false
		}
	}
	return true
}
func (c *HTTPClient) Wait(x context.Context, id string, v uint64) (uint64, error) {
	r, e := c.rpc(x, WireRequest{Op: "wait", Folder: id, After: v})
	return r.Version, e
}
func (c *HTTPClient) Upload(x context.Context, id string, t Ticket, body io.Reader) error {
	if !validID(id) || !validID(t.ID) {
		return ErrDenied
	}
	r, e := c.request(x, http.MethodPut, "/upload/"+id+"/"+t.ID, body)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	var v wireResponse
	if e = c.decodeResponse(r.Body, &v); e != nil {
		return e
	}
	if v.Error != nil {
		return decodeError(v.Error)
	}
	if r.StatusCode != http.StatusOK {
		return ErrInvalid
	}
	return nil
}
func (c *HTTPClient) Download(x context.Context, id, blob string) (io.ReadCloser, error) {
	if !validID(id) || !validID(blob) {
		return nil, ErrDenied
	}
	r, e := c.request(x, http.MethodGet, "/download/"+id+"/"+blob, nil)
	if e != nil {
		return nil, e
	}
	if r.StatusCode != http.StatusOK {
		defer r.Body.Close()
		var v wireResponse
		if e = c.decodeResponse(r.Body, &v); e != nil {
			return nil, e
		}
		return nil, decodeError(v.Error)
	}
	return r.Body, nil
}

func (c *HTTPClient) decodeResponse(r io.Reader, v any) error {
	maximum := c.MaxResponseBytes
	if maximum == 0 {
		maximum = 64 << 20
	}
	if maximum < 0 {
		return decodeJSON(r, v)
	}
	return decodeJSON(&responseLimit{r, maximum}, v)
}

type responseLimit struct {
	reader    io.Reader
	remaining int64
}

func (r *responseLimit) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, ErrInvalid
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, e := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, e
}

func (c *HTTPClient) PrepareFolder(ctx context.Context) (FolderChallenge, error) {
	r, e := c.rpc(ctx, WireRequest{Op: "prepare_folder"})
	return r.Challenge, e
}
