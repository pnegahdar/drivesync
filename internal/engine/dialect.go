package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// sqlTx is the subset of *sql.Tx the shared metadata transaction uses.
type sqlTx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

// boundTx rewrites ? placeholders to $n. SQLite passes *sql.Tx directly so its
// SQL text stays unchanged.
type boundTx struct {
	*sql.Tx
	dollar bool
}

func (b boundTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return b.Tx.ExecContext(ctx, ph(q, b.dollar), args...)
}
func (b boundTx) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return b.Tx.QueryContext(ctx, ph(q, b.dollar), args...)
}
func (b boundTx) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return b.Tx.QueryRowContext(ctx, ph(q, b.dollar), args...)
}
func (b boundTx) PrepareContext(ctx context.Context, q string) (*sql.Stmt, error) {
	return b.Tx.PrepareContext(ctx, ph(q, b.dollar))
}

func ph(q string, dollar bool) string {
	if !dollar || !strings.Contains(q, "?") {
		return q
	}
	var b strings.Builder
	b.Grow(len(q) + 8)
	n := 1
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			n++
		} else {
			b.WriteByte(q[i])
		}
	}
	return b.String()
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "SQLSTATE 23505")
}

// persistScoped loads the scoped rows, runs fn, and writes the diff.
// It does not commit. ticketPathsInGo selects ticket rows by PathID in process
// so Postgres does not evaluate JSON in the query. A read-only scope returns
// after the callback and leaves the caller to roll the snapshot back.
func persistScoped(ctx context.Context, tx sqlTx, fn func(*Metadata) error, ticketPathsInGo bool) (map[string]bool, error) {
	scope, scoped := ScopeFromContext(ctx)
	var e error
	m := newMetadata()
	old := map[string]map[string][]byte{}
	for _, table := range []string{"folders", "grants", "files", "tickets", "garbage"} {
		old[table] = map[string][]byte{}
		q := "SELECT id,data FROM " + table
		if table == "files" {
			q = "SELECT folder || '/' || path,data FROM files"
		}
		if table == "grants" {
			q = "SELECT folder || '/' || principal,data FROM grants"
		}
		var args []any
		if scope, ok := ctx.Value(scopeKey{}).(Scope); ok {
			m.filesLoaded = !scope.NoFiles && !scope.GC && scope.Paths == nil
			filter := "id=?"
			args = []any{scope.Folder}
			if scope.Folder == "" {
				if scope.Create {
					filter = "FALSE"
					args = nil
				} else if scope.Principal.valid() {
					filter = `id IN (SELECT folder FROM grants WHERE principal=?)`
					args = []any{principalKey(scope.Principal)}
				} else {
					// Maintenance reads every folder. An empty principal is not a grant lookup.
					filter = "TRUE"
					args = nil
				}
			}
			if scope.GC {
				args = nil
				if table == "files" {
					q += " WHERE FALSE"
				}
			} else if scope.NoFiles && table == "files" {
				q += " WHERE FALSE"
				args = nil
			} else if table == "folders" {
				q += " WHERE " + filter
			} else if table == "files" {
				q += " WHERE folder IN (SELECT id FROM folders WHERE " + filter + ")"
				if scope.Paths != nil {
					if len(scope.Paths) == 0 {
						q += " AND FALSE"
					} else {
						q += " AND path IN (" + strings.TrimSuffix(strings.Repeat("?,", len(scope.Paths)), ",") + ")"
						for _, p := range scope.Paths {
							args = append(args, p)
						}
					}
				}
			} else {
				q += ` WHERE folder IN (SELECT id FROM folders WHERE ` + filter + ")"
			}
		}
		if scoped && !scope.GC && (table == "tickets" || table == "garbage") {
			ids := scope.Tickets
			if table == "garbage" {
				ids = scope.Garbage
			}
			if table == "tickets" && scope.TicketPaths != nil && ids == nil {
				m.transfersPartial = true
				if len(scope.TicketPaths) == 0 {
					q += " AND FALSE"
				} else if !ticketPathsInGo {
					q += " AND json_extract(data,'$.PathID') IN (" + strings.TrimSuffix(strings.Repeat("?,", len(scope.TicketPaths)), ",") + ")"
					for _, pid := range scope.TicketPaths {
						args = append(args, pid)
					}
				}
			}
			if ids != nil {
				m.transfersPartial = true
				if len(ids) == 0 {
					q += " AND FALSE"
				} else {
					q += " AND id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")"
					for _, id := range ids {
						args = append(args, id)
					}
				}
			}
		}
		rs, e := tx.QueryContext(ctx, q, args...)
		if e != nil {
			return nil, e
		}
		for rs.Next() {
			var id string
			var data []byte
			if e = rs.Scan(&id, &data); e != nil {
				rs.Close()
				return nil, e
			}
			if ticketPathsInGo && table == "tickets" {
				if scope, ok := ScopeFromContext(ctx); ok && scope.TicketPaths != nil {
					var probe Ticket
					if json.Unmarshal(data, &probe) == nil && !ticketPathSelected(scope.TicketPaths, probe.PathID) {
						continue
					}
				}
			}
			old[table][id] = data
			switch table {
			case "grants":
				var role Role
				e = json.Unmarshal(data, &role)
				f := m.Folders[id[:32]]
				if id[33:] != principalKey(f.Folder.Owner) {
					f.Grants[id[33:]] = role
				}
				m.Folders[id[:32]] = f
			case "garbage":
				var v Garbage
				if e = json.Unmarshal(data, &v); e == nil {
					m.Garbage[id] = v
				}
			case "folders":
				var v FolderRecord
				if e = json.Unmarshal(data, &v); e == nil {
					v.Grants = map[string]Role{}
					m.Folders[id] = v
					if m.Files[id] == nil {
						m.Files[id] = map[string]Row{}
					}
				}
			case "tickets":
				var v Ticket
				if e = json.Unmarshal(data, &v); e == nil {
					m.Tickets[id] = v
				}
			case "files":
				var v Row
				if e = json.Unmarshal(data, &v); e == nil {
					if m.Files[v.FolderID] == nil {
						m.Files[v.FolderID] = map[string]Row{}
					}
					m.Files[v.FolderID][v.PathID] = v
				}
			}
			if e != nil {
				rs.Close()
				return nil, e
			}
		}
		e = rs.Err()
		rs.Close()
		if e != nil {
			return nil, e
		}
		if table == "grants" {
			if scope, ok := ScopeFromContext(ctx); ok && scope.Folder != "" && scope.Principal.valid() {
				if _, e = access(m, scope.Principal, scope.Folder, false, false); e != nil {
					return nil, e
				}
			}
		}

	}
	if scoped && scope.ReadOnly {
		m.Prepare(m.filesLoaded)
		m.ctx = ctx
		return nil, fn(m)
	}
	keys := map[string]bool{}
	if scope, ok := ctx.Value(scopeKey{}).(Scope); ok && scope.Folder == "" && scope.Principal.valid() {
		keys[principalKey(scope.Principal)] = true
	}
	for _, f := range m.Folders {
		keys[principalKey(f.Folder.Owner)] = true
	}
	for _, g := range m.Garbage {
		keys[principalKey(g.Owner)] = true
	}
	if scoped && scope.GC {
		rows, qe := tx.QueryContext(ctx, "SELECT id,data FROM accounts")
		if qe != nil {
			return nil, qe
		}
		for rows.Next() {
			var key string
			var data []byte
			var a Account
			if qe = rows.Scan(&key, &data); qe == nil {
				qe = json.Unmarshal(data, &a)
			}
			if qe != nil {
				rows.Close()
				return nil, qe
			}
			m.Accounts[key] = a
		}
		qe = rows.Err()
		rows.Close()
		if qe != nil {
			return nil, qe
		}
	} else {
		for key := range keys {
			var data []byte
			e = tx.QueryRowContext(ctx, "SELECT data FROM accounts WHERE id=?", key).Scan(&data)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return nil, e
			}
			var a Account
			if len(data) > 0 {
				if e = json.Unmarshal(data, &a); e != nil {
					return nil, e
				}
			}
			m.Accounts[key] = a
		}
	}
	beforeAccounts := make(map[string]Account, len(m.Accounts))
	for k, a := range m.Accounts {
		beforeAccounts[k] = a
	}
	m.Prepare(m.filesLoaded)
	if e = fn(m); e != nil {
		return nil, e
	}
	m.Finish()
	accounts := []any{}
	for key, a := range m.Accounts {
		if a == beforeAccounts[key] {
			continue
		}
		b, err := json.Marshal(a)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, key, b)
	}
	if e = writeValues(ctx, tx, "INSERT INTO accounts(id,data) VALUES ", " ON CONFLICT(id) DO UPDATE SET data=excluded.data", 2, accounts); e != nil {
		return nil, e
	}

	next := map[string]map[string][]byte{"folders": {}, "grants": {}, "files": {}, "tickets": {}, "garbage": {}}
	for id, v := range m.Folders {
		b, e := json.Marshal(v)
		if e != nil {
			return nil, e
		}
		next["folders"][id] = b
		if !v.Deleted {
			next["grants"][id+"/"+principalKey(v.Folder.Owner)], _ = json.Marshal(Owner)
			for key, role := range v.Grants {
				next["grants"][id+"/"+key], _ = json.Marshal(role)
			}
		}
	}
	for f, rows := range m.Files {
		for p, v := range rows {
			b, e := json.Marshal(v)
			if e != nil {
				return nil, e
			}
			next["files"][f+"/"+p] = b
		}
	}
	for id, v := range m.Garbage {
		b, e := json.Marshal(v)
		if e != nil {
			return nil, e
		}
		next["garbage"][id] = b
	}
	for id, v := range m.Tickets {
		b, e := json.Marshal(v)
		if e != nil {
			return nil, e
		}
		next["tickets"][id] = b
	}
	for _, table := range []string{"folders", "grants", "files", "tickets", "garbage"} {
		prefix := "INSERT INTO " + table + "(id,folder,data) VALUES "
		suffix := " ON CONFLICT(id) DO UPDATE SET data=excluded.data,folder=excluded.folder"
		if table == "files" {
			prefix = "INSERT INTO files(folder,path,data) VALUES "
			suffix = " ON CONFLICT(folder,path) DO UPDATE SET data=excluded.data"
		}
		if table == "grants" {
			prefix = "INSERT INTO grants(folder,principal,data) VALUES "
			suffix = " ON CONFLICT(folder,principal) DO UPDATE SET data=excluded.data"
		}
		if table == "folders" {
			prefix = "INSERT INTO folders(id,owner,name,live,data) VALUES "
			suffix = " ON CONFLICT(id) DO UPDATE SET data=excluded.data,owner=excluded.owner,name=excluded.name,live=excluded.live"
		}
		width := 3
		if table == "folders" {
			width = 5
		}
		if table == "garbage" {
			prefix = "INSERT INTO garbage(id,folder,size,writing,data) VALUES "
			suffix = " ON CONFLICT(id) DO UPDATE SET data=excluded.data,folder=excluded.folder,size=excluded.size,writing=excluded.writing"
			width = 5
		}
		updated, created := []any{}, []any{}
		for id, b := range next[table] {
			if bytes.Equal(b, old[table][id]) {
				continue
			}
			var args []any
			switch table {
			case "files", "grants":
				args = []any{id[:32], id[33:], b}
			case "folders":
				live := 1
				if m.Folders[id].Deleted {
					live = 0
				}
				args = []any{id, principalKey(m.Folders[id].Folder.Owner), m.Folders[id].Folder.Name, live, b}
			case "tickets":
				args = []any{id, m.Tickets[id].FolderID, b}
			case "garbage":
				writing := 0
				if m.Garbage[id].Writing {
					writing = 1
				}
				args = []any{id, m.Garbage[id].FolderID, m.Garbage[id].Size, writing, b}
			}
			if table == "folders" && old[table][id] == nil {
				created = append(created, args...)
			} else {
				updated = append(updated, args...)
			}
		}
		if e = writeValues(ctx, tx, prefix, suffix, width, updated); e != nil {
			return nil, e
		}
		if e = writeValues(ctx, tx, prefix, "", width, created); e != nil {
			if isUniqueViolation(e) {
				return nil, ErrConflict
			}
			return nil, e
		}
		if table == "files" || table == "grants" {
			var remove *sql.Stmt
			for id := range old[table] {
				if _, ok := next[table][id]; ok {
					continue
				}
				q := "DELETE FROM files WHERE folder=? AND path=?"
				args := []any{id[:32], id[33:]}
				if table == "grants" {
					q = "DELETE FROM grants WHERE folder=? AND principal=?"
				}
				if remove == nil {
					remove, e = tx.PrepareContext(ctx, q)
					if e != nil {
						return nil, e
					}
				}
				if _, e = remove.ExecContext(ctx, args...); e != nil {
					remove.Close()
					return nil, e
				}
			}
			if remove != nil {
				remove.Close()
			}
			continue
		}
		gone := make([]any, 0)
		for id := range old[table] {
			if _, ok := next[table][id]; !ok {
				gone = append(gone, id)
			}
		}
		if e = execIn(ctx, tx, "DELETE FROM "+table+" WHERE id IN (", gone); e != nil {
			return nil, e
		}
	}
	changed := map[string]bool{}
	for _, table := range []string{"folders", "grants"} {
		for id, b := range next[table] {
			if !bytes.Equal(b, old[table][id]) {
				if table == "grants" {
					id = id[:32]
				}
				changed[id] = true
			}
		}
		for id := range old[table] {
			if _, ok := next[table][id]; !ok {
				if table == "grants" {
					id = id[:32]
				}
				changed[id] = true
			}
		}
	}
	return changed, nil
}

func ticketPathSelected(paths []string, id string) bool {
	for _, p := range paths {
		if p == id {
			return true
		}
	}
	return false
}
