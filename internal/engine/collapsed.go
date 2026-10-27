package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

// planPlaceholders decides expansion using the complete authenticated path list,
// before publication order or chunk boundaries can favor a deep subtree.
func (r *Replica) planPlaceholders(metadata map[string]FileMetadata) {
	r.remotePlan = map[string]bool{}
	if !r.opts.OnDemand {
		return
	}
	if r.collapsed == nil {
		r.collapsed = map[string]bool{}
	}
	entries := map[string]bool{}
	for p, i := range r.index {
		if !i.Deleted {
			entries[p] = i.Directory
		}
	}
	for _, m := range metadata {
		if !r.ignore(m.Path, m.Directory, r.patterns()) {
			entries[m.Path] = m.Directory
		}
	}
	children := map[string][]string{}
	for p := range entries {
		for q := path.Dir(p); q != "."; q = path.Dir(q) {
			entries[q] = true
		}
	}
	for p := range entries {
		children[path.Dir(p)] = append(children[path.Dir(p)], p)
	}
	for p := range children {
		sort.Strings(children[p])
	}
	if r.version == 0 {
		// Replaying a partially committed initial pull makes the same decisions.
		r.collapsed = map[string]bool{}
		count := 0
		queue := []string{"."}
		for len(queue) > 0 {
			parent := queue[0]
			queue = queue[1:]
			list := children[parent]
			if len(list) > r.opts.MaxFolderPlaceholders || count+len(list) > r.opts.MaxPlaceholders {
				r.collapsed[parent] = true
				continue
			}
			count += len(list)
			for _, p := range list {
				if entries[p] {
					queue = append(queue, p)
				}
			}
		}
	}
	materialized := 0
	direct := map[string]int{}
	for p, i := range r.index {
		if !i.Deleted && !i.RemoteOnly {
			materialized++
			direct[path.Dir(p)]++
		}
	}
	paths := make([]string, 0, len(metadata))
	for _, m := range metadata {
		paths = append(paths, m.Path)
	}
	sort.Slice(paths, func(a, b int) bool {
		da, db := strings.Count(paths[a], "/"), strings.Count(paths[b], "/")
		if da != db {
			return da < db
		}
		return paths[a] < paths[b]
	})
	for _, p := range paths {
		i, exists := r.index[p]
		if exists && !i.Deleted && !i.RemoteOnly {
			continue
		}
		remote := r.underCollapsed(p)
		if r.version != 0 && !remote && (!exists || i.Deleted) {
			remote = materialized >= r.opts.MaxPlaceholders || direct[path.Dir(p)] >= r.opts.MaxFolderPlaceholders
		}
		if exists && i.RemoteOnly {
			remote = true
		}
		r.remotePlan[p] = remote
		if !remote {
			materialized++
			direct[path.Dir(p)]++
		}
	}
	r.guidesDirty = true
}

func (r *Replica) underCollapsed(p string) bool {
	for parent := path.Dir(p); ; parent = path.Dir(parent) {
		i, known := r.index[parent]
		if r.collapsed[parent] || r.remotePlan[parent] || (known && !i.Deleted && i.RemoteOnly) {
			return true
		}
		if parent == "." {
			return false
		}
	}
}

// No filesystem calls are needed for a path we deliberately do not materialize.
// The same spelling reservations as localPath still protect case collisions.
func (r *Replica) remoteLocal(p, pid string) string {
	if i, ok := r.index[p]; ok && !i.Deleted {
		return i.Local
	}
	parts := strings.Split(p, "/")
	local := ""
	for n, part := range parts {
		remote := strings.Join(parts[:n+1], "/")
		if i, ok := r.index[remote]; ok && !i.Deleted && i.Directory {
			local = i.Local
			continue
		}
		want := path.Join(local, part)
		local = want
		for serial := 0; ; serial++ {
			other, ok := r.byFold[foldPath(local)]
			if !ok || other == remote || r.index[other].Deleted {
				break
			}
			suffix := " (case conflict " + pid[:32] + ")"
			if serial > 0 {
				suffix = fmt.Sprintf(" (case conflict %s-%d)", pid[:32], serial)
			}
			local = variantPath(want, suffix)
		}
	}
	return local
}

func (r *Replica) saveRemoteOnly(row Row, m FileMetadata) error {
	if m.Size < 0 || m.Size > MaxRequestBytes || SealedSize(m.Size) != row.SealedSize || (m.Directory && (m.Size != 0 || m.Hash != "directory")) {
		return ErrIntegrity
	}
	hash, he := parseHash(m.Hash)
	if m.Directory && m.Hash == "directory" {
		he = nil
	}
	blob, be := parseBlob(row.BlobID)
	if he != nil || be != nil || row.BlobID == "" || (!m.Directory && m.Hash == "") {
		return ErrIntegrity
	}
	local := r.remoteLocal(m.Path, row.PathID)
	if _, e := NormalizePath(local); e != nil {
		return e
	}
	if r.ignore(m.Path, m.Directory, r.patterns()) || r.ignore(local, m.Directory, r.patterns()) {
		r.ignoredRows[row.PathID] = row
		return nil
	}
	r.guidesDirty = true
	return r.save(IndexEntry{Path: m.Path, Local: local, Hash: hash, Blob: blob, Version: row.Version, Directory: m.Directory, Mode: m.Mode, Size: m.Size, Placeholder: true, RemoteOnly: true})
}

func (r *Replica) storeCollapsed() error {
	data, e := json.Marshal(r.collapsed)
	if e != nil {
		return e
	}
	_, e = r.db.Exec("INSERT INTO config(key,value) VALUES('collapsed',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(data))
	return e
}

// The first existing ancestor is the physical anchor of an absent remote-only
// path. Its disappearance requests deletion of that subtree, rather than every
// intentionally absent child being interpreted as a local removal.
func (r *Replica) remoteOnlyAnchor(i IndexEntry) string {
	for parent := path.Dir(i.Path); parent != "."; parent = path.Dir(parent) {
		if r.collapsed[parent] {
			if a, ok := r.index[parent]; ok {
				return a.Local
			}
			return parent
		}
		if a, ok := r.index[parent]; ok && !a.Deleted && !a.RemoteOnly {
			return a.Local
		}
	}
	return "."
}

func (r *Replica) includeRemoteOnly(local map[string]LocalFile) {
	present := map[string]bool{".": true}
	checked := map[string]bool{".": true}
	for p, i := range r.index {
		if i.Deleted || !i.RemoteOnly {
			continue
		}
		anchor := r.remoteOnlyAnchor(i)
		if !checked[anchor] {
			info, e := r.root.Lstat(anchor)
			// Only a definite absence can request deletion. Scan blocks obstacles.
			present[anchor] = !errors.Is(e, os.ErrNotExist) && (e != nil || info.IsDir())
			checked[anchor] = true
		}
		if present[anchor] {
			local[p] = LocalFile{Path: p, Local: i.Local, Hash: i.hashString(), Size: i.Size, Mode: i.Mode, Directory: i.Directory}
		}
	}
}

func (r *Replica) remoteOnlyContainerPresent(i IndexEntry, cached map[string]bool) bool {
	anchor := r.remoteOnlyAnchor(i)
	if present, ok := cached[anchor]; ok {
		return present
	}
	_, e := r.root.Lstat(anchor)
	present := !errors.Is(e, os.ErrNotExist)
	cached[anchor] = present
	return present
}

// Preserve an unseen local write before scans can use the remote version as its
// base. This is also used on a mode switch, before all remote-only files hydrate.
func (r *Replica) prepareRemoteOnly(ctx context.Context) error {
	hasRemote := false
	for _, i := range r.index {
		if i.RemoteOnly && !i.Deleted {
			hasRemote = true
			break
		}
	}
	if !hasRemote {
		return nil
	}
	// A walk visits only materialized paths, avoiding one Lstat per remote row.
	var first error
	var walk func(string) error
	walk = func(parent string) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if e := r.safe(path.Join(parent, "x")); e != nil {
			return e
		}
		f, e := r.root.Open(parent)
		if e != nil {
			return e
		}
		list, e := f.ReadDir(-1)
		f.Close()
		if e != nil {
			return e
		}
		for _, d := range list {
			local := path.Join(parent, d.Name())
			if r.ignore(local, d.IsDir(), r.patterns()) {
				continue
			}
			p := r.remotePath(local)
			if i, ok := r.index[p]; ok && i.RemoteOnly && !i.Deleted {
				if d.Type()&os.ModeSymlink != 0 {
					r.blockLocal(local)
					continue
				}
				if d.IsDir() && i.Directory {
					if e = r.safe(path.Join(local, "x")); e != nil {
						r.blockLocal(local)
						if first == nil {
							first = e
						}
						continue
					}
					if e = r.syncParent(local); e != nil {
						return e
					}
					i.RemoteOnly, i.Placeholder = false, false
					if e = r.save(i); e != nil {
						return e
					}
				} else if d.Type().IsRegular() || d.IsDir() {
					if _, e = r.preserve(local); e != nil {
						return e
					}
					if r.opts.OnDemand {
						if i.Directory {
							if e = r.materializeDirectory(i); e != nil {
								return e
							}
							continue
						}
						if e = r.writePlaceholder(r.contentRow(i), i.metadata(), local, i); e != nil {
							return e
						}
					}
					continue
				}
			}
			if d.IsDir() {
				if e = walk(local); e != nil && first == nil {
					first = e
				}
			}
		}
		return nil
	}
	if e := walk("."); e != nil {
		return e
	}
	return first
}

func (r *Replica) materializeParents(local string) error {
	if e := r.safe(local); e != nil {
		return e
	}
	if e := durableMkdirAll(r.root, path.Dir(local)); e != nil {
		return e
	}
	for parent := path.Dir(local); parent != "."; parent = path.Dir(parent) {
		p, ok := r.byLocal[parent]
		if !ok {
			continue
		}
		i := r.index[p]
		if i.Deleted || !i.Directory || !i.RemoteOnly {
			continue
		}
		i.RemoteOnly, i.Placeholder = false, false
		if e := r.save(i); e != nil {
			return e
		}
	}
	r.guidesDirty = true
	return nil
}

func (r *Replica) materializeDirectory(i IndexEntry) error {
	if !i.Directory || i.Size != 0 {
		return ErrIntegrity
	}
	if e := r.materializeParents(path.Join(i.Local, "x")); e != nil {
		return e
	}
	if e := r.chmod(i.Local, i.Mode, true); e != nil {
		return e
	}
	i.RemoteOnly, i.Placeholder = false, false
	return r.save(i)
}

func (r *Replica) fetchTargetUnchanged(i IndexEntry, local string) bool {
	if !i.RemoteOnly {
		return r.placeholderAt(i, local)
	}
	_, e := r.root.Lstat(local)
	return errors.Is(e, os.ErrNotExist)
}

// A collapsed directory still has an inode. When it moves, hydrate its unseen
// descendants at the new location before ordinary same-hash rename detection.
// Missing contents can then never become delete-only changes for that move.
func (r *Replica) prepareCollapsedMoves(local map[string]LocalFile) {
	sources := map[placeholderID]IndexEntry{}
	for p := range r.collapsed {
		i, ok := r.index[p]
		if !ok || i.Deleted || i.RemoteOnly || i.Inode == 0 {
			continue
		}
		if _, e := r.root.Lstat(i.Local); errors.Is(e, os.ErrNotExist) {
			sources[placeholderID{i.Dev, i.Inode}] = i
		}
	}
	if len(sources) == 0 {
		return
	}
	for p, v := range local {
		if !v.Directory {
			continue
		}
		if i, ok := r.index[p]; ok && !i.Deleted {
			continue
		}
		info, e := r.root.Lstat(v.Local)
		if e != nil || !info.IsDir() {
			continue
		}
		old, ok := sources[placeholderID{device(info), inodeOf(info)}]
		if !ok {
			continue
		}
		for child, i := range r.index {
			if i.Deleted || !i.RemoteOnly || !strings.HasPrefix(child, old.Path+"/") {
				continue
			}
			next := p + strings.TrimPrefix(child, old.Path)
			nextLocal := v.Local + strings.TrimPrefix(i.Local, old.Local)
			if r.ignore(nextLocal, i.Directory, r.patterns()) {
				r.blockLocal(old.Local)
				continue
			}
			if found, exists := local[next]; exists && sameFile(found, i) {
				continue
			}
			if i.Directory {
				if e = r.safe(nextLocal); e == nil {
					e = durableMkdirAll(r.root, nextLocal)
				}
				if e != nil {
					r.blockLocal(old.Local)
					r.addError(e)
					continue
				}
				local[next] = LocalFile{Path: next, Local: nextLocal, Hash: "directory", Directory: true, Mode: i.Mode}
				continue
			}
			local[next] = LocalFile{Path: next, Local: nextLocal, Hash: i.hashString(), Size: i.Size, Mode: i.Mode}
			r.placeholderMoved[next] = i
		}
	}
}
