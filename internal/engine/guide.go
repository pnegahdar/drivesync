package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

type guideTotal struct{ files, bytes int64 }

// Remove guides from an earlier configuration before an unignored remote row
// can adopt their path. The directory barrier precedes forgetting ownership.
func (r *Replica) prepareGuides() error {
	var saved string
	_ = r.db.QueryRow("SELECT value FROM config WHERE key='guides'").Scan(&saved)
	if saved == "" {
		return nil
	}
	var old, keep []string
	if json.Unmarshal([]byte(saved), &old) != nil {
		return ErrInvalid
	}
	enabled := r.opts.OnDemand && r.opts.GuideName != "" && r.opts.Guide != ""
	changed := false
	for _, local := range old {
		if enabled && foldPath(path.Base(local)) == foldPath(r.opts.GuideName) {
			keep = append(keep, local)
			continue
		}
		if e := r.safe(local); e != nil {
			return e
		}
		if e := r.root.Remove(local); e == nil {
			if e = r.syncParent(local); e != nil {
				return e
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		changed = true
	}
	if !changed {
		return nil
	}
	data, _ := json.Marshal(keep)
	_, e := r.db.Exec("INSERT INTO config(key,value) VALUES('guides',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(data))
	if e == nil {
		r.guidesDirty = true
	}
	return e
}

// Guides are derived from the index, never the input to it. Publishing them
// before the final index barrier is safe: replay repairs any interrupted guide.
func (r *Replica) refreshGuides() error {
	if !r.guidesDirty {
		return nil
	}
	remaining := map[string]bool{}
	for p, i := range r.index {
		if i.Deleted || !i.RemoteOnly {
			continue
		}
		for parent := path.Dir(p); ; parent = path.Dir(parent) {
			remaining[parent] = true
			if parent == "." {
				break
			}
		}
	}
	changed := false
	for parent := range r.collapsed {
		if !remaining[parent] {
			delete(r.collapsed, parent)
			changed = true
		}
	}
	if changed && r.pullIndex == nil {
		if e := r.storeCollapsed(); e != nil {
			return e
		}
	}
	var old []string
	var saved string
	_ = r.db.QueryRow("SELECT value FROM config WHERE key='guides'").Scan(&saved)
	if saved != "" && json.Unmarshal([]byte(saved), &old) != nil {
		return ErrInvalid
	}
	want := map[string]string{}
	if r.opts.OnDemand && r.opts.GuideName != "" && r.opts.Guide != "" {
		want[path.Join(".", r.opts.GuideName)] = "."
		for p := range r.collapsed {
			if p == "." {
				continue
			}
			local := p
			if i, ok := r.index[p]; ok {
				if i.Deleted || i.RemoteOnly {
					continue
				}
				local = i.Local
			}
			info, e := r.root.Lstat(local)
			if errors.Is(e, os.ErrNotExist) {
				continue
			}
			if e != nil {
				return e
			}
			if !info.IsDir() {
				continue
			}
			want[path.Join(local, r.opts.GuideName)] = p
		}
	}
	for _, local := range old {
		if _, keep := want[local]; keep {
			continue
		}
		if _, e := NormalizePath(local); e != nil {
			return e
		}
		if e := r.safe(local); e != nil {
			return e
		}
		if e := r.root.Remove(local); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	// Aggregate each unfetched file into its ancestor summaries in one pass.
	totals := map[string]guideTotal{}
	children := map[string]map[string]bool{}
	for p, i := range r.index {
		if i.Deleted || !i.Placeholder || (i.Directory && !i.RemoteOnly) {
			continue
		}
		files := int64(1)
		if i.Directory {
			files = 0
		}
		for child := p; child != "."; child = path.Dir(child) {
			parent := path.Dir(child)
			if children[parent] == nil {
				children[parent] = map[string]bool{}
			}
			children[parent][child] = true
			t := totals[child]
			t.files += files
			t.bytes += i.Size
			totals[child] = t
		}
	}
	paths := make([]string, 0, len(want))
	for local, parent := range want {
		if e := r.safe(local); e != nil {
			return e
		}
		list := make([]string, 0, len(children[parent]))
		for child := range children[parent] {
			list = append(list, child)
		}
		sort.Strings(list)
		var b strings.Builder
		b.WriteString(r.opts.Guide)
		if !strings.HasSuffix(r.opts.Guide, "\n") {
			b.WriteByte('\n')
		}
		b.WriteByte('\n')
		for n, child := range list {
			if n == 1000 {
				fmt.Fprintf(&b, "… and %d more\n", len(list)-n)
				break
			}
			i, exists := r.index[child]
			t := totals[child]
			if !exists || i.Directory {
				fmt.Fprintf(&b, "%s/ (%s, %s)\n", path.Base(child), guideCount(t.files), guideSize(t.bytes))
			} else {
				fmt.Fprintf(&b, "%s (%s)\n", path.Base(child), guideSize(i.Size))
			}
		}
		temp := path.Join(path.Dir(local), ".drivesync-tmp-"+randomID())
		f, e := r.root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = f.WriteString(b.String())
		ce := f.Close()
		if e == nil {
			e = ce
		}
		if e == nil {
			e = r.safe(local)
		}
		if e == nil {
			e = r.root.Rename(temp, local)
		}
		// Guides are disposable derived data. They need atomic replacement,
		// while the independent index barriers protect all remote contents.
		// Attach regenerates them after a crash, including lost guide writes.
		_ = r.root.Remove(temp)
		if e != nil {
			return e
		}
		paths = append(paths, local)
	}
	sort.Strings(paths)
	data, _ := json.Marshal(paths)
	if string(data) == saved {
		r.guidesDirty = false
		return nil
	}
	_, e := r.db.Exec("INSERT INTO config(key,value) VALUES('guides',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", string(data))
	if e == nil {
		r.guidesDirty = false
	}
	return e
}

func guideCount(files int64) string {
	if files == 1 {
		return "1 file"
	}
	return fmt.Sprintf("%d files", files)
}

// guideSize is a decimal size a reader can scan, such as 48 kB or 1.2 GB.
func guideSize(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d B", n)
	}
	size := float64(n)
	for _, unit := range []string{"kB", "MB", "GB", "TB", "PB"} {
		size /= 1000
		if size < 1000 || unit == "PB" {
			return fmt.Sprintf("%.3g %s", size, unit)
		}
	}
	return ""
}
