package drivesync

// Account is a durable quota counter. MetaStore implementations must load it
// alongside the selected folder and apply its contribution delta atomically.
type Account struct{ Bytes, Rows, Folders int64 }

func fileUsage(rows map[string]Row) (u Usage) {
	for _, r := range rows {
		u.Bytes = sat(u.Bytes, rowBytes(r))
		u.Rows++
		if !r.Deleted {
			u.Files++
		}
	}
	return
}
func contributions(m *Metadata) map[string]Account {
	out := map[string]Account{}
	for id, f := range m.Folders {
		key := principalKey(f.Folder.Owner)
		a := out[key]
		u := usage(m, id)
		if f.Allocated {
			a.Bytes = sat(a.Bytes, f.Folder.Limits.MaxTotalBytes)
			a.Rows = sat(a.Rows, allocatedRows(f.Folder.Limits))
		} else {
			a.Bytes = sat(a.Bytes, sat(u.Bytes, u.Reserved))
			a.Rows = sat(a.Rows, sat(sat(u.Rows, u.GarbageRows), u.ReservedRows))
		}
		if !f.Deleted {
			a.Folders++
		}
		out[key] = a
	}
	return out
}
func accountUsage(m *Metadata, key string) Account {
	a := m.Accounts[key]
	old := m.baseline[key]
	next := contributions(m)[key]
	return Account{sat(max(0, a.Bytes-old.Bytes), next.Bytes), sat(max(0, a.Rows-old.Rows), next.Rows), sat(max(0, a.Folders-old.Folders), next.Folders)}
}
func ownerUsage(m *Metadata, p Principal) (int64, int64) {
	a := accountUsage(m, principalKey(p))
	return a.Bytes, a.Rows
}

// Prepare captures the selected folders' contribution before mutation. A store
// which omits files must load each folder's FileUsage cache. Accounts use the
// canonical json.Marshal(Principal) string as their key (including escaped NUL).
func (m *Metadata) Prepare(filesLoaded bool) {
	m.filesLoaded = filesLoaded
	m.baseline = contributions(m)
}

// Finish updates scoped file caches and owner counters. Call only after the
// callback succeeds, then atomically persist the records and counters.
func (m *Metadata) Finish() {
	for id, f := range m.Folders {
		if m.filesLoaded {
			f.FileUsage = fileUsage(m.Files[id])
		}
		m.Folders[id] = f
	}
	keys := map[string]bool{}
	for key := range m.baseline {
		keys[key] = true
	}
	for key := range contributions(m) {
		keys[key] = true
	}
	next := map[string]Account{}
	for key := range keys {
		next[key] = accountUsage(m, key)
	}
	if m.Accounts == nil {
		m.Accounts = map[string]Account{}
	}
	for key, a := range next {
		m.Accounts[key] = a
	}
}
