package engine

// Account is a durable quota counter, updated with the selected folder delta.
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
func (m *Metadata) fileTotals(id string) Usage {
	next := fileUsage(m.Files[id])
	if m.filesLoaded {
		return next
	}
	old := m.selected[id]
	u := m.Folders[id].FileUsage
	u.Bytes = sat(max(0, u.Bytes-old.Bytes), next.Bytes)
	u.Files = sat(max(0, u.Files-old.Files), next.Files)
	u.Rows = sat(max(0, u.Rows-old.Rows), next.Rows)
	return u
}

// allUsage groups reservations and garbage once, rather than rescanning them
// for every folder. Runtime is linear in the selected records.
func allUsage(m *Metadata) map[string]Usage { return usageWithTransfers(m, transferUsage(m)) }
func usageWithTransfers(m *Metadata, transfers map[string]Usage) map[string]Usage {
	out := make(map[string]Usage, len(m.Folders))
	for id := range m.Folders {
		out[id] = m.fileTotals(id)
	}
	for id, f := range m.Folders {
		extra := transfers[id]
		if m.transfersPartial {
			extra = transferDelta(f.TransferUsage, m.selectedTransfers[id], extra)
		}
		u := out[id]
		u.Bytes = sat(u.Bytes, extra.Bytes)
		u.Reserved, u.ReservedFiles, u.ReservedRows, u.GarbageRows = extra.Reserved, extra.ReservedFiles, extra.ReservedRows, extra.GarbageRows
		out[id] = u
	}

	return out
}
func contributions(m *Metadata) map[string]Account { return contributionTotals(m, allUsage(m)) }
func contributionTotals(m *Metadata, totals map[string]Usage) map[string]Account {
	out := map[string]Account{}
	for id, f := range m.Folders {
		key := principalKey(f.Folder.Owner)
		a := out[key]
		u := totals[id]
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
func accountDelta(a, old, next Account) Account {
	return Account{sat(max(0, a.Bytes-old.Bytes), next.Bytes), sat(max(0, a.Rows-old.Rows), next.Rows), sat(max(0, a.Folders-old.Folders), next.Folders)}
}
func accountUsage(m *Metadata, key string) Account {
	return accountDelta(m.Accounts[key], m.baseline[key], contributions(m)[key])
}
func ownerUsage(m *Metadata, p Principal) (int64, int64) {
	a := accountUsage(m, principalKey(p))
	return a.Bytes, a.Rows
}

// Prepare captures contributions before mutation. With partial/no file loading,
// FileUsage must contain the full cache; Files holds just the selected rows.
func (m *Metadata) Prepare(filesLoaded bool) {
	m.filesLoaded = filesLoaded
	m.selected = map[string]Usage{}
	if !filesLoaded {
		for id, rows := range m.Files {
			m.selected[id] = fileUsage(rows)
		}
	}
	m.selectedTransfers = transferUsage(m)
	m.baseline = contributionTotals(m, usageWithTransfers(m, m.selectedTransfers))
}

// Finish computes contributions once and atomically updates caches/counters.
func (m *Metadata) Finish() {
	extra := transferUsage(m)
	next := contributionTotals(m, usageWithTransfers(m, extra))
	if m.Accounts == nil {
		m.Accounts = map[string]Account{}
	}
	for key, old := range m.baseline {
		m.Accounts[key] = accountDelta(m.Accounts[key], old, next[key])
		delete(next, key)
	}
	for key, a := range next {
		m.Accounts[key] = accountDelta(m.Accounts[key], Account{}, a)
	}
	for id, f := range m.Folders {
		if m.transfersPartial {
			f.TransferUsage = transferDelta(f.TransferUsage, m.selectedTransfers[id], extra[id])
		} else {
			f.TransferUsage = extra[id]
		}
		f.FileUsage = m.fileTotals(id)
		m.Folders[id] = f
	}
}

func transferUsage(m *Metadata) map[string]Usage {
	out := map[string]Usage{}
	for _, t := range m.Tickets {
		u := out[t.FolderID]
		u.Reserved = sat(u.Reserved, t.ReservedBytes)
		u.ReservedFiles = sat(u.ReservedFiles, t.ReservedFiles)
		u.ReservedRows = sat(u.ReservedRows, t.ReservedRows)
		out[t.FolderID] = u
	}
	for _, g := range m.Garbage {
		u := out[g.FolderID]
		u.GarbageRows++
		u.Bytes = sat(u.Bytes, g.Size)
		out[g.FolderID] = u
	}
	return out
}
func transferDelta(total, old, next Usage) Usage {
	return Usage{Bytes: sat(max(0, total.Bytes-old.Bytes), next.Bytes), Reserved: sat(max(0, total.Reserved-old.Reserved), next.Reserved), ReservedFiles: sat(max(0, total.ReservedFiles-old.ReservedFiles), next.ReservedFiles), ReservedRows: sat(max(0, total.ReservedRows-old.ReservedRows), next.ReservedRows), GarbageRows: sat(max(0, total.GarbageRows-old.GarbageRows), next.GarbageRows)}
}
