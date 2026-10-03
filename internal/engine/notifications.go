package engine

import "sync"

// Waits hold entries only while active. A mutation wakes only its folder.
type folderWake struct {
	ch   chan struct{}
	refs int
}
type notifications struct {
	creationKey FolderKey
	publication sync.RWMutex
	running     bool
	active      map[string]bool
	mu          sync.Mutex
	folders     map[string]*folderWake
	principals  map[string]int
}

func newNotifications() *notifications {
	return &notifications{creationKey: NewFolderKey(), active: map[string]bool{}, folders: map[string]*folderWake{}, principals: map[string]int{}}
}

const MaxWaitsPerPrincipal = 256

func (n *notifications) acquire(p Principal, id string) (func(), error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := principalKey(p)
	if n.principals[key] >= MaxWaitsPerPrincipal {
		return nil, ErrWaitLimit
	}
	n.principals[key]++
	w := n.folders[id]
	if w == nil {
		w = &folderWake{ch: make(chan struct{})}
		n.folders[id] = w
	}
	w.refs++
	return func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.principals[key]--
		if n.principals[key] == 0 {
			delete(n.principals, key)
		}
		w.refs--
		if w.refs == 0 {
			delete(n.folders, id)
		}
	}, nil
}
func (n *notifications) channel(id string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.folders[id].ch
}
func (n *notifications) notify(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if w := n.folders[id]; w != nil {
		close(w.ch)
		w.ch = make(chan struct{})
	}
}

// Authorities opening the same SQLite database in this process share wakes.
// The embedder must route cross-process notifications through its transport;
// external stores can provide NotificationSource for that purpose.
type NotificationSource interface{ Notifications() *Notifications }

// Notifications is the process-local folder wake hub used by a metadata store.
type Notifications = notifications

// NewNotifications creates a hub shared by authorities using an external store.
func NewNotifications() *Notifications { return newNotifications() }

// Notify wakes subscribers after a durable folder/grant mutation. External
// stores must distribute these events between processes before acknowledging it.
func (n *notifications) Notify(id string) { n.notify(id) }

var sqliteHubs = struct {
	sync.Mutex
	hubs map[string]*hubRef
}{hubs: map[string]*hubRef{}}

type hubRef struct {
	n    *notifications
	refs int
}

func sqliteNotifications(name string) (*notifications, func()) {
	sqliteHubs.Lock()
	defer sqliteHubs.Unlock()
	if name == ":memory:" {
		return newNotifications(), func() {}
	}
	h := sqliteHubs.hubs[name]
	if h == nil {
		h = &hubRef{n: newNotifications()}
		sqliteHubs.hubs[name] = h
	}
	h.refs++
	return h.n, func() {
		sqliteHubs.Lock()
		defer sqliteHubs.Unlock()
		h.refs--
		if h.refs == 0 {
			delete(sqliteHubs.hubs, name)
		}
	}
}

func (n *notifications) live(key string) bool { n.mu.Lock(); defer n.mu.Unlock(); return n.active[key] }
