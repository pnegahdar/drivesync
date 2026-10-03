//go:build linux

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// T2 (Linux; written on macOS and only cross-compiled here - NOT executed).
// watchDirectory adds an inotify watch for every directory under the root,
// including ignored trees (node_modules, .git, build output), cross-device
// mounts and foreign attachments, and re-walks every created directory
// (watcher_linux.go:15-25,35-36). Watches count against the per-user
// fs.inotify.max_user_watches shared with IDEs and build tools.
func TestQA2InotifyWatchesIgnoredTrees(t *testing.T) {
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	dir := filepath.Join(t.TempDir(), "a")
	for i := 0; i < 300; i++ {
		qaWrite(t, filepath.Join(dir, "node_modules", fmt.Sprint("pkg", i), "index.js"), "x")
	}
	qaWrite(t, filepath.Join(dir, ".drivesyncignore"), "node_modules/\n")
	qaWrite(t, filepath.Join(dir, "src", "main.go"), "package main")
	r, e := Attach(qaCtx, c, f.ID, k, dir, Options{Name: "a", StateDir: filepath.Join(t.TempDir(), "state"), RescanInterval: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	time.Sleep(200 * time.Millisecond)
	watches := 0
	fds, _ := os.ReadDir("/proc/self/fd")
	for _, fd := range fds {
		if target, _ := os.Readlink("/proc/self/fd/" + fd.Name()); target == "anon_inode:inotify" {
			b, _ := os.ReadFile("/proc/self/fdinfo/" + fd.Name())
			watches += strings.Count(string(b), "inotify wd:")
		}
	}
	if watches > 10 {
		t.Fatalf("%d inotify watches for a root with 2 synced directories (300 ignored)", watches)
	}
}
