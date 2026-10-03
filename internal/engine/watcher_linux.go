//go:build linux

package engine

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

func watchDirectory(ctx context.Context, dir string, wake chan<- struct{}) (func(), string, error) {
	w, e := fsnotify.NewWatcher()
	if e != nil {
		return nil, "polling", e
	}
	add := func(p string) {
		rootInfo, se := os.Lstat(dir)
		if se != nil {
			return
		}
		patterns := watchPatterns(dir)
		_ = filepath.WalkDir(p, func(p string, d fs.DirEntry, e error) error {
			if e != nil || !d.IsDir() {
				return nil
			}
			info, ie := d.Info()
			if ie != nil {
				return nil
			}
			if device(info) != device(rootInfo) {
				return fs.SkipDir
			}
			rel, _ := filepath.Rel(dir, p)
			rel = filepath.ToSlash(rel)
			if rel != "." {
				if pathIgnored(rel, true, patterns) {
					return fs.SkipDir
				}
				if _, e := os.Lstat(filepath.Join(p, rootMarker)); e == nil {
					return fs.SkipDir
				}
				if _, e := os.Lstat(filepath.Join(p, stateMarker)); e == nil {
					return fs.SkipDir
				}
			}
			_ = w.Add(p)
			return nil
		})
	}
	add(dir)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-w.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create != 0 {
					add(event.Name)
				}
				select {
				case wake <- struct{}{}:
				default:
				}
			case _, ok := <-w.Errors:
				if !ok {
					return
				}
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}()
	return func() { _ = w.Close() }, "inotify", nil
}

func watchPatterns(dir string) []string {
	b, e := os.ReadFile(filepath.Join(dir, ".drivesyncignore"))
	if e != nil {
		return nil
	}
	return strings.Split(string(b), "\n")
}
