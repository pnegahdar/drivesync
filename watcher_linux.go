//go:build linux

package drivesync

import (
	"context"
	"github.com/fsnotify/fsnotify"
	"io/fs"
	"path/filepath"
)

func watchDirectory(ctx context.Context, dir string, wake chan<- struct{}) (func(), string, error) {
	w, e := fsnotify.NewWatcher()
	if e != nil {
		return nil, "polling", e
	}
	add := func(p string) {
		_ = filepath.WalkDir(p, func(p string, d fs.DirEntry, e error) error {
			if e == nil && d.IsDir() {
				_ = w.Add(p)
			}
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
