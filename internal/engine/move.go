package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// relocateMovedRoot accepts a root that was renamed on the same volume.
// The state marker and the binding record the absolute path, so a plain
// re-attach refuses the new path even when the marker token and directory
// inode still match. Rewriting those two records lets Attach reuse the index
// instead of uploading the tree again. A copy, a replaced marker, or a missing
// index is refused.
func relocateMovedRoot(dir, folder, state string) error {
	if state == "" {
		return fmt.Errorf("moved attach needs its existing state directory: %w", ErrInvalid)
	}
	state, err := filepath.Abs(state)
	if err != nil {
		return err
	}
	state, err = filepath.EvalSymlinks(state)
	if err != nil {
		return fmt.Errorf("moved attach needs its existing state directory: %w", ErrInvalid)
	}
	lock, err := lockState(filepath.Join(state, "lock"))
	if err != nil {
		return fmt.Errorf("moved attach could not lock its state: %w", err)
	}
	defer lock.Close()
	markerPath := filepath.Join(state, stateMarker)
	previous, err := os.ReadFile(markerPath)
	if err != nil {
		return fmt.Errorf("moved attach has no state marker: %w", ErrInvalid)
	}
	prefix := folder + "/"
	if !strings.HasPrefix(string(previous), prefix) {
		return fmt.Errorf("state directory is not dedicated to this replica: %w", ErrInvalid)
	}
	oldDir := string(previous[len(prefix):])
	if !filepath.IsAbs(oldDir) {
		return fmt.Errorf("state directory is not dedicated to this replica: %w", ErrInvalid)
	}
	indexPath := filepath.Join(state, "index.sqlite")
	if _, err = os.Lstat(indexPath); err != nil {
		return fmt.Errorf("moved attach has no replica index: %w", ErrInvalid)
	}
	db, err := sql.Open("sqlite", indexPath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		return err
	}
	var saved string
	err = db.QueryRow("SELECT value FROM config WHERE key='root'").Scan(&saved)
	if err != nil {
		return fmt.Errorf("moved root has no saved identity: %w", ErrInvalid)
	}
	var savedID rootIdentity
	if json.Unmarshal([]byte(saved), &savedID) != nil || savedID.Token == "" || savedID.Inode == 0 || savedID.Device == 0 || savedID.Folder != folder {
		return fmt.Errorf("moved root has no saved identity: %w", ErrInvalid)
	}
	live, err := identifyRoot(dir)
	if err != nil || live.Folder != folder || live.Token != savedID.Token || live.Inode == 0 || live.Inode != savedID.Inode || live.Device == 0 || live.Device != savedID.Device {
		return fmt.Errorf("root is neither the attached directory nor the same directory renamed: %w", ErrInvalid)
	}
	var binding string
	err = db.QueryRow("SELECT value FROM config WHERE key='binding'").Scan(&binding)
	if err != nil || !strings.HasPrefix(binding, prefix) || !filepath.IsAbs(binding[len(prefix):]) {
		return fmt.Errorf("state binding does not match the previous path: %w", ErrInvalid)
	}
	want := prefix + dir
	if binding == want && oldDir == dir {
		return nil
	}
	if oldDir != dir && binding != prefix+oldDir {
		return fmt.Errorf("state binding does not match the previous path: %w", ErrInvalid)
	}
	if oldDir != dir {
		next := []byte(want)
		if err = rewriteMarker(markerPath, next); err != nil {
			return err
		}
	}
	res, err := db.Exec("UPDATE config SET value=? WHERE key='binding' AND value=?", want, binding)
	if err != nil {
		if oldDir != dir {
			_ = rewriteMarker(markerPath, previous)
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if oldDir != dir {
			_ = rewriteMarker(markerPath, previous)
		}
		return fmt.Errorf("could not record the moved path: %w", ErrInvalid)
	}
	return nil
}

func rewriteMarker(name string, b []byte) error {
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err = finishMarkerWrite(f); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, name); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	d, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer d.Close()
	return syncDirectoryFile(d)
}
