package confirmreview

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	ds "github.com/pnegahdar/drivesync"
)

type rep struct {
	*ds.Replica
	dir string
}

func attach(t testing.TB, c ds.Client, f ds.Folder, k ds.FolderKey, name string) rep {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "files")
	r, e := ds.Attach(bg, c, f.ID, k, dir, ds.Options{Name: name, StateDir: filepath.Join(base, "state"), Manual: true})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { r.Close() })
	d, _ := filepath.EvalSymlinks(dir)
	return rep{r, d}
}
func write(t testing.TB, r rep, p, content string) {
	full := filepath.Join(r.dir, filepath.FromSlash(p))
	os.MkdirAll(filepath.Dir(full), 0700)
	if e := os.WriteFile(full, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
}
func files(t testing.TB, r rep) map[string]string {
	out := map[string]string{}
	filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(r.dir, p)
		b, _ := os.ReadFile(p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out
}
func syncAll(t testing.TB, rs ...rep) {
	for i := 0; i < 2; i++ {
		for _, r := range rs {
			_ = r.Sync(bg)
		}
	}
}
