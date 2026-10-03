package qa

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ds "github.com/pnegahdar/drivesync/internal/engine"
)

// A station principal attaching more than 32 folders loses push notifications
// on the extra replicas: their Wait gets ErrBusy ("path has an active upload")
// every second, Status.Errors fills with it, and changes wait for a rescan.
func TestStationWithManyFoldersLosesPushAndReportsWrongError(t *testing.T) {
	s, _, _ := newServer(t)
	station := ds.Principal{Tenant: "t1", Subject: "station-7"}
	writer := s.Client(alice)
	var last *ds.Replica
	var lastDir string
	var lastFolder ds.Folder
	var lastKey ds.FolderKey
	for i := 0; i < 64; i++ {
		f, k := mkFolder(t, writer, ds.Limits{MaxTotalBytes: 1 << 20, MaxRows: 1000, MaxFileBytes: 1 << 16})
		if e := writer.Grant(bg, f.ID, station, ds.Reader); e != nil {
			t.Fatal(e)
		}
		dir := filepath.Join(t.TempDir(), fmt.Sprint(i))
		r, e := ds.Attach(bg, s.Client(station), f.ID, k, dir, ds.Options{Name: fmt.Sprint(i), RescanInterval: time.Hour, Debounce: 10 * time.Millisecond})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { r.Close() })
		last, lastDir, lastFolder, lastKey = r, dir, f, k
	}
	time.Sleep(1500 * time.Millisecond)
	put(t, writer, lastFolder, lastKey, "hello.txt", 0, []byte("hi"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(lastDir, "hello.txt")); e == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	errs := last.Status().Errors
	t.Fatalf("replica #%d never woke for a remote change; status errors (%d) e.g. %q", 64, len(errs), strings.Join(errs[:min(2, len(errs))], " | "))
}
