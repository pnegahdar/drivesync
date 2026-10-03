package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"
)

func qaVolume(t *testing.T, fs, name string) string {
	t.Helper()
	if _, e := exec.LookPath("hdiutil"); e != nil {
		t.Skip("macOS hdiutil required")
	}
	img := filepath.Join(t.TempDir(), name+".dmg")
	if out, e := exec.Command("hdiutil", "create", "-size", "8m", "-fs", fs, "-volname", name, "-layout", "NONE", img).CombinedOutput(); e != nil {
		t.Skipf("hdiutil create %s: %v %s", fs, e, out)
	}
	at := filepath.Join(t.TempDir(), "mnt")
	qaMount(t, img, at)
	return at
}

// T2. exFAT/FAT (and SMB without unix extensions) report a fixed mode (0700 on
// macOS) and ignore chmod. Every download is immediately "modified" (mode
// differs from the index), re-uploaded in full, and every APFS/ext4 peer then
// chmods the file to the volume's synthetic mode.
func TestQAExFATRootRewritesPeerModes(t *testing.T) {
	vol := qaVolume(t, "ExFAT", "qaexfat")
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	p := qaAttach(t, c, f, k, filepath.Join(t.TempDir(), "p"), "p")
	writeLocal(t, p, "report.txt", "quarterly")
	os.Chmod(filepath.Join(p.dir, "report.txt"), 0644)
	qaSync(t, p)
	v1 := qaRemoteVersion(t, c, f, k, "report.txt")
	e := qaAttach(t, c, f, k, filepath.Join(vol, "shared"), "e")
	_ = e.Sync(qaCtx)
	_ = e.Sync(qaCtx)
	_ = p.Sync(qaCtx)
	info, _ := os.Stat(filepath.Join(p.dir, "report.txt"))
	if v2 := qaRemoteVersion(t, c, f, k, "report.txt"); v2 != v1 || info.Mode().Perm() != 0644 {
		t.Fatalf("exFAT replica re-uploaded an untouched download (version %d -> %d) and changed the peer's mode to %v", v1, v2, info.Mode().Perm())
	}
}

// Probe: HFS+ stores names decomposed (NFD). A peer's NFC name is renamed
// remotely to NFD once, then must stay stable with no duplicate or conflict.
func TestQAHFSPlusNormalizationConverges(t *testing.T) {
	vol := qaVolume(t, "HFS+", "qahfs")
	s, _ := testServer(t)
	c := s.Client(owner)
	f, k := folderFor(t, c, Limits{})
	nfc := norm.NFC.String("café/résumé.txt")
	qaPut(t, c, f, k, nfc, 0, []byte("cv"), 0644)
	h := qaAttach(t, c, f, k, filepath.Join(vol, "shared"), "h")
	for i := 0; i < 4; i++ {
		if e := h.Sync(qaCtx); e != nil {
			t.Logf("sync %d: %v", i, e)
		}
	}
	remote := qaRemote(t, c, f, k)
	files := 0
	for p := range remote {
		if !strings.HasSuffix(p, "/") {
			files++
		}
		if strings.Contains(p, "conflict") {
			t.Errorf("unexpected conflict entry %q", p)
		}
	}
	t.Logf("remote after HFS+ replica: %q", keys(remote))
	if files != 1 {
		t.Fatalf("NFC name did not converge to one file: %q", keys(remote))
	}
}
