package engine

import (
	"os"
	"testing"

	"github.com/pnegahdar/drivesync/internal/embedpg"
)

func TestMain(m *testing.M) {
	code := m.Run()
	embedpg.Shutdown()
	os.Exit(code)
}
