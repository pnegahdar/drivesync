package engine

import (
	"crypto/rand"
	"encoding/json"
)

func sealRawMetadata(k FolderKey, folder, pid string, m FileMetadata) []byte {
	b, _ := json.Marshal(m)
	a := aead(subkey(k, "metadata", folder))
	n := make([]byte, a.NonceSize())
	_, _ = rand.Read(n)
	return a.Seal(n, n, b, []byte(folder+"/"+pid))
}
