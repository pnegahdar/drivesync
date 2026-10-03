package drivesync

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"unicode/utf8"
)

type FolderKey [32]byte

func NewFolderKey() FolderKey {
	var k FolderKey
	if _, e := rand.Read(k[:]); e != nil {
		panic(e)
	}
	return k
}
func KeyCheck(k FolderKey) []byte {
	h := hmac.New(sha256.New, k[:])
	h.Write([]byte("drivesync/v1/key-check"))
	return h.Sum(nil)
}
func CheckKey(k FolderKey, check []byte) error {
	if !hmac.Equal(KeyCheck(k), check) {
		return ErrKey
	}
	return nil
}
func subkey(k FolderKey, purpose, folder string) []byte {
	b, e := hkdf.Key(sha256.New, k[:], []byte(folder), "drivesync/v1/"+purpose, 32)
	if e != nil {
		panic(e)
	}
	return b
}
func aead(k []byte) cipher.AEAD {
	b, e := aes.NewCipher(k)
	if e != nil {
		panic(e)
	}
	a, e := cipher.NewGCM(b)
	if e != nil {
		panic(e)
	}
	return a
}

// NormalizePath accepts portable, relative UTF-8 paths. It rejects rather than cleans traversal.
func NormalizePath(s string) (string, error) {
	if s == "" || !utf8.ValidString(s) || len(s) > 4096 || strings.ContainsAny(s, "\\\x00:") || strings.HasPrefix(s, "/") {
		return "", ErrInvalid
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || part == "." || part == ".." || len(part) > 255 || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return "", ErrInvalid
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || strings.EqualFold(part, ".drivesync") || strings.EqualFold(part, ".drivesyncignore") || strings.HasPrefix(strings.ToLower(part), ".drivesync-tmp-") {
			return "", ErrInvalid
		}
		if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '0' && base[3] <= '9' {
			return "", ErrInvalid
		}
		for _, c := range part {
			if c < 32 || c == 127 || strings.ContainsRune("<>\"|?*", c) {
				return "", ErrInvalid
			}
		}
	}
	if path.Clean(s) != s {
		return "", ErrInvalid
	}
	return s, nil
}
func PathID(k FolderKey, folder, p string) (string, error) {
	n, e := NormalizePath(p)
	if e != nil {
		return "", e
	}
	h := hmac.New(sha256.New, subkey(k, "path", folder))
	h.Write([]byte(n))
	return hex.EncodeToString(h.Sum(nil)), nil
}

type FileMetadata struct {
	Path, BlobID string
	Size         int64
	Mode         uint32
	Directory    bool
	Hash         string
}

func SealMetadata(k FolderKey, folder, pid string, m FileMetadata) ([]byte, error) {
	n, e := NormalizePath(m.Path)
	if e != nil {
		return nil, e
	}
	id, _ := PathID(k, folder, n)
	if id != pid {
		return nil, ErrInvalid
	}
	b, e := json.Marshal(m)
	if e != nil {
		return nil, e
	}
	a := aead(subkey(k, "metadata", folder))
	nonce := make([]byte, a.NonceSize())
	if _, e = rand.Read(nonce); e != nil {
		return nil, e
	}
	return a.Seal(nonce, nonce, b, []byte(folder+"/"+pid)), nil
}
func OpenMetadata(k FolderKey, folder string, row Row) (FileMetadata, error) {
	var m FileMetadata
	a := aead(subkey(k, "metadata", folder))
	if row.FolderID != folder || len(row.Metadata) < a.NonceSize() {
		return m, ErrIntegrity
	}
	nonce := row.Metadata[:a.NonceSize()]
	b, e := a.Open(nil, nonce, row.Metadata[a.NonceSize():], []byte(folder+"/"+row.PathID))
	if e != nil {
		return m, ErrIntegrity
	}
	if json.Unmarshal(b, &m) != nil {
		return m, ErrIntegrity
	}
	id, e := PathID(k, folder, m.Path)
	if e != nil || id != row.PathID || m.BlobID != row.BlobID || m.Size < 0 || m.Mode & ^uint32(0777) != 0 {
		return m, ErrIntegrity
	}
	return m, nil
}

const ChunkSize = 64 * 1024

// SealedSize includes the authenticated final empty chunk, even for an empty file.
func SealedSize(n int64) int64 {
	if n < 0 || n > (1<<62) {
		return -1
	}
	return 20 + n + ((n+ChunkSize-1)/ChunkSize)*21 + 21
}
func contentAEAD(k FolderKey, folder, blob, pid string, header []byte) cipher.AEAD {
	base := subkey(k, "content/"+blob+"/"+pid, folder)
	key, e := hkdf.Key(sha256.New, base, header[4:], "drivesync/v1/content-stream", 32)
	if e != nil {
		panic(e)
	}
	return aead(key)
}
func chunkAD(folder, blob, pid string, index uint64, final byte, header []byte) []byte {
	return append([]byte(fmt.Sprintf("%s/%s/%s/%d/%d/", folder, blob, pid, index, final)), header...)
}
func SealContent(w io.Writer, r io.Reader, k FolderKey, folder, blob, pid string) error {
	if !validID(folder) || !validID(blob) || !validPathID(pid) {
		return ErrInvalid
	}
	header := make([]byte, 20)
	copy(header, []byte{'D', 'S', 'C', 1})
	if _, e := rand.Read(header[4:]); e != nil {
		return e
	}
	a := contentAEAD(k, folder, blob, pid, header)
	if e := writeAll(w, header); e != nil {
		return e
	}
	buf := make([]byte, ChunkSize)
	var idx uint64
	for {
		n, e := io.ReadFull(r, buf)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return e
		}
		if n > 0 {
			if err := writeChunk(w, a, header, folder, blob, pid, idx, 0, buf[:n]); err != nil {
				return err
			}
			idx++
		}
		if e != nil {
			return writeChunk(w, a, header, folder, blob, pid, idx, 1, nil)
		}
	}
}
func writeChunk(w io.Writer, a cipher.AEAD, header []byte, folder, blob, pid string, idx uint64, final byte, p []byte) error {
	nonce := make([]byte, 12)
	copy(nonce, header[4:])
	binary.BigEndian.PutUint64(nonce[4:], idx)
	sealed := a.Seal(nil, nonce, p, chunkAD(folder, blob, pid, idx, final, header))
	frame := make([]byte, 5)
	binary.BigEndian.PutUint32(frame, uint32(len(sealed)))
	frame[4] = final
	if e := writeAll(w, frame); e != nil {
		return e
	}
	return writeAll(w, sealed)
}

// OpenContent only promises integrity on successful return. Callers must stage output before publishing it.
func OpenContent(w io.Writer, r io.Reader, k FolderKey, folder, blob, pid string) error {
	if !validID(folder) || !validID(blob) || !validPathID(pid) {
		return ErrIntegrity
	}
	header := make([]byte, 20)
	if _, e := io.ReadFull(r, header); e != nil || !bytes.Equal(header[:4], []byte{'D', 'S', 'C', 1}) {
		return ErrIntegrity
	}
	a := contentAEAD(k, folder, blob, pid, header)
	var idx uint64
	for {
		frame := make([]byte, 5)
		if _, e := io.ReadFull(r, frame); e != nil {
			return ErrIntegrity
		}
		n := binary.BigEndian.Uint32(frame)
		final := frame[4]
		if n < 16 || n > ChunkSize+16 || final > 1 || (final == 1 && n != 16) || (final == 0 && n == 16) {
			return ErrIntegrity
		}
		sealed := make([]byte, n)
		if _, e := io.ReadFull(r, sealed); e != nil {
			return ErrIntegrity
		}
		nonce := make([]byte, 12)
		copy(nonce, header[4:])
		binary.BigEndian.PutUint64(nonce[4:], idx)
		p, e := a.Open(nil, nonce, sealed, chunkAD(folder, blob, pid, idx, final, header))
		if e != nil {
			return ErrIntegrity
		}
		if final == 1 {
			var trailing [1]byte
			for empty := 0; empty < 100; empty++ {
				n, e := r.Read(trailing[:])
				if n != 0 {
					return ErrIntegrity
				}
				if e == io.EOF {
					return nil
				}
				if e != nil {
					return ErrIntegrity
				}
			}
			return io.ErrNoProgress
		}
		if e = writeAll(w, p); e != nil {
			return e
		}
		idx++
	}
}

func writeAll(w io.Writer, p []byte) error {
	n, e := w.Write(p)
	if e != nil {
		return e
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}
