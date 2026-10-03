package engine

import (
	"errors"
	"io"
	"os"
)

type countReader struct {
	io.Reader
	n int64
}

func (r *countReader) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	r.n += int64(n)
	return n, e
}
func storedContentError(e error, received, size int64) error {
	var truncated *incompleteContent
	if errors.As(e, &truncated) && received == size {
		return ErrIntegrity
	}
	return e
}

var errLocalObstacle = errors.New("local obstacle")

func isTransferError(e error) bool {
	var p *os.PathError
	return !errors.As(e, &p) && !errors.Is(e, errLocalObstacle) && !errors.Is(e, ErrBusy)
}
