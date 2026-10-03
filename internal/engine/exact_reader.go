package engine

import "io"

// Never hand the oversize sentinel to a blob store. Publication requires EOF;
// an extra byte instead terminates its staging reader with an error.
type exactReader struct {
	r    io.Reader
	left int64
}

func (r *exactReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		var extra [1]byte
		n, e := io.ReadFull(r.r, extra[:])
		if n > 0 {
			return 0, ErrInvalid
		}
		return 0, e
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, e := r.r.Read(p)
	r.left -= int64(n)
	return n, e
}
