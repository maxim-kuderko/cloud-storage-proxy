package buffers

import (
	"io"
	"compress/gzip"
	"sync"
	"sync/atomic"
)

type MemBuffer struct {
	r    *io.PipeReader
	w    *io.PipeWriter
	gz   *gzip.Writer
	rw   sync.RWMutex
	size int64
}

func NewMemBuffer(compression int) io.ReadWriteCloser {
	r, w := io.Pipe()
	g, _ := gzip.NewWriterLevel(w, compression)
	return &MemBuffer{
		r:    r,
		w:    w,
		gz:   g,
		size: 0,
	}
}

func (mb *MemBuffer) Read(p []byte) (n int, err error) {
	read, err := mb.r.Read(p)
	if err != nil {
		return read, err
	}
	return read, err
}

func (mb *MemBuffer) Write(p []byte) (n int, err error) {
	w, e := mb.gz.Write(p)
	mb.gz.Write([]byte("\n"))
	atomic.AddInt64(&mb.size, int64(w))
	return w, e
}

func (mb *MemBuffer) Close() error {
	err := mb.gz.Close()
	mb.w.Close()
	return err
}
