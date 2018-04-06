package buffer_drivers

import (
	"bytes"
	"compress/gzip"
	"io"
)

type MemBuffer struct {
	buff *bytes.Buffer
	gz   *gzip.Writer
}

func NewMemBuffer() io.ReadWriteCloser {
	b := &bytes.Buffer{}
	return &MemBuffer{
		buff: b,
		gz:   gzip.NewWriter(b),
	}
}

func (mb *MemBuffer) Read(p []byte) (n int, err error) {
	return mb.buff.Read(p)
}
func (mb *MemBuffer) Write(p []byte) (n int, err error) {
	mb.gz.Write(p)
	return mb.gz.Write([]byte("\n"))
}

func (mb *MemBuffer) Close() error {
	return mb.gz.Close()
}
