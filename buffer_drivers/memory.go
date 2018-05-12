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

func NewMemBuffer(compression int) io.ReadWriteCloser {
	b := &bytes.Buffer{}
	g, _ := gzip.NewWriterLevel(b, compression)
	return &MemBuffer{
		buff: b,
		gz:   g,
	}
}

func (mb *MemBuffer) Read(p []byte) (n int, err error) {
	d, err := mb.buff.Read(p)
	if err != nil {
		mb.buff = nil
	}
	return d, err
}
func (mb *MemBuffer) Write(p []byte) (n int, err error) {
	sl := mb.buff.Len()
	_, e := mb.gz.Write(p)
	mb.gz.Write([]byte("\n"))
	written := mb.buff.Len() - sl
	return written, e
}

func (mb *MemBuffer) Close() error {
	return mb.gz.Close()
}
