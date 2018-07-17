package buffers

import (
	"io"
	"sync/atomic"
)

// Pipe buffer is will direct all received bytes to the reader
// this is the most basic buffers that given a fast enough reader will hold minimal data in mem
// this buffer comes with built in gzip func
// the function can be non thread safe as the topic already is
type Pipe struct {
	r    *io.PipeReader
	w    *io.PipeWriter
	compressor   io.WriteCloser
	size int64
	sep  []byte
	io.ReadWriteCloser
}

// NewPipeBuffer initialized the buffer with a user defined  gzip compression level and a separator for the data
func NewPipeBuffer(compressorFunc func(w io.WriteCloser) io.WriteCloser, separator []byte) io.ReadWriteCloser {
	r, w := io.Pipe()
	return &Pipe{
		r:    r,
		w:    w,
		compressor:   compressorFunc(w),
		size: 0,
		sep:  separator,
	}
}

// Read method reads from the pipe
// No need to close it on EOF
func (pb *Pipe) Read(p []byte) (n int, err error) {
	read, err := pb.r.Read(p)
	if err != nil {
		if err == io.EOF {
			pb.r.Close()
		}
		return read, err
	}
	return read, err
}

// Write writes to the underlining buffer
// NEED to close it on write finish
// note: the returned n int is the len(p) not the len of bytes actually written to buffer
func (pb *Pipe) Write(p []byte) (n int, err error) {
	w, e := pb.compressor.Write(p)
	pb.compressor.Write(pb.sep)
	atomic.AddInt64(&pb.size, int64(w))
	return w, e
}

// Close is called the the expiration of a buffer (flush) is executed i.e every 10 secs
// which means the
func (pb *Pipe) Close() error {
	err := pb.compressor.Close()
	pb.w.Close()
	return err
}
