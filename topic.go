package storage_buffer

import (
	"io"
	"time"
	"sync"
	"sync/atomic"
)

type topic struct {
	topicOptions      *TopicOptions
	buff              io.ReadWriteCloser
	currentByteSize   int64
	currentCount      int64
	globalSizeCounter *int64
	lastFlush         *int64
	s                 sync.Mutex
}

func newTopic(globalSizeCounter *int64, topicOptions *TopicOptions) *topic {
	tm := time.Now().UnixNano()
	t := topic{
		topicOptions:      topicOptions,
		globalSizeCounter: globalSizeCounter,
		currentCount: 0,
		currentByteSize: 0,
		lastFlush: &tm,
	}
	t.initBuffer()
	go func() { t.flush() }()
	return &t
}

func (c *topic) write(d []byte) (int, error) {
	c.s.Lock()
	defer c.s.Unlock()
	bWritten, err := c.buff.Write(d)
	if err != nil {
		return bWritten, err
	}
	w := int64(bWritten)
	atomic.AddInt64(&c.currentCount, 1)
	atomic.AddInt64(&c.currentByteSize, w)
	if (c.topicOptions.MaxSize != -1 && c.currentByteSize >= c.topicOptions.MaxSize) || (c.topicOptions.MaxLen != -1 && c.currentCount >= c.topicOptions.MaxLen) {
		c.send(false)
	}
	return bWritten, err
}

func (c *topic) flush() {
	ticker := time.NewTicker(c.topicOptions.Interval)
	for range ticker.C {
		go func(c *topic) {
			if atomic.LoadInt64(c.lastFlush) <= time.Now().Add( -1 * c.topicOptions.Interval).UnixNano() || atomic.LoadInt64(&c.currentCount) == 0{
				return
			}
			c.send(true)
		}(c)
	}

}

func (c *topic) swapBuffers(getLock bool) io.ReadWriteCloser {
	if getLock {
		c.s.Lock()
		defer c.s.Unlock()
	}
	c.buff.Close()
	tmp := c.buff
	c.initBuffer()
	atomic.StoreInt64(c.lastFlush,time.Now().UnixNano())
	atomic.StoreInt64(&c.currentCount,0 )
	atomic.StoreInt64(&c.currentByteSize, 0)
	return tmp
}

func (c *topic) send(getLock bool) {
	sizeToAck := atomic.LoadInt64(&c.currentByteSize)
	dataToSend := c.swapBuffers(getLock)
	go func(d io.ReadWriteCloser, sizeToAck int64) {
		defer func() {
			atomic.AddInt64(c.globalSizeCounter, -1*sizeToAck)
			d = nil
		}()
		res := c.topicOptions.StorageDriver(d)
		c.topicOptions.Callback(res)
	}(dataToSend, sizeToAck)
}

func (c *topic) initBuffer() {
	c.buff = c.topicOptions.BufferDriver()
}

func (c *topic) shutdown() {
	c.send(true)
}

type TopicOptions struct {
	Name             string
	MaxLen           int64
	MaxSize          int64
	Interval         time.Duration
	BufferDriver     func() io.ReadWriteCloser
	StorageDriver    func(closer io.ReadWriteCloser) (output map[string]interface{})
	Callback         func(output map[string]interface{})
}
