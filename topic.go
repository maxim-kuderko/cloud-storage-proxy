package storage_buffer

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
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
		currentCount:      0,
		currentByteSize:   0,
		lastFlush:         &tm,
	}
	t.swapBuffers(true)
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
	if c.shouldFlush() {
		c.swapBuffers(false)
	}
	return bWritten, err
}

func (c *topic) shouldFlush() bool {
	return ((c.topicOptions.MaxSize != -1 && atomic.LoadInt64(&c.currentByteSize) >= c.topicOptions.MaxSize) || (c.topicOptions.MaxLen != -1 && atomic.LoadInt64(&c.currentCount) >= c.topicOptions.MaxLen)) && atomic.LoadInt64(&c.currentCount) > 0
}

func (c *topic) flush() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		go func(c *topic) {
			if time.Now().Unix()-atomic.LoadInt64(c.lastFlush) <= int64(c.topicOptions.Interval) || atomic.LoadInt64(&c.currentCount) == 0 {
				return
			}
			c.swapBuffers(true)
		}(c)
	}

}

func (c *topic) swapBuffers(getLock bool) {
	if getLock {
		c.s.Lock()
		defer c.s.Unlock()
	}
	if c.buff != nil {
		c.buff.Close()
	}
	c.buff = c.topicOptions.BufferDriver()
	go func() {
		c.topicOptions.Callback(c.topicOptions.StorageDriver(c.buff))
	}()
	atomic.StoreInt64(c.lastFlush, time.Now().Unix())
	atomic.StoreInt64(&c.currentCount, 0)
	atomic.StoreInt64(&c.currentByteSize, 0)
	return
}

func (c *topic) shutdown() {
	c.swapBuffers(true)
}

// TopicOptions contains the data necessary to init a topic
// Name: name of the topic key
// maxLen at what count of events in topic should we flush the buffer
// maxSize at what size of events in bytes should we flish the buffer of the topic
// at what max internal should we flush the topic buffer
// note: the topic will flush when we'll hit the limit of any of the three conditions above
// the buffer driver implementation, some are provided in the buffers folder
// the storage buffer is the sink of the data some are provided in the storage folder
// callback function, after a flush to sink this function will be invoked with an error/nil, and the output of the storage driver for the user's convenience
type TopicOptions struct {
	Name          string
	MaxLen        int64
	MaxSize       int64
	Interval      time.Duration
	BufferDriver  func() io.ReadWriteCloser
	StorageDriver func(closer io.ReadWriteCloser) (output map[string]interface{}, err error)
	Callback      func(output map[string]interface{}, err error)
}
