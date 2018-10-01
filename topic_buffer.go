package storage_buffer

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type TopicBuffer struct {
	topicOptions    *TopicOptions
	partition       []string
	store           io.WriteCloser
	currentByteSize int64
	currentCount    int64
	s               sync.Mutex
	ticker          *time.Ticker
	lastFlush       *int64
}

func newTopicBuffer(topicOptions *TopicOptions, partition []string) *TopicBuffer {
	f := time.Now().Unix()
	tb := &TopicBuffer{
		topicOptions:    topicOptions,
		partition:       partition,
		currentByteSize: 0,
		currentCount:    0,
		lastFlush:       &f,
		ticker:          time.NewTicker(time.Second),
	}
	tb.swapBuffers(true)
	go tb.flush()
	return tb
}

func (c *TopicBuffer) write(b []byte) (int, error) {
	c.s.Lock()
	defer c.s.Unlock()
	written, err := c.store.Write(b)
	if err != nil {
		return written, err
	}
	atomic.AddInt64(&c.currentCount, int64(1))
	atomic.AddInt64(&c.currentByteSize, int64(written))
	if c.shouldFlush() {
		c.swapBuffers(false)
	}
	return written, err
}

func (c *TopicBuffer) close() {
	c.s.Lock()
	c.ticker.Stop()
	c.swapBuffers(false)
}

func (c *TopicBuffer) shouldFlush() bool {
	return ((c.topicOptions.MaxSize != -1 && atomic.LoadInt64(&c.currentByteSize) >= c.topicOptions.MaxSize) || (c.topicOptions.MaxLen != -1 && atomic.LoadInt64(&c.currentCount) >= c.topicOptions.MaxLen)) && atomic.LoadInt64(&c.currentCount) > 0
}

func (c *TopicBuffer) flush() {
	for range c.ticker.C {
		go func(c *TopicBuffer) {
			if time.Now().Unix()-atomic.LoadInt64(c.lastFlush) <= int64(c.topicOptions.Interval.Seconds()) || atomic.LoadInt64(&c.currentCount) == 0 {
				return
			}
			c.swapBuffers(true)
		}(c)
	}

}

func (c *TopicBuffer) swapBuffers(getLock bool) {
	if getLock {
		c.s.Lock()
		defer c.s.Unlock()
	}
	if c.store != nil {
		tmp := c.store
		go tmp.Close()
	}
	c.store = c.topicOptions.StorageDriver(c.partition)
	atomic.StoreInt64(c.lastFlush, time.Now().Unix())
	atomic.StoreInt64(&c.currentCount, 0)
	atomic.StoreInt64(&c.currentByteSize, 0)
	return
}
