package cloud_storage_proxy

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type topic struct {
	name                    string
	storeFunc               func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error)
	bufferDriverInitializer func() io.ReadWriteCloser
	topicOptions            *TopicOptions
	buff                    io.ReadWriteCloser
	currentSize             int64
	currentLength           int64
	globalSizeCounter       *int64
	lastFlush               time.Time
	s                       sync.Mutex
}

func newTopic(
	name string,
	globalSizeCounter *int64,
	storeFunc func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error),
	bufferDriverInitializer func() io.ReadWriteCloser,
	topicOptions *TopicOptions) *topic {
	t := topic{
		name:                    name,
		storeFunc:               storeFunc,
		bufferDriverInitializer: bufferDriverInitializer,
		topicOptions:            topicOptions,
		globalSizeCounter:       globalSizeCounter,
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
	c.currentLength++
	c.currentSize += w
	atomic.AddInt64(c.globalSizeCounter, w)
	if (c.topicOptions.MaxSize != -1 && c.currentSize >= c.topicOptions.MaxSize) || (c.topicOptions.MaxLen != -1 && c.currentLength >= c.topicOptions.MaxLen) {
		c.send(false)
	}
	return bWritten, err
}

func (c *topic) flush() {
	ticker := time.NewTicker(c.topicOptions.Interval)
	for range ticker.C {
		go func(c *topic) {
			if time.Now().Sub(c.lastFlush) < c.topicOptions.Interval {
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
	c.lastFlush = time.Now()
	c.currentLength = 0
	c.currentSize = 0
	return tmp
}

func (c *topic) send(getLock bool) {
	sizeToAck := c.currentSize
	dataToSend := c.swapBuffers(getLock)
	go func(d io.ReadWriteCloser, sizeToAck int64) {
		res, err := c.storeFunc(d, c.topicOptions)
		if err != nil {
			log.Println(err)
			return
		}
		atomic.AddInt64(c.globalSizeCounter, -1*sizeToAck)
		c.topicOptions.Callback(res)
		d = nil
	}(dataToSend, sizeToAck)
}

func (c *topic) initBuffer() {
	c.buff = c.bufferDriverInitializer()
}

func (c *topic) shutdown() {
	c.send(true)
}
