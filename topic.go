package storage_buffer

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type topic struct {
	topicOptions      *TopicOptions
	buffers           map[string]*TopicBuffer
	globalSizeCounter *int64
	s                 sync.RWMutex
}

func newTopic(globalSizeCounter *int64, topicOptions *TopicOptions) *topic {
	t := topic{
		topicOptions:      topicOptions,
		globalSizeCounter: globalSizeCounter,
		buffers:make(map[string]*TopicBuffer),
	}
	go t.gc()
	return &t
}

func (c *topic) gc() {
	ticker := time.NewTicker(c.topicOptions.Interval * 2)
	for range ticker.C{
		func(){
			c.s.Lock()
			defer c.s.Unlock()
			for k, v :=  range c.buffers{
				if atomic.LoadInt64(v.lastFlush)  <= time.Now().Add(-1 * (c.topicOptions.Interval * 10)).Unix(){
					v.close()
					delete(c.buffers, k)
				}
			}
		}()
	}
}

func (c *topic) write(partition []string, d []byte) (int, error) {
	bWritten, err := c.loadOrStoreTopicBuffer(partition).write(d)
	if err != nil {
		return bWritten, err
	}
	return bWritten, err
}

func (c *topic) loadOrStoreTopicBuffer(partition []string) *TopicBuffer {
	key := strings.Join(partition,",")
		c.s.RLock()
	v, ok := c.buffers[key]
	c.s.RUnlock()
	if !ok {
		c.s.Lock()
		defer c.s.Unlock()
		v, ok := c.buffers[key]
		if ok {
			return v
		}
		v = c.initTopicBuffer(partition)
		c.buffers[key] = v
		return v
	}
	return v
}

func (c *topic) initTopicBuffer(partition []string) *TopicBuffer {
	return newTopicBuffer(c.topicOptions, partition)
}


// TopicOptions contains the data necessary to init a topic
// Name: name of the topic key
// maxLen at what count of events in topic should we flush the buffer
// maxSize at what size of events in bytes should we flush the buffer of the topic
// at what max internal should we flush the topic buffer
// note: the topic will flush when we'll hit the limit of any of the three conditions above
// the buffer driver implementation, some are provided in the buffers folder
// the storage buffer is the sink of the data some are provided in the storage folder
// callback function, after a flush to sink this function will be invoked with an error/nil, and the output of the storage driver for the user's convenience
type TopicOptions struct {
	ID            string
	Name          string
	MaxLen        int64
	MaxSize       int64
	Interval      time.Duration
	StorageDriver func(partition []string) io.WriteCloser
	LastUpdated   time.Time
}
