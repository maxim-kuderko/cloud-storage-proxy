package storage_buffer

import (
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Collection struct is responsible for executing the users Write commands,
// and is responsible for the strategy of creation, initialization of the topics
type Collection struct {
	m                       map[string]*topic
	storeFunc               func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error)
	bufferDriverInitializer func() io.ReadWriteCloser
	currentDatacount        *int64
	memMaxUsage             int64
	gracePeriod             time.Duration
	s                       sync.RWMutex
}

// NewCollection initiates the collection with the default params of limits, and a function the fetches the topic config
func NewCollection(memMaxUsage int64, gracePeriod time.Duration) *Collection {
	s := int64(0)
	c := Collection{
		m: map[string]*topic{},
		memMaxUsage:      memMaxUsage,
		currentDatacount: &s,
		gracePeriod:      gracePeriod,
	}
	go c.blockByMaxSize()
	return &c
}

// Write the data to a specific topic in a thread-safe manner
// This function is blocking at boot time of any new topic and when the Collection has reached memMaxUsage
// *** -> if you use go functions to call it implement a semaphore to avoid excess go routines <- ***
func (c *Collection) Write(topic *TopicOptions, partition []string, d []byte) (int, error) {
	t, ok := c.safeRead(topic)
	if !ok {
		t = c.safeInitTopic(topic)
	}
	written, err := t.write(partition, d)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(c.currentDatacount, int64(written))
	return written, nil
}

// Shutdown blocks until collection and it's topic data is empty
func (c *Collection) Shutdown() {
	c.s.Lock()
	<-time.After(c.gracePeriod)
}

func (c *Collection) safeRead(topic *TopicOptions) (t *topic, ok bool) {
	c.s.RLock()
	defer c.s.RUnlock()
	t, ok = c.m[topic.ID]
	if !ok {
		return t, ok
	}
	if t.topicOptions.LastUpdated.Before(topic.LastUpdated) {
		return t, false
	}
	return t, ok
}

// checks the mem usage of the runtime every milli and checks it against memMaxsize, blocks incoming writes until mem will free up
func (c *Collection) blockByMaxSize() {
	var m runtime.MemStats
	for range time.NewTicker(time.Millisecond).C {
		runtime.ReadMemStats(&m)
		if m.Sys >= uint64(c.memMaxUsage) {
			func() {
				c.s.Lock()
				defer c.s.Unlock()
				for m.Sys >= uint64(c.memMaxUsage) {
					time.Sleep(time.Millisecond)
					runtime.ReadMemStats(&m)
				}
			}()

		}
	}
}

func (c *Collection) safeInitTopic(topic *TopicOptions) *topic {
	c.s.Lock()
	defer c.s.Unlock()
	v, ok := c.m[topic.ID]
	if ok && v.topicOptions.LastUpdated.Equal(topic.LastUpdated) {
		return v
	}
	v = newTopic(
		c.currentDatacount,
		topic)
	c.m[topic.ID] = v
	return v
}
