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
	topicsOptionsFetcher    func(topicName string) *TopicOptions
	m                       map[string]*topic
	storeFunc               func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error)
	bufferDriverInitializer func() io.ReadWriteCloser
	topicsOptions           map[string]*TopicOptions
	currentDatacount        *int64
	memMaxUsage             int64
	s                       sync.RWMutex
}

// NewCollection initiates the collection with the default params of limits, and a function the fetches the topic config
func NewCollection(memMaxUsage int64, topicsOptionsFetcher func(topicName string) *TopicOptions) *Collection {
	s := int64(0)
	c := Collection{m: map[string]*topic{},
		memMaxUsage: memMaxUsage,
		currentDatacount: &s,
		topicsOptionsFetcher: topicsOptionsFetcher,
	}
	go c.blockByMaxSize()
	return &c
}

// Write the data to a specific topic in a thread-safe manner
// This function is blocking at boot time of any new topic and when the Collection has reached memMaxUsage
// *** -> if you use go functions to call it implement a semaphore to avoid excess go routines <- ***
func (c *Collection) Write(topicName string, d []byte) (int, error) {
	t, ok := c.safeRead(topicName)
	if !ok {
		t, ok = c.safeInitTopic(topicName)
	}
	written, err := t.write(d)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(c.currentDatacount, int64(written))
	return written, nil
}

// Shutdown blocks until collection and it's topic data is empty
func (c *Collection) Shutdown() {
	c.s.Lock()
	defer c.s.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(c.m))
	for _, t := range c.m {
		go func(t *topic, wg *sync.WaitGroup) {
			defer wg.Done()
			t.shutdown()
		}(t, &wg)
	}
	wg.Wait()
}

func (c *Collection) safeRead(topic string) (t *topic, ok bool) {
	c.s.RLock()
	defer c.s.RUnlock()
	t, ok = c.m[topic]
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

func (c *Collection) safeInitTopic(topic string) (*topic, bool) {
	c.s.Lock()
	defer c.s.Unlock()
	v, ok := c.m[topic]
	if ok {
		return v, true
	}
	v = newTopic(
		c.currentDatacount,
		c.topicsOptionsFetcher(topic))
	c.m[topic] = v
	return v, true
}
