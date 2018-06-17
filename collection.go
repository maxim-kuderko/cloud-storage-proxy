package storage_buffer

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
	"log"
	"runtime"
)

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

func NewCollection(memMaxUsage int64, topicsOptionsFetcher func(topicName string) *TopicOptions) *Collection {
	s := int64(0)
	c := Collection{m:
	map[string]*topic{},
		memMaxUsage: memMaxUsage,
		currentDatacount: &s,
		topicsOptionsFetcher: topicsOptionsFetcher,
	}
	go c.flush()
	return &c
}

func (c *Collection) Write(topic string, d []byte) (int, error) {
	t, ok := c.safeRead(topic)
	if !ok {
		t, ok = c.safeInitTopic(topic)
	}
	written, err := t.write(d)
	if err != nil {
		log.Println("errrrrr ", err)
		return 0, err
	}
	atomic.AddInt64(c.currentDatacount, int64(written))
	return written, err
}

func (c *Collection) safeRead(topic string) (t *topic, ok bool) {
	c.s.RLock()
	defer c.s.RUnlock()
	t, ok = c.m[topic]
	return t, ok
}

func (c *Collection) blockByMaxSize() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Sys >= uint64(c.memMaxUsage) {
		c.s.Lock()
		defer c.s.Unlock()
		log.Println("locked")
		for m.Sys >= uint64(c.memMaxUsage) {
			time.Sleep(time.Second)
			runtime.ReadMemStats(&m)
			log.Println("still locked")
		}
		log.Println("released")
	}
}

func (c *Collection) flush() {
	for range time.NewTicker(time.Millisecond).C {
		c.blockByMaxSize()
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

func (c *Collection) Shutdown() {
	c.s.Lock()
	defer c.s.Unlock()
	wg := sync.WaitGroup{}
	for _, topic := range c.m {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			topic.shutdown()
			wg.Done()
		}(&wg)

	}
	wg.Wait()
}
