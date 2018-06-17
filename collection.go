package cloud_storage_proxy

import (
	"io"
	"sync"
	"log"
	"sync/atomic"
	"time"
)

type Collection struct {
	m                       map[string]*topic
	storeFunc               func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error)
	bufferDriverInitializer func() io.ReadWriteCloser
	topicsOptions           map[string]*TopicOptions
	currentDatacount        *int64
	globalBufferMaxSize     int64
	s                       sync.RWMutex
}

func NewCollection(
	storeFunc func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}, err error),
	bufferDriverInitializer func() io.ReadWriteCloser,
	globalBufferMaxSize int64,
	topicsOptions map[string]*TopicOptions) *Collection {
		s := int64(0)
	c := Collection{m: map[string]*topic{}, storeFunc: storeFunc, globalBufferMaxSize: globalBufferMaxSize, currentDatacount: &s,
	bufferDriverInitializer: bufferDriverInitializer, topicsOptions: topicsOptions}
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

func (c *Collection) blockByMaxSize(){
	if atomic.LoadInt64(c.currentDatacount) >= c.globalBufferMaxSize{
		c.s.Lock()
		defer c.s.Unlock()
		for atomic.LoadInt64(c.currentDatacount) >= c.globalBufferMaxSize{
			time.Sleep(time.Millisecond)
		}
	}
}

func (c *Collection) flush(){
	for range time.NewTicker(time.Millisecond).C{
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
		topic,
		c.currentDatacount,
		c.storeFunc,
		c.bufferDriverInitializer,
		c.topicsOptions[topic])
	c.m[topic] = v
	return v, true
}

func (c *Collection) Shutdown() {
	c.s.Lock()
	defer c.s.Unlock()
	wg := sync.WaitGroup{}
	for t, topic := range c.m {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			log.Println("Shutting down: ", t)
			topic.shutdown()
			log.Println("Topic: ", t, " was shutting down")
			wg.Done()
		}(&wg)

	}
	wg.Wait()
}
