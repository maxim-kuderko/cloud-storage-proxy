package cloud_storage_proxy

import (
	"io"
	"sync"
	"log"
)

type Collection struct {
	m map[string]*topic
	storeFunc func(reader *io.Reader, options *TopicOptions) (res map[string]interface{})
	bufferDriverInitializer func(options *TopicOptions) io.ReadWriteCloser
	topicsOptions map[string]*TopicOptions
	cb                     func(m map[string]interface{}) (bool, error)
	s                      sync.RWMutex
}

func NewCollection(
	storeFunc func(reader *io.Reader, options *TopicOptions) (res map[string]interface{}),
	bufferDriverInitializer func(options *TopicOptions) io.ReadWriteCloser,
	topicsOptions map[string]*TopicOptions,
	cb func(m map[string]interface{}) (bool, error)) *Collection {
	c := Collection{m: map[string]*topic{}, storeFunc: storeFunc, bufferDriverInitializer: bufferDriverInitializer, cb: cb, topicsOptions:topicsOptions}
	return &c
}

func (c *Collection) Write(topic string, d []byte) bool {
	t, ok := c.safeRead(topic)
	if !ok {
		t, ok =c.safeInitTopic(topic)
	}
	return t.write(d)
}

func (c *Collection) safeRead(topic string) (t *topic, ok bool) {
	c.s.RLock()
	defer c.s.RUnlock()
	t, ok = c.m[topic]
	return  t, ok
}

func (c *Collection) safeInitTopic(topic string) (*topic, bool){
	c.s.Lock()
	defer c.s.Unlock()
	v , ok := c.safeRead(topic)
	if ok {
		return v, true
	}
	v = newTopic(
		topic,
		c.storeFunc,
		c.bufferDriverInitializer,
		c.topicsOptions[topic],
		c.cb,
	)
	return v, true
}



func (c *Collection) Shutdown() {
	c.s.Lock()
	defer c.s.Unlock()
	wg := sync.WaitGroup{}
	for t, topic := range c.m{
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
