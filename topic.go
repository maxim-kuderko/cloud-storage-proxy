package cloud_storage_proxy

import (
	"io"
	"sync"
	"time"
)

type topic struct{
	name string
	storeFunc func(reader *io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{})
	bufferDriverInitializer func(opt *TopicOptions) io.ReadWriteCloser
	topicOptions *TopicOptions
	cb                     func(m map[string]interface{}) (bool, error)
	buff io.ReadWriteCloser
	//lastFlush int64
	s                      sync.Mutex
}

func newTopic(
	name string,
	storeFunc func(reader *io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}),
	bufferDriverInitializer func(opt *TopicOptions) io.ReadWriteCloser,
	topicOptions *TopicOptions,
	cb func(m map[string]interface{}) (bool, error)) *topic{
		t := topic{
			name: name,
			storeFunc:storeFunc,
			bufferDriverInitializer:bufferDriverInitializer,
			topicOptions: topicOptions,
			cb: cb,
		}
		t.initBuffer()
		go func() {t.flush()}()
		return &t
}

func (c *topic) write(d []byte) bool {
	c.s.Lock()
	defer c.s.Unlock()
	_, err := c.buff.Write(d)
	if err != nil{
		return false
	}
	return true
}

func (c *topic) flush(){
	for{
		<- time.After(c.topicOptions.Interval)
		c.send()
	}

}

func (c *topic) swapBuffers() *io.ReadWriteCloser{
	c.s.Lock()
	defer c.s.Unlock()
	c.buff.Close()
	tmp := c.buff
	c.initBuffer()
	return &tmp
}

func (c *topic) send(){
	dataToSend := c.swapBuffers()
	go func() {
		c.storeFunc(dataToSend, c.topicOptions)
	}()
}

func (c *topic) initBuffer(){
	c.buff = c.bufferDriverInitializer(c.topicOptions)
}

func (c *topic) shutdown() {
	c.send()
}