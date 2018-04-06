package cloud_storage_proxy

import (
	"io"
	"sync"
	"time"
)

type topic struct{
	name string
	storeFunc func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{})
	bufferDriverInitializer func() io.ReadWriteCloser
	topicOptions *TopicOptions
	cb                     func(m map[string]interface{}) (bool, error)
	buff io.ReadWriteCloser
	currentSize int64
	currentLength int64
	lastFlush time.Time
	s                      sync.Mutex
}

func newTopic(
	name string,
	storeFunc func(reader io.ReadWriteCloser, options *TopicOptions) (res map[string]interface{}),
	bufferDriverInitializer func() io.ReadWriteCloser,
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
	bWritten, err := c.buff.Write(d)
	c.buff.Write([]byte("\n"))
	if err != nil{
		return false
	}
	c.currentLength++
	c.currentSize += int64(bWritten)
	if c.currentSize >= c.topicOptions.MaxSize || c.currentLength >= c.topicOptions.MaxLen{
		go func() {c.send()}()
	}
	return true
}

func (c *topic) flush(){
	for{
		<- time.After(c.topicOptions.Interval)
		go func(c *topic){
			if time.Now().Sub(c.lastFlush) < c.topicOptions.Interval{
				return
			}
			c.send()
		}(c)
	}

}

func (c *topic) swapBuffers() io.ReadWriteCloser{
	c.s.Lock()
	defer c.s.Unlock()
	c.buff.Close()
	tmp := c.buff
	c.initBuffer()
	c.lastFlush = time.Now()
	c.currentLength = 0
	c.currentSize = 0
	return tmp
}

func (c *topic) send(){
	dataToSend := c.swapBuffers()
	c.storeFunc(dataToSend, c.topicOptions)
}

func (c *topic) initBuffer(){
	c.buff = c.bufferDriverInitializer()
}

func (c *topic) shutdown() {
	c.send()
}