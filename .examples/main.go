package main

import (
	"github.com/maxim-kuderko/cloud_storage_proxy/storage_drivers"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"github.com/maxim-kuderko/cloud_storage_proxy/buffer_drivers"
	"errors"
	"time"
)

func main() {
	a := cloud_storage_proxy.TopicOptions{
		MaxLen: 999999,
		MaxSize: 1024,
		Interval: time.Second * 10,
	}
	opt := map[string]*cloud_storage_proxy.TopicOptions{"test": &a}
	c := cloud_storage_proxy.NewCollection(storage_drivers.S3Store, buffer_drivers.NewMemBuffer, opt, SQSCallback)
	for{
		<- time.After(time.Millisecond * 100)
		go c.Write("test", []byte("AA"))
		go c.Write("test", []byte("BB"))
	}
}

func SQSCallback(m map[string]interface{}) (resp bool, err error) {
	return true, errors.New("")
}
