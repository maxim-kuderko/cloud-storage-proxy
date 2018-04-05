package main

import (
	"github.com/maxim-kuderko/cloud_storage_proxy/storage_drivers"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"github.com/maxim-kuderko/cloud_storage_proxy/buffer_drivers"
	"errors"
	"io"
	"time"
)

func main() {
	c := cloud_storage_proxy.NewCollection(storage_drivers.S3Store, NewMemBuferInitializer, SQSCallback)
	credentials := make(map[string]*map[string]interface{})
	for{
		<- time.After(time.Millisecond * 100)
		go c.Write("t", []byte(""))
		go c.Write("t2", []byte(""))
	}
}

func SQSCallback(m map[string]interface{}) (resp bool, err error) {
	return true, errors.New("")
}

func NewMemBuferInitializer() io.ReadWriteCloser{
	return &buffer_drivers.MemBuffer{}
}