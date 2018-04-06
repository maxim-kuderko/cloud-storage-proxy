package main

import (
	"github.com/maxim-kuderko/cloud_storage_proxy/storage_drivers"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"github.com/maxim-kuderko/cloud_storage_proxy/buffer_drivers"
	"errors"
	"time"
	"log"
	"math/rand"
)

func main() {
	a := cloud_storage_proxy.TopicOptions{
		MaxLen: -1,
		MaxSize: 1024*1024*1024,
		Interval: time.Second * 60,
	}
	opt := map[string]*cloud_storage_proxy.TopicOptions{"test": &a, "test2": &a}
	c := cloud_storage_proxy.NewCollection(storage_drivers.S3Store, buffer_drivers.NewMemBuffer, opt, SQSCallback)
	for{
		<- time.After(time.Nanosecond * 10 )
		go c.Write("test", generateRandomStr(1024))
	}
}

func SQSCallback(m map[string]interface{}) (resp bool, err error) {
	log.Println("SQS callback , ", m)
	return true, errors.New("")
}


func generateRandomStr(size int) []byte{
	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b

}
var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")