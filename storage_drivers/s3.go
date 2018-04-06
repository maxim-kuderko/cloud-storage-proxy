package storage_drivers

import (
	"io"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"log"
	"bufio"
	"compress/gzip"
	"io/ioutil"
)



func S3Store(reader io.ReadWriteCloser, opt *cloud_storage_proxy.TopicOptions) map[string]interface{}{
	r,_ := gzip.NewReader(reader)
	b := bufio.NewReader(r)
	d, _ := ioutil.ReadAll(b)
	log.Println(string(d))
	return make(map[string]interface{})
}