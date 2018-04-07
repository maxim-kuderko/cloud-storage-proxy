package storage_drivers

import (
	"io"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"bufio"
	"compress/gzip"
	"log"
)



func S3Store(reader io.ReadWriteCloser, opt *cloud_storage_proxy.TopicOptions) map[string]interface{}{
	r,_ := gzip.NewReader(reader)
	b := bufio.NewReader(r)
	s,_ :=b.ReadString('\n')
	log.Println(s)
	reader = nil
	return make(map[string]interface{})
}