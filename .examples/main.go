package main

import (
	csp "github.com/maxim-kuderko/storage-buffer"
	strg "github.com/maxim-kuderko/storage-buffer/storage"
	"github.com/maxim-kuderko/storage-buffer/buffers"
	"log"
	"io"
	"github.com/klauspost/pgzip"
	"runtime"
	"os"
	"os/signal"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)
	a := csp.TopicOptions{
		Name:     "test",
		MaxLen:   -1,
		MaxSize:  -1,
		Interval: 10,
		BufferDriver: initMemBufferWithCompress,
		StorageDriver: strg.NewS3Loader("test", "us-east-1","", "","txt","", "").S3Store,
		Callback: SQSCallback,
	}
	cfg := cfg{
		m:map[string]*csp.TopicOptions{"test": &a},
	}
	c := csp.NewCollection(1024*1024*1024*10, cfg.get)
	sem := make(chan bool, runtime.NumCPU() * 2)
	for {
		sem <- true
		go func() {
			defer func() {<-sem}()
			if _, err := c.Write("test", []byte("XVlBzgbaiCMRAjWwhTHctcuAxhxKQFDaFpLSjFbcXoEFfRsWxPLDnJObCsNVlgTeMaPEZQleQYhYzRyWJjPjzpfRFEgmotaFetHsbZRjxAwnwekrBEmfdzdcEkXBAkjQZLCtTMtTCoaNatyyiNKAReKJyiXJrscctNswYNsGRussVmaozFZBsbOJiFQGZsnwTKSmVoiGLOpbUOpEdKupdOMeRVjaRzLNTXYeUCWKsXbGyRAOmBTvKSJfjzaLbtZsyMGeuDtRzQMDQiYCOhgHOvgSeycJPJHYNufNjJhhjUVRuSqfgqVMkPYVkURUpiFvIZRgBmyArKCtzkjkZIvaBjMkXVbWGvbqzgexyALBsdjSGpngCwFkDifIBuufFMoWdiTskZoQJMqrTICTojIYxyeSxZyfroRODMbNDRZnPNRWCJPMHDtJmHAYORsUfUMApsVgzHblmYYtEjVgwfFbbGGcnqbaEREunUZjQXmZOtaRLUtmYgmSVYBADDvoxIfsfgPyCKmxIubeYTNDtjAyRRDedMiyLprucjiOgjhYeVwBTCMLfrDGXqwpzwVGqMZcLVCxaSJlDSYEofkkEYeqkKHqgBpnbPbgHMLUIDjUMmpBHCSjMJjxzuaiIsNBakqSwQpOQgNczgaczAInLqLIbAatLYHdaopovFOkqIexsFzXzrlcztxcdJJFuyZHRCovgpVvlGsXalGqARmneBZBFelhXkzzfNaVtAyyqWzKqQFbucqNJYWRncGKKLdTkNyoCSfkFohsVVxSAZWEXejhAquXdaaaZlRHoNXvpayoSsqcnCTuGZamCToZvPynaEphIdXaKUaqmBdtZtcOfFSPqKXSLEfZAPaJzldaUEdhITGHvBrQPqWARPXPtPVGNpdGERwVhGCMdfLitTqwLUecgOczXTbRMGxqPexOUAbUdQrIPjyQyQFStFubVVdHtAknjEQxCqkDIfTGXeJtuncbfqQUsXTOdPORvAUkAwwwTndUJHiQecbxzvqzlPWyqOsU")); err !=nil{
				log.Println(err)
			}
		}()
	}

	<-stop
	c.Shutdown()

}

type cfg struct {
	m map[string]*csp.TopicOptions
}

func (cfg *cfg) get(key string) *csp.TopicOptions{
	return cfg.m[key]
}


func SQSCallback(output map[string]interface{}, err error) {
	if err != nil{
		log.Println("error: ", err.Error())
	}
	log.Println("success: ",output)
}

func initMemBufferWithCompress() io.ReadWriteCloser {
	f := func(w io.WriteCloser) io.WriteCloser {
		cw, _ := pgzip.NewWriterLevel(w, 9)
		return cw
	}
	return buffers.NewPipeBuffer(f, []byte{'\n'})
}
