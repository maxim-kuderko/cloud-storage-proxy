package main

import (
	csp "github.com/maxim-kuderko/storage-buffer"
	strg "github.com/maxim-kuderko/storage-buffer/storage"
	bf "github.com/maxim-kuderko/storage-buffer/buffers"
	"time"
	"log"
	"io"
)

func main() {

	a := csp.TopicOptions{
		Name:     "test",
		MaxLen:   -1,
		MaxSize:  1024 * 1024 * 250,
		Interval: time.Second * 60,
		BufferDriver: initMemBufferWithCompress,
		StorageDriver: strg.NewS3Loader("test", "us-east-1","", "","txt","", "").S3Store,
		Callback: SQSCallback,
	}
	cfg := cfg{
		m:map[string]*csp.TopicOptions{"test": &a},
	}
	c := csp.NewCollection(1024*1024*1024, cfg.get)
	for {
		c.Write("test", []byte("XVlBzgbaiCMRAjWwhTHctcuAxhxKQFDaFpLSjFbcXoEFfRsWxPLDnJObCsNVlgTeMaPEZQleQYhYzRyWJjPjzpfRFEgmotaFetHsbZRjxAwnwekrBEmfdzdcEkXBAkjQZLCtTMtTCoaNatyyiNKAReKJyiXJrscctNswYNsGRussVmaozFZBsbOJiFQGZsnwTKSmVoiGLOpbUOpEdKupdOMeRVjaRzLNTXYeUCWKsXbGyRAOmBTvKSJfjzaLbtZsyMGeuDtRzQMDQiYCOhgHOvgSeycJPJHYNufNjJhhjUVRuSqfgqVMkPYVkURUpiFvIZRgBmyArKCtzkjkZIvaBjMkXVbWGvbqzgexyALBsdjSGpngCwFkDifIBuufFMoWdiTskZoQJMqrTICTojIYxyeSxZyfroRODMbNDRZnPNRWCJPMHDtJmHAYORsUfUMApsVgzHblmYYtEjVgwfFbbGGcnqbaEREunUZjQXmZOtaRLUtmYgmSVYBADDvoxIfsfgPyCKmxIubeYTNDtjAyRRDedMiyLprucjiOgjhYeVwBTCMLfrDGXqwpzwVGqMZcLVCxaSJlDSYEofkkEYeqkKHqgBpnbPbgHMLUIDjUMmpBHCSjMJjxzuaiIsNBakqSwQpOQgNczgaczAInLqLIbAatLYHdaopovFOkqIexsFzXzrlcztxcdJJFuyZHRCovgpVvlGsXalGqARmneBZBFelhXkzzfNaVtAyyqWzKqQFbucqNJYWRncGKKLdTkNyoCSfkFohsVVxSAZWEXejhAquXdaaaZlRHoNXvpayoSsqcnCTuGZamCToZvPynaEphIdXaKUaqmBdtZtcOfFSPqKXSLEfZAPaJzldaUEdhITGHvBrQPqWARPXPtPVGNpdGERwVhGCMdfLitTqwLUecgOczXTbRMGxqPexOUAbUdQrIPjyQyQFStFubVVdHtAknjEQxCqkDIfTGXeJtuncbfqQUsXTOdPORvAUkAwwwTndUJHiQecbxzvqzlPWyqOsU"))
	}

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
	return bf.NewPipeBuffer(9, []byte{'\n'})
}
