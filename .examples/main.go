package main

import (
	csp "github.com/maxim-kuderko/storage-buffer"
	bf "github.com/maxim-kuderko/storage-buffer/buffers"
	strg "github.com/maxim-kuderko/storage-buffer/storage"
	"time"
	"io"
	"log"
)

func main() {

	a := csp.TopicOptions{
		Name:     "test",
		MaxLen:   -1,
		MaxSize:  1024 * 1024 * 50,
		Interval: time.Second * 3,
		StoreCredentials: map[string]string{
			"aws-region": "",
			"aws-key":    "",
			"aws-secret": "",
			"bucket":     "",
		},
		BufferDriver: initMemBufferWithCompress,
		StorageDriver: strg.NewS3Loader().S3Store
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
/*

func SQSCallback(output map[string]interface{}) {
	log.Println(output)
}

func initMemBufferWithCompress() io.ReadWriteCloser {
	return bf.NewMemBuffer(9)
}
