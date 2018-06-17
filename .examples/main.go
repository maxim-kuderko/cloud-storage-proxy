package main

import (
	csp "github.com/maxim-kuderko/cloud_storage_proxy"
	"time"
	"io"
	"log"
)

func main() {

	a := csp.TopicOptions{
		Name:     "test",
		MaxLen:   -1,
		MaxSize:  1024 * 1024 * 50,
		Interval: time.Second * 60,
		StoreCredentials: map[string]string{
			"aws-region": "",
			"aws-key":    "",
			"aws-secret": "",
			"bucket":     "",
		},
		Callback: SQSCallback,
	}
	opt := map[string]*csp.TopicOptions{"test": &a}
	c := csp.NewCollection(csp.S3Store, initMemBufferWithCompress,1024*1024*1024, opt)
	for {
		c.Write("test", []byte("XVlBzgbaiCMRAjWwhTHctcuAxhxKQFDaFpLSjFbcXoEFfRsWxPLDnJObCsNVlgTeMaPEZQleQYhYzRyWJjPjzpfRFEgmotaFetHsbZRjxAwnwekrBEmfdzdcEkXBAkjQZLCtTMtTCoaNatyyiNKAReKJyiXJrscctNswYNsGRussVmaozFZBsbOJiFQGZsnwTKSmVoiGLOpbUOpEdKupdOMeRVjaRzLNTXYeUCWKsXbGyRAOmBTvKSJfjzaLbtZsyMGeuDtRzQMDQiYCOhgHOvgSeycJPJHYNufNjJhhjUVRuSqfgqVMkPYVkURUpiFvIZRgBmyArKCtzkjkZIvaBjMkXVbWGvbqzgexyALBsdjSGpngCwFkDifIBuufFMoWdiTskZoQJMqrTICTojIYxyeSxZyfroRODMbNDRZnPNRWCJPMHDtJmHAYORsUfUMApsVgzHblmYYtEjVgwfFbbGGcnqbaEREunUZjQXmZOtaRLUtmYgmSVYBADDvoxIfsfgPyCKmxIubeYTNDtjAyRRDedMiyLprucjiOgjhYeVwBTCMLfrDGXqwpzwVGqMZcLVCxaSJlDSYEofkkEYeqkKHqgBpnbPbgHMLUIDjUMmpBHCSjMJjxzuaiIsNBakqSwQpOQgNczgaczAInLqLIbAatLYHdaopovFOkqIexsFzXzrlcztxcdJJFuyZHRCovgpVvlGsXalGqARmneBZBFelhXkzzfNaVtAyyqWzKqQFbucqNJYWRncGKKLdTkNyoCSfkFohsVVxSAZWEXejhAquXdaaaZlRHoNXvpayoSsqcnCTuGZamCToZvPynaEphIdXaKUaqmBdtZtcOfFSPqKXSLEfZAPaJzldaUEdhITGHvBrQPqWARPXPtPVGNpdGERwVhGCMdfLitTqwLUecgOczXTbRMGxqPexOUAbUdQrIPjyQyQFStFubVVdHtAknjEQxCqkDIfTGXeJtuncbfqQUsXTOdPORvAUkAwwwTndUJHiQecbxzvqzlPWyqOsU"))
	}

}

func SQSCallback(m map[string]interface{}) (resp bool, err error) {
	log.Println("Sent")
	return true, nil
}

func initMemBufferWithCompress() io.ReadWriteCloser {
	return csp.NewMemBuffer(0)
}
