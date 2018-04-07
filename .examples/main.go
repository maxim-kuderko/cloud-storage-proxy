package main

import (
	"github.com/maxim-kuderko/cloud_storage_proxy/storage_drivers"
	"github.com/maxim-kuderko/cloud_storage_proxy"
	"github.com/maxim-kuderko/cloud_storage_proxy/buffer_drivers"
	"time"
	"io"
	"compress/gzip"
	"net/http"
	_ "net/http/pprof"
	"net/http/pprof"
	"log"
)

func main() {

	go func() {
		r := http.NewServeMux()

		// Register pprof handlers
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)

		http.ListenAndServe(":8080", r)
	}()

	a := cloud_storage_proxy.TopicOptions{
		MaxLen:   -1,
		MaxSize:  1024 * 1024 * 1024,
		Interval: time.Second * 60,
	}
	opt := map[string]*cloud_storage_proxy.TopicOptions{
		"test": &a, "test2": &a}
	c := cloud_storage_proxy.NewCollection(storage_drivers.S3Store, initMemBufferWithCompress, opt, SQSCallback)
	for {
		c.Write("test", []byte("XVlBzgbaiCMRAjWwhTHctcuAxhxKQFDaFpLSjFbcXoEFfRsWxPLDnJObCsNVlgTeMaPEZQleQYhYzRyWJjPjzpfRFEgmotaFetHsbZRjxAwnwekrBEmfdzdcEkXBAkjQZLCtTMtTCoaNatyyiNKAReKJyiXJrscctNswYNsGRussVmaozFZBsbOJiFQGZsnwTKSmVoiGLOpbUOpEdKupdOMeRVjaRzLNTXYeUCWKsXbGyRAOmBTvKSJfjzaLbtZsyMGeuDtRzQMDQiYCOhgHOvgSeycJPJHYNufNjJhhjUVRuSqfgqVMkPYVkURUpiFvIZRgBmyArKCtzkjkZIvaBjMkXVbWGvbqzgexyALBsdjSGpngCwFkDifIBuufFMoWdiTskZoQJMqrTICTojIYxyeSxZyfroRODMbNDRZnPNRWCJPMHDtJmHAYORsUfUMApsVgzHblmYYtEjVgwfFbbGGcnqbaEREunUZjQXmZOtaRLUtmYgmSVYBADDvoxIfsfgPyCKmxIubeYTNDtjAyRRDedMiyLprucjiOgjhYeVwBTCMLfrDGXqwpzwVGqMZcLVCxaSJlDSYEofkkEYeqkKHqgBpnbPbgHMLUIDjUMmpBHCSjMJjxzuaiIsNBakqSwQpOQgNczgaczAInLqLIbAatLYHdaopovFOkqIexsFzXzrlcztxcdJJFuyZHRCovgpVvlGsXalGqARmneBZBFelhXkzzfNaVtAyyqWzKqQFbucqNJYWRncGKKLdTkNyoCSfkFohsVVxSAZWEXejhAquXdaaaZlRHoNXvpayoSsqcnCTuGZamCToZvPynaEphIdXaKUaqmBdtZtcOfFSPqKXSLEfZAPaJzldaUEdhITGHvBrQPqWARPXPtPVGNpdGERwVhGCMdfLitTqwLUecgOczXTbRMGxqPexOUAbUdQrIPjyQyQFStFubVVdHtAknjEQxCqkDIfTGXeJtuncbfqQUsXTOdPORvAUkAwwwTndUJHiQecbxzvqzlPWyqOsU"))
	}

}

func SQSCallback(m map[string]interface{}) (resp bool, err error) {
	log.Println("Sent")
	return true, nil
}

func initMemBufferWithCompress() io.ReadWriteCloser {
	return buffer_drivers.NewMemBuffer(gzip.BestCompression)
}
