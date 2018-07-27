package main

import (
	csp "github.com/maxim-kuderko/storage-buffer"
	strg "github.com/maxim-kuderko/storage-buffer/storage"
	"github.com/maxim-kuderko/storage-buffer/buffers"
	"log"
	"io"
	"github.com/klauspost/pgzip"
	"os"
	"os/signal"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)
	a := &csp.TopicOptions{
		ID:            "UNIQUE_ID_1",
		Name:          "test",
		MaxLen:        -1,
		MaxSize:       -1,
		Interval:      10,
		BufferDriver:  initMemBufferWithCompress,
		StorageDriver: strg.NewS3Loader("test", "us-east-1", "", "", "txt", "", "").S3Store,
		Callback:      LogCallback,
	}

	c := csp.NewCollection(1024 * 1024 * 1024 * 10)
	for {
		if _, err := c.Write(a, []byte("XVlBzgbaiCMRAjWwhTHctcuAxhxKQFDaFpLSjFbcXoEFfRsWxPLDnJObCsNVlgTeMaPEZQleQYhYzRyWJjPjzpfRFEgmotaFetHsbZRjxAwnwekrBEmfdzdcEkXBAkjQZLCtTMtTCoaNatyyiNKAReKJyiXJrscctNswYNsGRussVmaozFZBsbOJiFQGZsnwTKSmVoiGLOpbUOpEdKupdOMeRVjaRzLNTXYeUCWKsXbGyRAOmBTvKSJfjzaLbtZsyMGeuDtRzQMDQiYCOhgHOvgSeycJPJHYNufNjJhhjUVRuSqfgqVMkPYVkURUpiFvIZRgBmyArKCtzkjkZIvaBjMkXVbWGvbqzgexyALBsdjSGpngCwFkDifIBuufFMoWdiTskZoQJMqrTICTojIYxyeSxZyfroRODMbNDRZnPNRWCJPMHDtJmHAYORsUfUMApsVgzHblmYYtEjVgwfFbbGGcnqbaEREunUZjQXmZOtaRLUtmYgmSVYBADDvoxIfsfgPyCKmxIubeYTNDtjAyRRDedMiyLprucjiOgjhYeVwBTCMLfrDGXqwpzwVGqMZcLVCxaSJlDSYEofkkEYeqkKHqgBpnbPbgHMLUIDjUMmpBHCSjMJjxzuaiIsNBakqSwQpOQgNczgaczAInLqLIbAatLYHdaopovFOkqIexsFzXzrlcztxcdJJFuyZHRCovgpVvlGsXalGqARmneBZBFelhXkzzfNaVtAyyqWzKqQFbucqNJYWRncGKKLdTkNyoCSfkFohsVVxSAZWEXejhAquXdaaaZlRHoNXvpayoSsqcnCTuGZamCToZvPynaEphIdXaKUaqmBdtZtcOfFSPqKXSLEfZAPaJzldaUEdhITGHvBrQPqWARPXPtPVGNpdGERwVhGCMdfLitTqwLUecgOczXTbRMGxqPexOUAbUdQrIPjyQyQFStFubVVdHtAknjEQxCqkDIfTGXeJtuncbfqQUsXTOdPORvAUkAwwwTndUJHiQecbxzvqzlPWyqOsU")); err != nil {
			log.Println(err)
		}
	}

	<-stop
	c.Shutdown()

}

func LogCallback(output map[string]interface{}, err error) {
	if err != nil {
		log.Println("error: ", err.Error())
	}
	log.Println("success: ", output)
}

func initMemBufferWithCompress() io.ReadWriteCloser {
	f := func(w io.WriteCloser) io.WriteCloser {
		cw, _ := pgzip.NewWriterLevel(w, 9)
		return cw
	}
	return buffers.NewPipeBuffer(f, []byte{'\n'})
}
