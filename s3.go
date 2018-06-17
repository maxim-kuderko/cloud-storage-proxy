package cloud_storage_proxy

import (
	"io"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"bytes"
	"time"
	"strconv"
	"fmt"
)

func S3Store(reader io.ReadWriteCloser, opt *TopicOptions) (map[string]interface{}, error) {
	defer func() {
		reader = nil
	}()
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(opt.StoreCredentials["aws-region"]),
		Credentials: credentials.NewStaticCredentials(opt.StoreCredentials["aws-key"], opt.StoreCredentials["aws-secret"], ""),
	}))
	t := time.Now()
	filename := bytes.Buffer{}
	filename.WriteString(opt.Name)
	filename.WriteString("/")
	filename.WriteString(opt.StoreCredentials["prefix"])
	filename.WriteString("/")
	filename.WriteString(strconv.Itoa(time.Now().Year()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", t.Month()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", t.Day()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", t.Hour()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", t.Minute()))
	filename.WriteString("/")
	filename.WriteString(fmt.Sprintf("|%02d|", t.UnixNano()))
	filename.WriteString(".")
	filename.WriteString(opt.Format)
	filename.WriteString(".gz")

	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(opt.StoreCredentials["bucket"]),
		Key:    aws.String(filename.String()),
		Body:   reader,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return make(map[string]interface{}), nil
}
